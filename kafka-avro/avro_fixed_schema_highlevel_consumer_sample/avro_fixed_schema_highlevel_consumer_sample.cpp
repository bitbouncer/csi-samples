#include <chrono>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/program_options.hpp>
#include <csi_kafka/highlevel_consumer.h>
#include <csi_kafka/internal/utility.h>
#include <csi_avro_utils/confluent_codec.h>
#include "contact_info.h"
#include "contact_info_key.h"

struct contact_info_key_compare
{
    bool operator() (const sample::contact_info_key& lhs, const sample::contact_info_key& rhs)
    {
        return lhs.md5 < rhs.md5;
    }
};

int main(int argc, char** argv)
{
    boost::program_options::options_description desc("options");
    desc.add_options()
        ("help", "produce help message")
        ("topic", boost::program_options::value<std::string>(), "topic")
        ("broker", boost::program_options::value<std::string>(), "broker")
        ("schema_registry", boost::program_options::value<std::string>(), "schema_registry")
        ("schema_registry_port", boost::program_options::value<int>()->default_value(8081), "schema_registry_port")
        ;

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);

    if (vm.count("help"))
    {
        std::cout << desc << std::endl;
        return 0;
    }

    std::string topic;
    if (vm.count("topic"))
    {
        topic = vm["topic"].as<std::string>();
    }
    else
    {
        std::cout << "--topic must be specified" << std::endl;
        return 0;
    }

    int32_t kafka_port = 9092;
    std::vector<csi::kafka::broker_address> kafka_brokers;
    if (vm.count("broker"))
    {
        std::string s = vm["broker"].as<std::string>();
        size_t last_colon = s.find_last_of(':');
        if (last_colon != std::string::npos)
            kafka_port = atoi(s.substr(last_colon + 1).c_str());
        s = s.substr(0, last_colon);

        // now find the brokers...
        size_t last_separator = s.find_last_of(',');
        while (last_separator != std::string::npos)
        {
            std::string host = s.substr(last_separator + 1);
            kafka_brokers.push_back(csi::kafka::broker_address(host, kafka_port));
            s = s.substr(0, last_separator);
            last_separator = s.find_last_of(',');
        }
        kafka_brokers.push_back(csi::kafka::broker_address(s, kafka_port));
    }
    else
    {
        std::cout << "--broker must be specified" << std::endl;
        return 0;
    }

    int32_t schema_registry_port = 8081;
    std::vector<csi::kafka::broker_address> schema_registrys;
    std::string used_schema_registry;

    if (vm.count("schema_registry_port"))
    {
        schema_registry_port = vm["schema_registry_port"].as<int>();
    }

    if (vm.count("schema_registry"))
    {
        std::string s = vm["schema_registry"].as<std::string>();
        size_t last_colon = s.find_last_of(':');
        if (last_colon != std::string::npos)
            schema_registry_port = atoi(s.substr(last_colon + 1).c_str());
        s = s.substr(0, last_colon);

        // now find the brokers...
        size_t last_separator = s.find_last_of(',');
        while (last_separator != std::string::npos)
        {
            std::string host = s.substr(last_separator + 1);
            schema_registrys.push_back(csi::kafka::broker_address(host, schema_registry_port));
            s = s.substr(0, last_separator);
            last_separator = s.find_last_of(',');
        }
        schema_registrys.push_back(csi::kafka::broker_address(s, schema_registry_port));
    }
    else
    {
        // default - assume registry is running on all kafka brokers
        for (std::vector<csi::kafka::broker_address>::const_iterator i = kafka_brokers.begin(); i != kafka_brokers.end(); ++i)
        {
            schema_registrys.push_back(csi::kafka::broker_address(i->host_name, schema_registry_port));
        }
    }

    // right now the schema registry class cannot handle severel hosts so just stick to the first one.
    used_schema_registry = schema_registrys[0].host_name + ":" + std::to_string(schema_registrys[0].port);


    std::string kafka_broker_str = "";
    for (std::vector<csi::kafka::broker_address>::const_iterator i = kafka_brokers.begin(); i != kafka_brokers.end(); ++i)
    {
        kafka_broker_str += i->host_name + ":" + std::to_string(i->port);
        if (i != kafka_brokers.end() - 1)
            kafka_broker_str += ", ";
    }

    BOOST_LOG_TRIVIAL(info) << "kafka broker(s): " << kafka_broker_str;
    BOOST_LOG_TRIVIAL(info) << "topic          : " << topic;

    std::string schema_registrys_info;
    for (std::vector<csi::kafka::broker_address>::const_iterator i = schema_registrys.begin(); i != schema_registrys.end(); ++i)
    {
        schema_registrys_info += i->host_name + ":" + std::to_string(i->port);
        if (i != schema_registrys.end() - 1)
            schema_registrys_info += ", ";
    }
    BOOST_LOG_TRIVIAL(info) << "schema_registry(s)  : " << schema_registrys_info;
    BOOST_LOG_TRIVIAL(info) << "used schema registry: " << used_schema_registry;

    boost::asio::io_service fg_ios;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(fg_ios));
    boost::thread fg(boost::bind(&boost::asio::io_service::run, &fg_ios));

    csi::kafka::highlevel_consumer consumer(fg_ios, topic, 20, 10000);
    confluent::registry            registry(fg_ios, used_schema_registry);
    confluent::codec               avro_codec(registry);


    csi::kafka::table<sample::contact_info_key, sample::contact_info, contact_info_key_compare> datastore;

    int64_t message_total = 0;

    boost::system::error_code ec1 = consumer.connect(kafka_brokers);
    consumer.connect_forever(kafka_brokers);

    consumer.set_offset(csi::kafka::earliest_available_offset);



    BOOST_LOG_TRIVIAL(info) << "registring schemas";
    auto key_res = avro_codec.put_schema("sample.contact_info_key", sample::contact_info_key::valid_schema());

    if (key_res.first != 0)
    {
        BOOST_LOG_TRIVIAL(error) << "registring sample.contact_info_key failed";
        return -1;
    }
    auto val_res = avro_codec.put_schema("sample.contact_info", sample::contact_info::valid_schema());
    if (val_res.first != 0)
    {
        BOOST_LOG_TRIVIAL(error) << "registring sample.contact_info failed";
        return -1;
    }
    BOOST_LOG_TRIVIAL(info) << "registring schemas done";

    boost::thread do_log([&consumer]
    {
        while (true)
        {
            boost::this_thread::sleep(boost::posix_time::seconds(1));

            std::vector<csi::kafka::highlevel_consumer::metrics>  metrics = consumer.get_metrics();
            uint32_t rx_msg_sec_total = 0;
            uint32_t rx_kb_sec_total = 0;
            for (std::vector<csi::kafka::highlevel_consumer::metrics>::const_iterator i = metrics.begin(); i != metrics.end(); ++i)
            {
                rx_msg_sec_total += (*i).rx_msg_sec;
                rx_kb_sec_total += (*i).rx_kb_sec;
            }
            BOOST_LOG_TRIVIAL(info) << "\t\t" << rx_msg_sec_total << " msg/s \t" << (rx_kb_sec_total / 1024) << "MB/s";
        }
    });

    int32_t key_id = key_res.second;
    int32_t val_id = val_res.second;

    while (true)
    {
        auto r = consumer.fetch();
        size_t nr_of_msg = 0;
        for (std::vector<csi::kafka::rpc_result<csi::kafka::fetch_response>>::const_iterator i = r.begin(); i != r.end(); ++i)
        {
            if (i->ec)
                continue; // or die??
            for (std::vector<csi::kafka::fetch_response::topic_data>::const_iterator j = (*i)->topics.begin(); j != (*i)->topics.end(); ++j)
            {
                for (std::vector<std::shared_ptr<csi::kafka::fetch_response::topic_data::partition_data>>::const_iterator k = j->partitions.begin(); k != j->partitions.end(); ++k)
                {
                    if ((*k)->error_code)
                        continue; // or die??

                    nr_of_msg += (*k)->messages.size();

                    std::shared_ptr<sample::contact_info_key> key;
                    std::shared_ptr<sample::contact_info>     value;

                    bool has_key = false;
                    bool has_val = false;

                    for (std::vector<std::shared_ptr<csi::kafka::basic_message>>::const_iterator m = (*k)->messages.begin(); m != (*k)->messages.end(); ++m)
                    {
                        // decode key
                        if (!(*m)->key.is_null())
                        {
                            key = std::make_shared<sample::contact_info_key>();
                            has_key = avro_codec.decode_static(&(*m)->key[0], (*m)->key.size(), key_id, *key);
                            //do something with key...
                        }

                        //decode value
                        if (!(*m)->value.is_null())
                        {
                            value = std::make_shared<sample::contact_info>();
                            has_val = avro_codec.decode_static(&(*m)->value[0], (*m)->value.size(), val_id, *value);
                        }

                        if (has_key)
                        {
                            if (has_val)
                            {
                                datastore.put(*key, value);
                            }
                            else
                            {
                                datastore.put(*key, std::shared_ptr<sample::contact_info>());
                            }
                        }
                    } // message
                } // partition
            } //topic
        } // per connection
    }

    while (true)
    {
        boost::this_thread::sleep(boost::posix_time::seconds(1));
    }

    work.reset();
    fg_ios.stop();
    return EXIT_SUCCESS;
}
