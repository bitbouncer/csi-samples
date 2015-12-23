#include <chrono>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/program_options.hpp>
#include <csi_kafka/highlevel_producer.h>
#include <csi_avro_utils/confluent_codec.h>
#include "contact_info.h"
#include "contact_info_key.h"

void create_message(std::vector<std::pair<sample::contact_info_key, sample::contact_info>>& v, int32_t& cursor)
{
    v.clear();
    for (int i = 0; i != 10000; ++i, ++cursor)
    {
        std::string email = std::string("nisse") + boost::lexical_cast<std::string>(cursor)+"@csi.se";

        sample::contact_info_key key;
        key.md5 = email; // ugly but works as md5 for now is just a string in the json

        sample::contact_info value;
        value.care_of.set_null();
        value.city.set_string("stockholm");
        value.country.set_string("sweden");
        value.date_of_birth.set_null();
        value.email.set_string(email);
        value.family_name.set_string("gul");
        value.given_name.set_string(std::string("nisse") + boost::lexical_cast<std::string>(cursor));
        value.nin.set_null();
        value.postal_code.set_string("111 11");
        value.street_address.set_string("street 7a");
        v.push_back(std::pair<sample::contact_info_key, sample::contact_info>(key, value));
    }
}

void encode_messages(confluent::codec& codec, const std::vector<std::pair<sample::contact_info_key, sample::contact_info>>& src, int32_t key_id, int32_t value_id, std::vector<std::shared_ptr<csi::kafka::basic_message>>& dst)
{
	dst.reserve(src.size());
	for (std::vector<std::pair<sample::contact_info_key, sample::contact_info>>::const_iterator i = src.begin(); i != src.end(); ++i)
	{
		std::shared_ptr<csi::kafka::basic_message> msg(new csi::kafka::basic_message());

        //encode key
        {
            auto os = codec.encode_nonblock(key_id, i->first);
			size_t sz = os->byteCount();
			auto is = avro::memoryInputStream(*os);
			avro::StreamReader stream_reader(*is);
            msg->key.set_null(false);
            msg->key.resize(sz);
            stream_reader.readBytes(msg->key.data(), sz);
        }

	    //encode value
        {
            auto os = codec.encode_nonblock(value_id, i->second);
            size_t sz = os->byteCount();
			auto is = avro::memoryInputStream(*os);
            avro::StreamReader stream_reader(*is);
            msg->value.set_null(false);
            msg->value.resize(sz);
            stream_reader.readBytes(msg->value.data(), sz);
        }
		dst.push_back(msg);
	}
}

void send_messages(confluent::codec& codec, int32_t key_id, int32_t val_id, csi::kafka::highlevel_producer& producer, int id)
{
	int32_t cursor = 0;
	std::vector<std::pair<sample::contact_info_key, sample::contact_info>> v;
	create_message(v, cursor);

	while (true)
	{
		std::vector<std::shared_ptr<csi::kafka::basic_message>> v2;
		encode_messages(codec, v, key_id, val_id, v2);
		int32_t ec = producer.send_sync(v2);
		std::cerr << id;
		if (cursor > 10000000)
			cursor = 0;
	}
}

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


    int64_t total = 0;
	boost::asio::io_service fg_ios;
	std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(fg_ios));
	boost::thread fg(boost::bind(&boost::asio::io_service::run, &fg_ios));

	csi::kafka::highlevel_producer producer(fg_ios, topic, -1, 200, 1000000);
    confluent::registry            registry(fg_ios, used_schema_registry);
	confluent::codec               avro_codec(registry);

    producer.connect(kafka_brokers);
    BOOST_LOG_TRIVIAL(info) << "connected to kafka";
    producer.connect_forever(kafka_brokers);

	boost::thread do_log([&producer]
	{
		while (true)
		{
			boost::this_thread::sleep(boost::posix_time::seconds(1));

			std::vector<csi::kafka::highlevel_producer::metrics>  metrics = producer.get_metrics();

			size_t total_queue = 0;
			uint32_t tx_msg_sec_total = 0;
			uint32_t tx_kb_sec_total = 0;
			for (std::vector<csi::kafka::highlevel_producer::metrics>::const_iterator i = metrics.begin(); i != metrics.end(); ++i)
			{
				total_queue += (*i).msg_in_queue;
				tx_msg_sec_total += (*i).tx_msg_sec;
				tx_kb_sec_total += (*i).tx_kb_sec;
			}
            BOOST_LOG_TRIVIAL(info) << "\t        \tqueue:" << total_queue << "\t" << tx_msg_sec_total << " msg/s \t" << (tx_kb_sec_total / 1024) << "MB/s";
		}
	});


	std::cerr << "registring schemas" << std::endl;
	auto key_res = avro_codec.put_schema("sample.contact_info_key", sample::contact_info_key::valid_schema());

	if (key_res.first!=0)
	{
        BOOST_LOG_TRIVIAL(error) << "registring sample.contact_info_key failed";
		return -1;
	}
	auto val_res = avro_codec.put_schema("sample.contact_info", sample::contact_info::valid_schema());
	if (val_res.first!=0)
	{
        BOOST_LOG_TRIVIAL(error) << "registring sample.contact_info failed";
		return -1;
	}
    BOOST_LOG_TRIVIAL(info) << "registring schemas done";
	
    //produce messages

	std::vector<boost::thread*> threads;
	
	for (int i = 0; i != 10; ++i)
	{
        threads.emplace_back(new boost::thread([&avro_codec, key_res, val_res, &producer, i]
		{
            send_messages(avro_codec, key_res.second, val_res.second, producer, i);
		}));
	}

	while (true)
	{
		boost::this_thread::sleep(boost::posix_time::seconds(1));
	}


    work.reset();
	fg_ios.stop();
    return EXIT_SUCCESS;
}
