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

// --broker f013-520-kafka.internal.machines,f014-520-kafka.internal.machines,f014-520-kafka.internal.machines --topic saka.dev.mqtt_device_auth_view

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

    int64_t message_total = 0;

    boost::system::error_code ec1 = consumer.connect(kafka_brokers);
    consumer.connect_forever(kafka_brokers);

    consumer.set_offset(csi::kafka::earliest_available_offset);

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

	// generic decoding
	while (true)
	{
		auto r = consumer.fetch();
		for (std::vector<csi::kafka::highlevel_consumer::fetch_response>::const_iterator i = r.begin(); i != r.end(); ++i)
		{
			if ((*i).data)
			{
				const std::vector<std::shared_ptr<csi::kafka::basic_message>>& messages((*i).data->messages);
				for (std::vector<std::shared_ptr<csi::kafka::basic_message>>::const_iterator j = messages.begin(); j != messages.end(); ++j)
				{
					// decode key
					if (!(*j)->key.is_null())
					{
						//std::auto_ptr<avro::InputStream> stream = avro::memoryInputStream(&(*j)->key[0], (*j)->key.size());
						//auto res = avro_codec.decode_datum(&*stream);
                        auto res = avro_codec.decode_datum(&(*j)->key[0], (*j)->key.size());
                        // do something with key...
					}

					//decode value
					if (!(*j)->value.is_null())
					{
						//std::auto_ptr<avro::InputStream> stream = avro::memoryInputStream(&(*j)->value[0], (*j)->value.size());
						//auto res = avro_codec.decode_datum(&*stream);
                        auto res = avro_codec.decode_datum(&(*j)->value[0], (*j)->value.size());
					}
				} // message
			} // has data
		} // per connection
	}

	work.reset();
	fg_ios.stop();
    return EXIT_SUCCESS;
}
