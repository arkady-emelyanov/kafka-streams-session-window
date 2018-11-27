package com.example.streams.session;

import com.example.streams.serialize.Long;
import com.example.streams.serialize.WindowedString;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.state.SessionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * Main Topology class
 */
public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    private static final String ENV_VALUE_APPLICATION_LIVE = "live"; // marker for live environment

    private static final String DEFAULT_BROKER_LIST = "localhost:9092"; // default kafka broker list
    private static final String DEFAULT_APPLICATION_ID = "com-example-streams-session"; // default consumer-group
    private static final String DEFAULT_APPLICATION_ENV = "dev"; // default environment for application
    private static final String DEFAULT_NUM_THREADS = "1"; // default number of threads to use
    private static final String DEFAULT_WINDOW_MS = "200"; // default number of milliseconds for aggregate window

    private static final String PROPERTY_NAME_SRC_TOPIC = "com.example.streams.session.src-topic.name";
    private static final String PROPERTY_NAME_DST_TOPIC = "com.example.streams.session.dst-topic.name";
    private static final String PROPERTY_NAME_WINDOW_MS = "com.example.streams.session.window-ms";
    private static final String PROPERTY_NAME_APPLICATION_ENV = "com.example.streams.session.environment";

    private static final String ENV_NAME_APPLICATION_ID = "APP_ID";
    private static final String ENV_NAME_BROKER_LIST = "BROKER_LIST";
    private static final String ENV_NAME_SRC_TOPIC = "SRC_TOPIC";
    private static final String ENV_NAME_DST_TOPIC = "DST_TOPIC";
    private static final String ENV_NAME_NUM_THREADS = "NUM_THREADS";
    private static final String ENV_NAME_WINDOW_MS = "WINDOW_MS";
    private static final String ENV_NAME_APPLICATION_ENV = "APP_ENV";

    /**
     * Entry point
     *
     * @param args command line arguments
     * @throws ParseException when command line couldn't be parsed
     */
    public static void main(String[] args) throws ParseException {
        Properties properties = createProperties(args);

        String appEnv = properties.getProperty(PROPERTY_NAME_APPLICATION_ENV);
        if (appEnv.equals(ENV_VALUE_APPLICATION_LIVE)) {
            logger.info(String.format("Starting '%s' in production mode!",
                    properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)));

            properties.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
            properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 100 * 1024 * 1024L);
            properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        }

        KafkaStreams app = createApp(properties);
        if (!appEnv.equals(ENV_VALUE_APPLICATION_LIVE)) {
            // Do not perform cleanUp in production environment:
            // @see https://docs.confluent.io/current/streams/developer-guide/app-reset-tool.html#step-2-reset-the-local-environments-of-your-application-instances
            app.cleanUp();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(app::close));
        app.start();
    }

    /**
     * Create KafkaStreams application
     *
     * @param properties properties for kafka streams app
     * @return kafka streams application instance
     */
    private static KafkaStreams createApp(Properties properties) {
        String srcTopic = properties.getProperty(PROPERTY_NAME_SRC_TOPIC);
        String dstTopic = properties.getProperty(PROPERTY_NAME_DST_TOPIC);
        String windowMs = properties.getProperty(PROPERTY_NAME_WINDOW_MS);
        long window = java.lang.Long.parseLong(windowMs);


        Topology topology = createTopology(srcTopic, dstTopic, window);
        return new KafkaStreams(topology, properties);
    }

    /**
     * Create topology, public access requires for AppTest
     *
     * @param srcTopic events source topic
     * @param dstTopic aggregation destination topic
     * @param windowMs Window length in milliseconds
     * @return topology of streams application
     */
    public static Topology createTopology(String srcTopic, String dstTopic, long windowMs) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(srcTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                .windowedBy(SessionWindows.with(Duration.ofMillis(windowMs)))
                .count(Materialized.<String, java.lang.Long, SessionStore<Bytes, byte[]>>as(dstTopic)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .to(dstTopic, Produced.with(new WindowedString(), new Long()));
        return builder.build();
    }

    /**
     * Parse command line
     *
     * @param args arguments to parse
     * @return prepared properties for kafka streams
     * @throws ParseException when args couldn't be parsed
     */
    private static Properties createProperties(String[] args) throws ParseException {
        Options options = getCommandLineOptions();

        Properties properties = new Properties();
        HelpFormatter helpFormatter = new HelpFormatter();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmdLine = parser.parse(options, args);

        String appId = getConfigOption(cmdLine, "app-id", ENV_NAME_APPLICATION_ID, false);
        String brokerList = getConfigOption(cmdLine, "broker-list", ENV_NAME_BROKER_LIST, true);
        String windowMs = getConfigOption(cmdLine, "window-ms", ENV_NAME_WINDOW_MS, false);
        String srcTopic = getConfigOption(cmdLine, "src-topic", ENV_NAME_SRC_TOPIC, true);
        String dstTopic = getConfigOption(cmdLine, "dst-topic", ENV_NAME_DST_TOPIC, true);
        String numThreads = getConfigOption(cmdLine, "num-threads", ENV_NAME_NUM_THREADS, false);
        String appEnv = getConfigOption(cmdLine, "environment", ENV_NAME_APPLICATION_ENV, false);

        if (appId == null) {
            appId = DEFAULT_APPLICATION_ID;
        }
        if (numThreads == null) {
            numThreads = DEFAULT_NUM_THREADS;
        }
        if (windowMs == null) {
            windowMs = DEFAULT_WINDOW_MS;
        }
        if (appEnv == null) {
            appEnv = DEFAULT_APPLICATION_ENV;
        }

        if (brokerList == null || srcTopic == null || dstTopic == null) {
            helpFormatter.printHelp(appId, options);
            System.exit(1);
        }

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThreads);
        properties.put(PROPERTY_NAME_APPLICATION_ENV, appEnv);
        properties.put(PROPERTY_NAME_SRC_TOPIC, srcTopic);
        properties.put(PROPERTY_NAME_DST_TOPIC, dstTopic);
        properties.put(PROPERTY_NAME_WINDOW_MS, windowMs);
        return properties;
    }

    /**
     * Format set of options application accepts
     *
     * @return options for application
     */
    private static Options getCommandLineOptions() {
        Options opts = new Options();

        opts.addOption(new Option("b", "broker-list", true, String.format(
                "Kafka brokers to connect to (env: %s, default: %s)",
                ENV_NAME_BROKER_LIST,
                DEFAULT_BROKER_LIST)));

        opts.addOption(new Option("s", "src-topic", true, String.format(
                "Source topic (env: %s), required",
                ENV_NAME_SRC_TOPIC)));

        opts.addOption(new Option("d", "dst-topic", true, String.format(
                "Destination topic (env: %s), required",
                ENV_NAME_DST_TOPIC)));

        opts.addOption(new Option("t", "num-threads", true, String.format(
                "Number of threads (env: %s, default: %s)",
                ENV_NAME_NUM_THREADS,
                DEFAULT_NUM_THREADS)));

        opts.addOption(new Option("a", "app-id", true, String.format(
                "Consumer group (env: %s, default: %s)",
                ENV_NAME_APPLICATION_ID,
                DEFAULT_APPLICATION_ID)));

        opts.addOption(new Option("w", "window-ms", true, String.format(
                "Window length, in milliseconds (env: %s, default: %s)",
                ENV_NAME_WINDOW_MS,
                DEFAULT_WINDOW_MS)));

        opts.addOption(new Option("e", "environment", true, String.format(
                "Live/Dev switch, set to '%s' for production (env: %s, default: %s)",
                ENV_VALUE_APPLICATION_LIVE,
                ENV_NAME_APPLICATION_ENV,
                DEFAULT_APPLICATION_ENV)));

        return opts;
    }

    /**
     * Get option value either from provided arguments or from environment variable
     *
     * @param cmdLine    command line object
     * @param optionName name of option
     * @param envName    name of environment variable
     * @param required   indicate that provided option should be provided
     * @return null or string value of option
     */
    private static String getConfigOption(CommandLine cmdLine, String optionName, String envName, boolean required) {
        String optValue = System.getenv(envName);
        if (optValue == null || cmdLine.hasOption(optionName)) {
            optValue = cmdLine.getOptionValue(optionName);
        }

        if (optValue == null || optValue.isEmpty()) {
            if (required) {
                logger.error(optionName + " option is not provided or " + envName + " environment variable is not set!");
            }
            return null;
        }

        return optValue;
    }
}
