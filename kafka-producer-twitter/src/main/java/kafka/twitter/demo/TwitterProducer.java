package kafka.twitter.demo;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@NoArgsConstructor
public class TwitterProducer {
    /*
    Before the first execution do the following:

    1. Create respective topic
    > kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic twitter-tweets --partitions 6 --replication-factor 1

    2. Run kafka-console-consumer for created topic
    > kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic twitter-tweets
     */

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private final static String CONSUMER_KEY = "J07AamFLY4Th6UHmLIvwqM5Ab";
    private final static String CONSUMER_SECRET = "N3WA5aXDto6q5FrymXwO1UzJZiHGAngItuHx1sb4aKfXZHq0Hw";
    private final static String ACCESS_TOKEN = "907892294468210688-4r7m7ct14MyqvdviNafIOnjt5ektRXn";
    private final static String ACCESS_TOKEN_SECRET = "hJJxRnnYWVcPVqId1ixztHm3JYrGMF1yPnSPDcFkPVKXC";

    // to test high throughput
    private final static List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");// leave only "kafka" to test lower throughput

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){

        logger.info("Setup");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create a twitter client
        Client twitterClient = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        twitterClient.connect();

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            twitterClient.stop();
            logger.info("closing producer...");
            producer.close();//producer sends all messages to Kafka cluster before closing
            logger.info("done...");
        }) );

        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>("twitter-tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened: " + e.getMessage());
                        }
                    }
                });
            }
        }

        logger.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        //Declaring the connection information

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));                          // optional: use this if you want to process client events

        return builder.build();
    }

    private KafkaProducer<String, String> createKafkaProducer(){

        Properties properties = new Properties();
        //common producer properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");//kafka 2.0 >= 1.1 so we can keep this as 5 to put messages in correct order, otherwise use 1

        //high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));//32 Kb batch size

        //create the producer
        return new KafkaProducer<String, String>(properties);
    }
}
