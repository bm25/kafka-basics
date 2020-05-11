package com.github.bm25.kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumerDemo {
    public final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerDemo.class);
    private static JsonParser jsonParser = new JsonParser();

    public RestHighLevelClient createElasticClient(){
        /* Credentials from bonsai.io:
            https://arbztl88az:s7prqjfvrb@kafka-course-5738345425.eu-central-1.bonsaisearch.net:443
        */
        String hostName = "kafka-course-5738345425.eu-central-1.bonsaisearch.net";
        String userName = "arbztl88az";
        String password = "s7prqjfvrb";

        //do not do it if you run a local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;

    }

    public KafkaConsumer<String, String> createConsumer(String topic){
        final String bootstrapServers = "127.0.0.1:9092";
        final String groupId = "kafka-demo-elasticsearch";//new group id lead to reading all the messages from the topic very beginning
        //final String topic = "twitter-tweets";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");//max count of records to be polled by consumer from Kafka

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public void pollTweetsToElastic(RestHighLevelClient elasticClient, KafkaConsumer<String, String> consumer) throws IOException{
        //poll for new data

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));//new in Kafka 2.0.0
            logger.info("Consumer received " + records.count() + " records from Kafka-cluster");

            for (ConsumerRecord<String, String> record : records) {
                //2 strategies
                //1. Kafka generic Id
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                //2. Twitter feed specific id
                String id = extractIdFromTweet(record.value());


                IndexRequest indexRequest = new IndexRequest(
                        "twitter"//Do not forget to create respective "twitter" index in ElasticSearch
                        ,"_doc"//"tweets" type has been replaced with "_doc"
                        // Types are deprecated since Elastic 7.0: see https://www.elastic.co/guide/en/elasticsearch/reference/7.x/removal-of-types.html
                        ,id //this is to make our consumer idempotent
                ).source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = elasticClient.index(indexRequest, RequestOptions.DEFAULT);
                logger.info("record " + indexResponse.getId() + " has been added to ElasticSearch");

                try {
                    Thread.sleep(10);
                }
                catch(InterruptedException ex) {
                    ex.printStackTrace();
                }
            }

            logger.info("Committing offsets...");
            consumer.commitAsync();
            logger.info("Offsets have been committed");
            try {
                Thread.sleep(1000);
            }
            catch(InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }

    private static String extractIdFromTweet(String tweetJson){
        //gson library
        return
        jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {

        ElasticSearchConsumerDemo demo = new ElasticSearchConsumerDemo();
        RestHighLevelClient elasticClient = demo.createElasticClient();
        KafkaConsumer<String, String> consumer = demo.createConsumer("twitter-tweets");
        demo.pollTweetsToElastic(elasticClient, consumer);

        //close the client gracefully
        //client.close();
    }
}
