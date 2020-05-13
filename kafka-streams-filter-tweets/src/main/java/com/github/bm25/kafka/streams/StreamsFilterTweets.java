package com.github.bm25.kafka.streams;

import java.util.Properties;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class StreamsFilterTweets {
    private static JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create a topology
        StreamsBuilder streamBuilder = new StreamsBuilder();
        //"twitter-topics" topic should be created before start and enriched with twitter data feeds by running TwitterProducer
        KStream<String, String> inputTopic = streamBuilder.stream("twitter-tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                //filter for tweets which has a user of over 10000 followers
                (k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 10000
        );
        //"important-twitter" topic should be created before start
        filteredStream.to("important-tweets");

        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamBuilder.build(),
                properties
        );

        //start the stream application
        kafkaStreams.start();
    }


    private static Integer extractUserFollowersInTweet(String tweetJson){
        //gson library
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }
        catch(NullPointerException ex){
            return 0;
        }
    }
}
