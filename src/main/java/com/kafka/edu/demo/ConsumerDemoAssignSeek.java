package com.kafka.edu.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

// It works without consumer groups. Can be used in some kind of replay-service
public class ConsumerDemoAssignSeek {

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC = "first-topic";
    private static final Long OFFSET_TO_READ_FROM = 15L;
    private static final int NUMBER_OF_MESSAGES_TO_READ = 5;

    public static void main(String[] args) {
        boolean keepOnReading = true;
        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are mostly used to replay data or fetch a specific message

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(TOPIC, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, OFFSET_TO_READ_FROM);

        //poll for new data
        while(keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));//new in Kafka 2.0.0

            for (ConsumerRecord<String, String> record :records) {

                logger.info(String.format("Key: %s, Value: %s", record.key(), record.value()));
                logger.info(String.format("Partition: %s, offset: %s", record.partition(), record.offset()));
                numberOfMessagesReadSoFar++;
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;

                }
            }
        }

        logger.info("Exiting the application");
    }
}
