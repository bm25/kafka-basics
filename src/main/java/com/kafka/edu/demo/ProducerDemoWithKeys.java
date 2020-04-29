package com.kafka.edu.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    //All the messages will go to random partitions. Producer decides to which partition put the message, not Broker.
    //By default KafkaProducer uses org.apache.kafka.clients.producer.internals.DefaultPartitioner
    //The keys always go to the same partition for a fixed number of partitions.
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            String topic = "first-topic";
            String value = "hello world " + i;
            String key = "id_" + i;

            //creating a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            logger.info("Key:" + key);

            //send data - asynchronously
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes each time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        //the record was successfully sent
                        logger.info("Received new metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp()
                        );

                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get();//block the .send() to make it synchronous - don't do it in production
        }

        producer.flush();
        producer.close();
    }
}
