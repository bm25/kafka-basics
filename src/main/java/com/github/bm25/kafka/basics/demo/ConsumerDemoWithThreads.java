package com.github.bm25.kafka.basics.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String GROUP_ID = "my-sixth-application";
    private static final String TOPIC = "first-topic";
    private final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

    private ConsumerDemoWithThreads() {
    }

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private void run() {
        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        Runnable myConsumerRunnable = new ConsumerRunnable(
                BOOTSTRAP_SERVERS,
                GROUP_ID,
                TOPIC,
                latch
        );

        //start the thread
        new Thread(myConsumerRunnable).start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();
            try {
                latch.await();//wait till KafkaConsumer is closed
            } catch (InterruptedException ex){
                ex.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(String bootstrapServer,
                                String groupId,
                                String topic,
                                CountDownLatch latch
                              ){
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            //subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            //poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));//new in Kafka 2.0.0

                    records.forEach(record -> {
                        logger.info(String.format("Key: %s, Value: %s", record.key(), record.value()));
                        logger.info(String.format("Partition: %s, offset: %s", record.partition(), record.offset()));
                    });
                }
            }
            catch(WakeupException ex) {
                logger.info("Received shutdown signal!");
            }
            finally {
                consumer.close();
                //tell our main code we're done with this consumer
                latch.countDown();
            }
        }

        public void shutdown() {

            //the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the WakeUpException
            consumer.wakeup();
        }
    }
}
