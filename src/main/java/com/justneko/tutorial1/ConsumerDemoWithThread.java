package com.justneko.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    private final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {
        var app = new ConsumerDemoWithThread();
        app.run();
    }

    public void run() {
        var groupId = "my-sixth-application";
        var topic = "first_topic";

        consume(groupId, topic);
    }

    private void consume(String groupId, String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties(groupId));

        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable(latch, consumer, topic);

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e){
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    private Properties getProperties(String groupId) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //"earliest/last/none"
        return properties;
    }

    public static class ConsumerRunnable implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;
        private final String topic;

        public ConsumerRunnable(CountDownLatch latch, KafkaConsumer<String, String> consumer, String topic) {
            this.latch = latch;
            this.consumer = consumer;
            this.topic = topic;
        }

        @Override
        public void run() {
            consumer.subscribe(Collections.singleton(topic));
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info(String.format("Key: %s, Partition: %d, Offset: %d Value: %s",
                                record.key(), record.partition(), record.offset(), record.value()));
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }

        }

        /**
         * This method is a special method to interrupt consumer.pull()
         * It will throw an exception WakeUpException
         */
        public void shutdown() {
            consumer.wakeup();
        }
    }
}
