package com.justneko.tutorial1;

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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

    public static void main(String[] args) {
        var app = new ConsumerDemoAssignSeek();
        app.run();
    }

    public void run() {
        var topic = "first_topic";

        consume(topic);
    }

    private void consume(String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties());

        // assign and seek are mostly used to replay data or fetch a specific message

        // assign
        TopicPartition topicPartitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(List.of(topicPartitionToReadFrom));

        // seek
        consumer.seek(topicPartitionToReadFrom, offsetToReadFrom);

        int numberMessagesToRead = 5;
        int numberMessagesReadSoFar = 0;
        boolean keepOnReading = true;

        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));
            for (ConsumerRecord<String, String> record : records){
                numberMessagesReadSoFar++;
                logger.info(String.format("Key: %s, Partition: %d, Offset: %d Value: %s",
                        record.key(), record.partition(), record.offset(), record.value()));
                if(numberMessagesReadSoFar > numberMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the application");
    }

    private Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //"earliest/last/none"
        return properties;
    }
}
