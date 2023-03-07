package com.justneko.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        var app = new ProducerDemoWithCallback();
        app.run();
    }

    public void run() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties());
        var topic = "first_topic";

        for (int i = 1; i < 11; i++) {
            send(producer, topic, "Message #" + i);
        }

        producer.close();
    }

    public void send(KafkaProducer<String, String> producer, String topic, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        producer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                logger.error("Error wile producing", e);
            }
            logger.info("Received new metadata.\n" +
                    "Topic:" + recordMetadata.topic() + "\n" +
                    "Partition:" + recordMetadata.partition() + "\n" +
                    "Offsets:" + recordMetadata.offset() + "\n" +
                    "Timestamp:" + recordMetadata.timestamp()
            );
        });
        producer.flush();
    }

    private Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
