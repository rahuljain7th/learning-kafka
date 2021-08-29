package com.developercookie.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    private static String boostrapServers = "localhost:9092";
    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

    public static void main(String[] args) {
        // Create Producer Property
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        // send data
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", "hello world");
        // This is async
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                logger.info("Recieved new metadata.\n" + "Topic: -" + recordMetadata.topic() + "\n partition:-" + recordMetadata.partition()
                        + "\n Offset" + recordMetadata.offset() + "\n Timestamp:-" + recordMetadata.timestamp());
                // record was successfully sent
            } else {
                logger.error("Error while producing", e);
                // There was an error while sending record.
            }

        });

        // Flushing - ensure the data is send before exit the program
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
