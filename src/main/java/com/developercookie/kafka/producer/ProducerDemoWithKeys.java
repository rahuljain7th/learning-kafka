package com.developercookie.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    private static String boostrapServers = "localhost:9092";
    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Create Producer Property
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        // send data
        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String key = "id_" + i;
            String value = "value_" + i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);
            logger.info("Producer Key:- "+ key);
            kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Recieved new metadata.\n" + "Topic: -" + recordMetadata.topic() + "\n partition:-" + recordMetadata.partition()
                            + "\n Offset" + recordMetadata.offset() + "\n Timestamp:-" + recordMetadata.timestamp());
                    // record was successfully sent
                } else {
                    logger.error("Error while producing", e);
                    // There was an error while sending record.
                }

            }).get(); // adding .get is blocking and making the send as sync . should not be used in production
        }


        // Flushing - ensure the data is send before exit the program
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
