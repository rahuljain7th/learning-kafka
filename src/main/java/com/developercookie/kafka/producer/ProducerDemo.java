package com.developercookie.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    private static String boostrapServers = "localhost:9092";
    public static void main(String[] args) {
        // Create Producer Property
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        // create producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer(properties);
        // send data
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>("first_topic","hello world");
        // This is async
        kafkaProducer.send(producerRecord);

        // Flushing - ensure the data is send before exit the program
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
