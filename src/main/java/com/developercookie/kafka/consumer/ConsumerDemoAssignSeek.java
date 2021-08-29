package com.developercookie.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
    private static String boostrapServers = "localhost:9092";
  //  private static String groupId = "my-seven-application";
    private static String topic = "first_topic";

    // Note : If the consumer has already read the message it will not read it again
    public static void main(String[] args) {
        // Create Producer Property
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      //  properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // other options are latest
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        // assign and seek mostly used to replay data or fetch as specific message

        // Assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 35L;
        kafkaConsumer.assign(Arrays.asList(partitionToReadFrom));

        kafkaConsumer.seek(partitionToReadFrom,offsetToReadFrom);
        int numofMessageToRead= 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

       /* // Subscribe the topic
        kafkaConsumer.subscribe(Collections.singleton(topic));*/

        while (keepOnReading) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                numberOfMessagesReadSoFar+=1;
                logger.info("Key: {} \n Value:  {} \n", record.key(), record.value());
                logger.info("Partion: {} \n Offset:  {}", record.partition(), record.offset());
                if(numberOfMessagesReadSoFar >= numofMessageToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the application");
    }
}
