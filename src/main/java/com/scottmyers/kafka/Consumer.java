package com.scottmyers.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Consumer.class);
        // Example IP of Kafka server is 10.0.0.70 in this example.
        // Create variables for strings
        final String bootstrapServers = "10.0.0.70:9092";
        final String consumerGroupID = "java-group-consumer";

        // Create and populate properties object
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(p);

        // Subscribe to topic(s)
        consumer.subscribe(Arrays.asList("java-topic-2"));

        // Poll and Consume records
        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record: records) {
                System.out.println("Record: " + record.value().toString());
                /*
                logger.info("------>"+ record.value().toString());
                logger.info("\nReceived record metadata. (Consumer) " +
                        "Topic: " + record.topic() +
                        ", Partition: " + record.partition() + ", " +
                        "Offset: " + record.offset() +
                        " @ Timestamp: " + record.timestamp() + "\n"
                ); */
            }
        }
    }
}