package com.scottmyers.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    public static String createRandomWord(int len) {  // Convert to serial integer
        String name = "";
        for (int i = 0; i < len; i++) {
            int v = 1 + (int) (Math.random() * 26);
            char c = (char) (v + (i == 0 ? 'A' : 'a') - 1);
            name += c;
        }   return name;
    }

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(Producer.class);
        System.out.println("Kafka:");

        // Create properties object for Producer
        // Needed:  key.serializer, value.serializer, bootstrap.serializer
        // example: prop.setProperty("key.serializer", StringSerializer.class.getName());
        // Example IP of Kafka server is 10.0.0.70 in this example.
        // Console Commands on Server:
        // kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic sample-topic --create --partitions 3
        // kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic sample-topic --group java-group
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.0.0.70:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer
        final KafkaProducer<String,String> producer = new KafkaProducer<String,String>(prop);
        for (int i=1; i<1000; i++) {
            // Create the Producer Record
            ProducerRecord<String, String> record =
                    // new ProducerRecord<>("java-topic", "key_"+i, "value_"+i);
                    new ProducerRecord<>("java-topic-2", "key_"+i, createRandomWord(15)+"value_"+i);
                    // can be created without key, but value is necessary

            // Send Data - Asynchronous
            // producer.send(record);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("\nReceived record metadata. (Producer) \n" +
                                "Topic: " + recordMetadata.topic() +
                                ", Partition: " + recordMetadata.partition() + ", " +
                                "Offset: " + recordMetadata.offset() + " @ Timestamp: " + recordMetadata.timestamp() + "\n"
                        );
                    } else {
                        logger.error("Error Occurred:", e);
                    }
                }
            });
        }
        //flush and close producer
        producer.flush();
        producer.close();
    }
}