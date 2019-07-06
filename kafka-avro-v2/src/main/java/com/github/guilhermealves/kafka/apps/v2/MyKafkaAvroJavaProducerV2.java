package com.github.guilhermealves.kafka.apps.v2;

import java.util.Properties;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class MyKafkaAvroJavaProducerV2 {

    public static void main (String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "10");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");

        try (KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(properties)) {
            String topic = "customer-avro";

            Customer customer = Customer.newBuilder()
                    .setFirstName("Mark")
                    .setLastName("Johnson")
                    .setAge(31)
                    .setHeight(192.5f)
                    .setWeight(105.1f)
                    .setPhoneNumber("(11) 99999-9999")
                    .setEmail("mark.joshson@com")
                    .build();

            ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(topic, customer);

            kafkaProducer.send(producerRecord, (metaData, exception) -> {
                if (null == exception) {
                    System.out.println("Success!");
                    System.out.println(metaData);
                    return;
                }

                exception.printStackTrace();
            });

            kafkaProducer.flush();
        }
    }
}
