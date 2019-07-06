package com.github.guilhermealves.kafka.apps.v2;

import java.util.Collections;
import java.util.Properties;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MyKafkaAvroJavaConsumerV2 {

    public static void main (String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "my-avro-consumer-v2");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");
        properties.setProperty("specific.avro.reader", "true");

        try (KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(properties)) {

            String topic = "customer-avro";
            consumer.subscribe(Collections.singleton(topic));

            System.out.println("Waiting for data...");

            while (true) {
                ConsumerRecords<String, Customer> records = consumer.poll(500);
                records.forEach(record -> System.out.println(record.value()));
                consumer.commitSync();
            }
        }

    }
}
