package com.dhruv;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.BasicConfigurator;

import java.util.Properties;

public class SimpleStringProducer {

    public static void main(String[] args) {
        BasicConfigurator.configure();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("mytopic", "value-" + i);
            producer.send(record);

            try {
                Thread.sleep(250);
            } catch (InterruptedException ie) {
                System.out.print(ie);
            }
        }

        producer.close();
    }
}
