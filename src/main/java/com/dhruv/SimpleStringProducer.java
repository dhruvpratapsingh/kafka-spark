package com.dhruv;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.BasicConfigurator;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SimpleStringProducer extends Thread {

    private static final String fileName = "dataset/apache-access-logs.txt";
    private static final String topicName = "mytopic";
    private static final Boolean isAsync = true;

    public static void main(String[] args) {
        BasicConfigurator.configure();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int lineCount = 0;
        FileInputStream fis;
        BufferedReader br = null;

        try {
            fis = new FileInputStream(fileName);
            br = new BufferedReader(new InputStreamReader(fis));

            String line;
            String IPADDRESS_PATTERN =
                "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

            Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
            while ((line = br.readLine()) != null) {
                String IP = getIPAddress(line, pattern);
                lineCount++;
                sendMessage(lineCount+"", IP, producer);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally{
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }

    private static String getIPAddress(String line, Pattern pattern) {
        Matcher matcher = pattern.matcher(line);

        if (matcher.find()) {
            return matcher.group();
        } else{
            return "0.0.0.0";
        }
    }

    private static void sendMessage(String key, String value, KafkaProducer<String, String> producer) {
        long startTime = System.currentTimeMillis();
        if (isAsync) {
            producer.send(
                new ProducerRecord<>(topicName, value),
                (Callback) new DemoCallBack(startTime, key, value));
        } else {
            try {
                producer.send(new ProducerRecord<>(topicName, key, value)).get();
                System.out.println("Sent message: (" + key + ", " + value + ")");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}

class DemoCallBack implements Callback {

    private long startTime;
    private String key;
    private String message;

    DemoCallBack(long startTime, String key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling
     * of request completion. This method will be called when the record sent to
     * the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata
     *            The metadata for the record that was sent (i.e. the partition
     *            and offset). Null if an error occurred.
     * @param exception
     *            The exception thrown during processing of this record. Null if
     *            no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println("message(" + key + ", " + message
                + ") sent to partition(" + metadata.partition() + "), "
                + "offset(" + metadata.offset() + ") in " + elapsedTime
                + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
