package edu.supmti.kafka;

import java.util.Properties;
import java.util.HashMap;
import java.util.Map;
import java.time.Duration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class WordCountConsumer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "wordcount-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        try {
            consumer.subscribe(java.util.Arrays.asList("WordCount-Topic"));

            Map<String, Integer> wordCount = new HashMap<>();

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String word = record.key();
                    wordCount.put(word, wordCount.getOrDefault(word, 0) + 1);
                }

                System.out.println("\n--- COMPTAGE EN TEMPS RÉEL ---");
                wordCount.forEach((w, c) -> System.out.println(w + " : " + c));
            }
        } finally {
            consumer.close(); 
        }
    }
}