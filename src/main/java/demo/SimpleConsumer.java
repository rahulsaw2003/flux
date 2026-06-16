package demo;

import consumer.MiniKafkaConsumer;
import consumer.ConsumerRecord;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Simple consumer for manual demo
 * Usage: mvn exec:java -Dexec.mainClass="demo.SimpleConsumer"
 */
public class SimpleConsumer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:50051");
        props.put("group.id", "demo-consumer-group");
        props.put("auto.offset.reset", "earliest");

        MiniKafkaConsumer<String, String> consumer = new MiniKafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("demo-topic"));

        System.out.println("Consuming messages from 'demo-topic'...");
        System.out.println("Press Ctrl+C to stop\n");

        while (true) {
            List<ConsumerRecord<String, String>> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("✓ Received [partition=%d, offset=%d]: %s%n",
                    record.partition(), record.offset(), record.value());
            }
        }
    }
}
