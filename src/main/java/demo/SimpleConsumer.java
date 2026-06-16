package demo;

import consumer.MiniKafkaConsumer;
import consumer.ConsumerRecord;
import consumer.PollResult;
import java.time.Duration;
import java.util.Arrays;
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

        MiniKafkaConsumer<String, String> consumer = new MiniKafkaConsumer<>();
        consumer.subscribe(Arrays.asList("demo-topic"));

        System.out.println("Consuming messages from 'demo-topic'...");
        System.out.println("Press Ctrl+C to stop\n");

        while (true) {
            PollResult pollResult = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : pollResult.records()) {
                System.out.printf("✓ Received [partition=%d, offset=%d]: %s%n",
                    record.getPartition(), record.getOffset(), record.getValue());
            }
        }
    }
}
