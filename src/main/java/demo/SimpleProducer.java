package demo;

import producer.MiniKafkaProducer;
import producer.ProducerRecord;
import java.util.Properties;

/**
 * Simple producer for manual demo
 * Usage: mvn exec:java -Dexec.mainClass="demo.SimpleProducer" -Dexec.args="<message>"
 */
public class SimpleProducer {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: Provide message as argument");
            System.out.println("Example: mvn exec:java -Dexec.mainClass=\"demo.SimpleProducer\" -Dexec.args=\"Hello Kafka\"");
            System.exit(1);
        }

        String message = String.join(" ", args);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:50051");
        props.put("compression.type", "snappy");

        MiniKafkaProducer<String, String> producer = new MiniKafkaProducer<>(props);

        ProducerRecord<String, String> record = new ProducerRecord<>(
            "demo-topic",
            null, // no key, will use round-robin
            message
        );

        producer.send(record);
        producer.close(); // MUST close to flush

        System.out.println("✓ Sent: " + message);
    }
}
