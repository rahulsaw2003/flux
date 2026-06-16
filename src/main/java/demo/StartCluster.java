package demo;

import admin.MiniKafkaAdminClient;
import admin.NewTopic;
import java.util.Arrays;
import java.util.Properties;

/**
 * Start MiniKafka cluster for demo
 * Usage: mvn exec:java -Dexec.mainClass="demo.StartCluster"
 */
public class StartCluster {
    public static void main(String[] args) throws Exception {
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║         Starting MiniKafka Cluster                  ║");
        System.out.println("╚══════════════════════════════════════════════════════╝\n");

        // Start 3-broker cluster
        System.out.println("Starting 3-broker cluster...");
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:50051,localhost:50052,localhost:50053");
        props.put("broker.count", "3");

        MiniKafkaAdminClient admin = MiniKafkaAdminClient.create(props);

        // Wait for cluster to initialize
        Thread.sleep(3000);
        System.out.println("✓ Cluster started");
        System.out.println("  Broker 1 (Controller): localhost:50051");
        System.out.println("  Broker 2: localhost:50052");
        System.out.println("  Broker 3: localhost:50053\n");

        // Create demo topic
        System.out.println("Creating topic 'demo-topic' with 3 partitions...");
        admin.createTopics(Arrays.asList(new NewTopic("demo-topic", 3, 1)));

        Thread.sleep(1000);
        System.out.println("✓ Topic 'demo-topic' created\n");

        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║         Cluster Ready!                              ║");
        System.out.println("╚══════════════════════════════════════════════════════╝\n");

        System.out.println("You can now:");
        System.out.println("1. Produce messages: mvn exec:java -Dexec.mainClass=\"demo.SimpleProducer\" -Dexec.args=\"Your message\"");
        System.out.println("2. Consume messages: mvn exec:java -Dexec.mainClass=\"demo.SimpleConsumer\"");
        System.out.println("3. Run full demo: ./run-demo.sh\n");

        System.out.println("Press Ctrl+C to stop the cluster...");

        // Keep cluster running
        Thread.sleep(Long.MAX_VALUE);
    }
}
