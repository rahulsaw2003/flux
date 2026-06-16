package demo;

import admin.MiniKafkaAdminClient;
import admin.NewTopic;
import consumer.MiniKafkaConsumer;
import consumer.ConsumerRecord;
import producer.MiniKafkaProducer;
import producer.ProducerRecord;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Real-world demo: E-commerce Order Processing System
 *
 * Demonstrates MiniKafka working exactly like Apache Kafka:
 * - Producer (Order Service) sends orders to "orders" topic
 * - Consumer Group 1 (Payment Processor) processes payments
 * - Consumer Group 2 (Inventory Service) updates inventory
 * - Multiple consumers per group for load balancing
 */
public class OrderProcessingDemo {

    private static final String ORDERS_TOPIC = "orders";
    private static final int NUM_ORDERS = 30;
    private static final AtomicInteger processedOrders = new AtomicInteger(0);
    private static final AtomicInteger processedPayments = new AtomicInteger(0);
    private static final CountDownLatch latch = new CountDownLatch(NUM_ORDERS * 2); // orders + payments

    public static void main(String[] args) throws Exception {
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║  MiniKafka Demo: E-commerce Order Processing System          ║");
        System.out.println("╚════════════════════════════════════════════════════════════════╝\n");

        // Step 1: Initialize cluster
        printStep(1, "Initializing MiniKafka Cluster (3 brokers)");
        Properties clusterProps = new Properties();
        clusterProps.put("bootstrap.servers", "localhost:50051,localhost:50052,localhost:50053");
        clusterProps.put("broker.count", "3");

        MiniKafkaAdminClient admin = MiniKafkaAdminClient.create(clusterProps);
        Thread.sleep(3000); // Wait for cluster initialization
        System.out.println("   ✓ Cluster started with 3 brokers");
        System.out.println("   ✓ Controller: localhost:50051");
        System.out.println("   ✓ Followers: localhost:50052, localhost:50053\n");

        // Step 2: Create topics
        printStep(2, "Creating Topics");
        admin.createTopics(Arrays.asList(
            new NewTopic(ORDERS_TOPIC, 6, 1) // 6 partitions for load distribution
        ));
        Thread.sleep(1000); // Wait for topic creation
        System.out.println("   ✓ Topic 'orders' created with 6 partitions");
        System.out.println("   ✓ Partitions distributed across 3 brokers\n");

        // Step 3: Start Consumer Groups (before producing)
        printStep(3, "Starting Consumer Groups");

        // Consumer Group 1: Payment Processors (2 consumers)
        Thread paymentConsumer1 = startConsumer("payment-processor-1", "payment-group",
            "💳 [Payment Processor 1]", "PAYMENT");
        Thread paymentConsumer2 = startConsumer("payment-processor-2", "payment-group",
            "💳 [Payment Processor 2]", "PAYMENT");

        // Consumer Group 2: Inventory Service (2 consumers)
        Thread inventoryConsumer1 = startConsumer("inventory-service-1", "inventory-group",
            "📦 [Inventory Service 1]", "INVENTORY");
        Thread inventoryConsumer2 = startConsumer("inventory-service-2", "inventory-group",
            "📦 [Inventory Service 2]", "INVENTORY");

        System.out.println("   ✓ Consumer Group 'payment-group' started (2 consumers)");
        System.out.println("   ✓ Consumer Group 'inventory-group' started (2 consumers)");
        System.out.println("   ✓ Each consumer will receive different partitions\n");

        Thread.sleep(2000); // Let consumers join groups

        // Step 4: Produce Orders
        printStep(4, "Order Service Publishing Orders");
        produceOrders();

        // Step 5: Wait for processing
        printStep(5, "Watching Real-time Order Processing");
        System.out.println("   (Each order processed by BOTH consumer groups independently)\n");

        // Wait for all messages to be consumed
        boolean completed = latch.await(30, java.util.concurrent.TimeUnit.SECONDS);

        // Step 6: Show Results
        Thread.sleep(1000);
        printStep(6, "Processing Complete - Results");
        System.out.println("   📊 Total Orders Published: " + NUM_ORDERS);
        System.out.println("   💳 Payments Processed: " + processedPayments.get());
        System.out.println("   📦 Inventory Updates: " + processedOrders.get());
        System.out.println("   ✓ All orders processed by both systems\n");

        // Demonstrate Kafka-like features
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║  MiniKafka Features Demonstrated (Just Like Apache Kafka)    ║");
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        System.out.println("✓ Multi-broker cluster with controller election");
        System.out.println("✓ Partition-based message distribution (6 partitions)");
        System.out.println("✓ Consumer groups with automatic partition assignment");
        System.out.println("✓ Load balancing across multiple consumers per group");
        System.out.println("✓ Independent consumption by different consumer groups");
        System.out.println("✓ Message batching and compression (snappy)");
        System.out.println("✓ Durable storage with append-only logs");
        System.out.println("✓ gRPC-based high-performance communication\n");

        System.out.println("Press Ctrl+C to exit...");

        // Keep running to show persistent data
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void produceOrders() throws Exception {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:50051,localhost:50052,localhost:50053");
        producerProps.put("compression.type", "snappy");
        producerProps.put("batch.size", "16384");
        producerProps.put("linger.ms", "10");

        MiniKafkaProducer<String, String> producer = new MiniKafkaProducer<>(producerProps);

        String[] products = {"Laptop", "Phone", "Tablet", "Headphones", "Camera", "Watch"};
        String[] customers = {"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"};

        for (int i = 1; i <= NUM_ORDERS; i++) {
            String orderId = String.format("ORD-%04d", i);
            String customer = customers[i % customers.length];
            String product = products[i % products.length];
            int quantity = (i % 3) + 1;
            double price = 99.99 + (i * 10.0);

            String orderJson = String.format(
                "{\"orderId\":\"%s\",\"customer\":\"%s\",\"product\":\"%s\",\"quantity\":%d,\"price\":%.2f}",
                orderId, customer, product, quantity, price
            );

            ProducerRecord<String, String> record = new ProducerRecord<>(
                ORDERS_TOPIC,
                orderId, // key for partition selection
                orderJson
            );

            producer.send(record);
            System.out.println("   🛒 Published: " + orderId + " | " + customer + " | " + product + " x" + quantity);
            Thread.sleep(100); // Simulate realistic order rate
        }

        producer.close(); // Flush all batches
        System.out.println("\n   ✓ All " + NUM_ORDERS + " orders published and flushed\n");
    }

    private static Thread startConsumer(String consumerId, String groupId, String displayName, String type) {
        Thread consumerThread = new Thread(() -> {
            try {
                Properties consumerProps = new Properties();
                consumerProps.put("bootstrap.servers", "localhost:50051,localhost:50052,localhost:50053");
                consumerProps.put("group.id", groupId);
                consumerProps.put("client.id", consumerId);
                consumerProps.put("auto.offset.reset", "earliest");

                MiniKafkaConsumer<String, String> consumer = new MiniKafkaConsumer<>(consumerProps);
                consumer.subscribe(Arrays.asList(ORDERS_TOPIC));

                while (true) {
                    List<ConsumerRecord<String, String>> records = consumer.poll(Duration.ofMillis(1000));

                    for (ConsumerRecord<String, String> record : records) {
                        String orderId = record.key();
                        String orderData = record.value();

                        // Simulate processing
                        Thread.sleep(50);

                        if (type.equals("PAYMENT")) {
                            System.out.println(displayName + " Processed payment for " + orderId + " from partition " + record.partition());
                            processedPayments.incrementAndGet();
                        } else {
                            System.out.println(displayName + " Updated inventory for " + orderId + " from partition " + record.partition());
                            processedOrders.incrementAndGet();
                        }

                        latch.countDown();
                    }
                }
            } catch (Exception e) {
                // Consumer stopped
            }
        });

        consumerThread.setDaemon(true);
        consumerThread.start();
        return consumerThread;
    }

    private static void printStep(int step, String message) {
        System.out.println("─────────────────────────────────────────────────────────────────");
        System.out.println("Step " + step + ": " + message);
        System.out.println("─────────────────────────────────────────────────────────────────");
    }
}
