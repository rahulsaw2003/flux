# MiniKafka - Simple Demo Guide

## What This Demo Shows

A **real-world e-commerce order processing system** that demonstrates MiniKafka working exactly like Apache Kafka:

- **Order Service** publishes 30 orders to MiniKafka
- **Payment Processors** (Consumer Group 1) process payments - 2 consumers sharing the load
- **Inventory Service** (Consumer Group 2) updates inventory - 2 consumers sharing the load
- Each order is consumed by **BOTH** consumer groups independently (like Kafka's pub-sub model)
- Messages distributed across **6 partitions** on **3 brokers**

## Prerequisites (One-Time Setup)

### 1. Install Java 23

```bash
# Install SDKMAN
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"

# Install and use Java 23
sdk install java 23-tem
sdk use java 23-tem

# Verify
java -version
# Must show: openjdk version "23"
```

### 2. Navigate to Project

```bash
cd /Users/rahulkumarsaw/Downloads/Alignerr\ Tasks/flux-main
```

## Running the Demo (2 Methods)

### Method 1: One-Command Run (Easiest)

```bash
./run-demo.sh
```

This script:
1. Checks Java version
2. Cleans old data
3. Builds the project
4. Runs the demo

### Method 2: Manual Run

```bash
# 1. Clean old data
rm -rf data/

# 2. Build
mvn clean compile

# 3. Run
mvn exec:java -Dexec.mainClass="Main"
```

## What You'll See

### Console Output:

```
╔════════════════════════════════════════════════════════════════╗
║  MiniKafka Demo: E-commerce Order Processing System          ║
╚════════════════════════════════════════════════════════════════╝

─────────────────────────────────────────────────────────────────
Step 1: Initializing MiniKafka Cluster (3 brokers)
─────────────────────────────────────────────────────────────────
   ✓ Cluster started with 3 brokers
   ✓ Controller: localhost:50051
   ✓ Followers: localhost:50052, localhost:50053

─────────────────────────────────────────────────────────────────
Step 2: Creating Topics
─────────────────────────────────────────────────────────────────
   ✓ Topic 'orders' created with 6 partitions
   ✓ Partitions distributed across 3 brokers

─────────────────────────────────────────────────────────────────
Step 3: Starting Consumer Groups
─────────────────────────────────────────────────────────────────
   ✓ Consumer Group 'payment-group' started (2 consumers)
   ✓ Consumer Group 'inventory-group' started (2 consumers)
   ✓ Each consumer will receive different partitions

─────────────────────────────────────────────────────────────────
Step 4: Order Service Publishing Orders
─────────────────────────────────────────────────────────────────
   🛒 Published: ORD-0001 | Alice | Laptop x1
   🛒 Published: ORD-0002 | Bob | Phone x2
   🛒 Published: ORD-0003 | Charlie | Tablet x3
   ...
   ✓ All 30 orders published and flushed

─────────────────────────────────────────────────────────────────
Step 5: Watching Real-time Order Processing
─────────────────────────────────────────────────────────────────
   (Each order processed by BOTH consumer groups independently)

💳 [Payment Processor 1] Processed payment for ORD-0001 from partition 2
📦 [Inventory Service 2] Updated inventory for ORD-0001 from partition 2
💳 [Payment Processor 2] Processed payment for ORD-0002 from partition 4
📦 [Inventory Service 1] Updated inventory for ORD-0002 from partition 4
...

─────────────────────────────────────────────────────────────────
Step 6: Processing Complete - Results
─────────────────────────────────────────────────────────────────
   📊 Total Orders Published: 30
   💳 Payments Processed: 30
   📦 Inventory Updates: 30
   ✓ All orders processed by both systems

╔════════════════════════════════════════════════════════════════╗
║  MiniKafka Features Demonstrated (Just Like Apache Kafka)    ║
╚════════════════════════════════════════════════════════════════╝
✓ Multi-broker cluster with controller election
✓ Partition-based message distribution (6 partitions)
✓ Consumer groups with automatic partition assignment
✓ Load balancing across multiple consumers per group
✓ Independent consumption by different consumer groups
✓ Message batching and compression (snappy)
✓ Durable storage with append-only logs
✓ gRPC-based high-performance communication

Press Ctrl+C to exit...
```

### Data Created on Disk:

```bash
# While demo is running, open another terminal and check:
tree data/

# You'll see:
data/
├── broker-1/
│   ├── orders-0/
│   │   ├── 00000000000000000000.log    (partition 0 messages)
│   │   └── 00000000000000000000.index  (offset index)
│   └── orders-3/
│       ├── 00000000000000000000.log    (partition 3 messages)
│       └── 00000000000000000000.index
├── broker-2/
│   ├── orders-1/
│   └── orders-4/
└── broker-3/
    ├── orders-2/
    └── orders-5/

# Each partition has messages distributed by key hash
```

## Understanding What's Happening

### 1. Cluster Startup
- 3 brokers start on ports 50051, 50052, 50053
- First broker becomes **controller** (manages cluster)
- Other brokers are **followers** (store data, send heartbeats)

### 2. Topic Creation
- Topic "orders" created with **6 partitions**
- Partitions distributed evenly:
  - Broker 1: partitions 0, 3
  - Broker 2: partitions 1, 4
  - Broker 3: partitions 2, 5

### 3. Consumer Groups Start
- **Payment Group**: 2 consumers split partitions (e.g., Consumer 1 gets [0,1,2], Consumer 2 gets [3,4,5])
- **Inventory Group**: 2 consumers also split partitions independently
- Each group consumes ALL messages, but work is split within each group

### 4. Orders Published
- Order Service sends 30 orders
- Each order has a key (orderId) → hashed to select partition
- Messages batched and compressed (snappy) before sending
- All messages durably stored on disk

### 5. Consumers Process
- Each consumer polls from its assigned partitions
- **Payment processors** handle payment logic
- **Inventory services** update stock
- Same message consumed by both groups (pub-sub pattern)

## Key Kafka-Like Features Shown

| Feature | Demonstrated | Just Like Kafka? |
|---------|-------------|------------------|
| Multi-broker cluster | ✓ 3 brokers | ✓ Yes |
| Partition distribution | ✓ 6 partitions across 3 brokers | ✓ Yes |
| Consumer groups | ✓ 2 groups, 2 consumers each | ✓ Yes |
| Load balancing | ✓ Partitions split among consumers | ✓ Yes |
| Independent consumption | ✓ Both groups get all messages | ✓ Yes |
| Message persistence | ✓ Stored in log files on disk | ✓ Yes |
| Compression | ✓ Snappy compression | ✓ Yes |
| Batching | ✓ Messages batched before send | ✓ Yes |

## Stopping the Demo

Press `Ctrl+C` in the terminal.

## Cleanup

```bash
# Remove data files
rm -rf data/

# Clean build artifacts
mvn clean
```

## Troubleshooting

### "invalid target release: 23"
**Solution:** Use Java 23
```bash
sdk use java 23-tem
```

### "Port already in use"
**Solution:** Kill old processes
```bash
for port in 50051 50052 50053; do
    lsof -ti:$port | xargs kill -9 2>/dev/null
done
```

### "BUILD FAILURE"
**Solution:** Check Java version and rebuild
```bash
java -version  # Must be 23
mvn clean compile
```

### Consumer not receiving messages
**Solution:** Ensure producer.close() is called to flush batches.

## What Makes This a Good Demo

1. **Real-world scenario**: E-commerce order processing is relatable
2. **Visual feedback**: See orders being published and consumed in real-time
3. **Multiple consumers**: Shows load balancing across consumer groups
4. **Kafka semantics**: Demonstrates publish-subscribe model correctly
5. **Durable storage**: Data persists to disk like real Kafka
6. **Performance features**: Shows batching, compression, partitioning

## For Your Demo Presentation

### What to Say:

> "This is a real-world order processing system built on MiniKafka. We have an Order Service publishing orders, Payment Processors consuming and processing payments, and Inventory Services updating stock. All running on a 3-broker MiniKafka cluster with 6 partitions."

> "Watch how the orders get distributed across partitions based on their order ID, and how each consumer group independently processes all messages. This is exactly how Kafka's publish-subscribe model works."

> "The system handles 30 orders, distributes them across 6 partitions on 3 brokers, and processes them through 2 consumer groups with 2 consumers each - demonstrating load balancing, partitioning, and independent consumption."

### What to Show:

1. **Run the demo**: `./run-demo.sh`
2. **Point out real-time processing**: Orders published, then consumed by both groups
3. **Show disk files** (in another terminal while demo runs):
   ```bash
   tree data/
   ls -lh data/broker-1/orders-0/
   ```
4. **Explain features**: "This demonstrates multi-broker clustering, partition-based distribution, consumer groups, compression, batching - all core Kafka features."

## Advanced: Modify the Demo

Want to change the demo? Edit `src/main/java/demo/OrderProcessingDemo.java`:

- `NUM_ORDERS`: Change number of orders (default: 30)
- `ORDERS_TOPIC`: Change topic name
- Number of partitions: Line with `new NewTopic(ORDERS_TOPIC, 6, 1)`
- Add more consumer groups: Copy the `startConsumer()` pattern

Then rebuild and run:
```bash
mvn clean compile
./run-demo.sh
```
