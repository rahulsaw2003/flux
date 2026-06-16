# MiniKafka Demo - Commands Reference

## Setup (One-Time)

```bash
# 1. Install Java 23
sdk install java 23-tem
sdk use java 23-tem

# 2. Navigate to project
cd /Users/rahulkumarsaw/Downloads/Alignerr\ Tasks/flux-main

# 3. Build
mvn clean compile
```

---

## Demo Option 1: Manual Producer/Consumer (Interactive)

### Step 1: Start Cluster (Terminal 1)

```bash
mvn exec:java -Dexec.mainClass="demo.StartCluster"
```

**Output:**
```
╔══════════════════════════════════════════════════════╗
║         Starting MiniKafka Cluster                  ║
╚══════════════════════════════════════════════════════╝

✓ Cluster started
  Broker 1 (Controller): localhost:50051
  Broker 2: localhost:50052
  Broker 3: localhost:50053

✓ Topic 'demo-topic' created

Cluster Ready! Press Ctrl+C to stop...
```

**Leave this running!**

---

### Step 2: Start Consumer (Terminal 2)

```bash
mvn exec:java -Dexec.mainClass="demo.SimpleConsumer"
```

**Output:**
```
Consuming messages from 'demo-topic'...
Press Ctrl+C to stop

(Waiting for messages...)
```

**Leave this running!**

---

### Step 3: Send Messages (Terminal 3)

```bash
# Send message 1
mvn exec:java -Dexec.mainClass="demo.SimpleProducer" -Dexec.args="Hello MiniKafka"

# Send message 2
mvn exec:java -Dexec.mainClass="demo.SimpleProducer" -Dexec.args="This is a distributed message queue"

# Send message 3
mvn exec:java -Dexec.mainClass="demo.SimpleProducer" -Dexec.args="Built with Claude Code"
```

**Each command outputs:**
```
✓ Sent: Hello MiniKafka
```

**Terminal 2 (Consumer) will show:**
```
✓ Received [partition=1, offset=0]: Hello MiniKafka
✓ Received [partition=2, offset=0]: This is a distributed message queue
✓ Received [partition=0, offset=0]: Built with Claude Code
```

---

### Step 4: Show Data on Disk (Terminal 3)

```bash
# See created data
tree data/

# Output:
# data/
# ├── broker-1/
# │   └── demo-topic-0/
# │       ├── 00000000000000000000.log
# │       └── 00000000000000000000.index
# ├── broker-2/
# │   └── demo-topic-1/
# └── broker-3/
#     └── demo-topic-2/

# Check log file size
ls -lh data/broker-1/demo-topic-0/
```

---

### Step 5: Cleanup

```bash
# Stop consumer (Terminal 2): Ctrl+C
# Stop cluster (Terminal 1): Ctrl+C
# Clean data (Terminal 3):
rm -rf data/
```

---

## Demo Option 2: Full E-Commerce Demo (Automated)

### One Command:

```bash
./run-demo.sh
```

**What it does:**
1. Starts 3-broker cluster
2. Creates 'orders' topic with 6 partitions
3. Starts 4 consumers (2 payment processors, 2 inventory services)
4. Publishes 30 e-commerce orders
5. Shows real-time processing
6. Displays results

**Output:**
```
╔══════════════════════════════════════════════════════════╗
║  MiniKafka Demo: E-commerce Order Processing System    ║
╚══════════════════════════════════════════════════════════╝

Step 1: Initializing MiniKafka Cluster (3 brokers)
   ✓ Cluster started with 3 brokers

Step 2: Creating Topics
   ✓ Topic 'orders' created with 6 partitions

Step 3: Starting Consumer Groups
   ✓ Consumer Group 'payment-group' started
   ✓ Consumer Group 'inventory-group' started

Step 4: Order Service Publishing Orders
   🛒 Published: ORD-0001 | Alice | Laptop x1
   🛒 Published: ORD-0002 | Bob | Phone x2
   ...

Step 5: Watching Real-time Order Processing
   💳 [Payment Processor 1] Processed payment for ORD-0001
   📦 [Inventory Service 2] Updated inventory for ORD-0001
   ...

Step 6: Processing Complete - Results
   📊 Total Orders Published: 30
   💳 Payments Processed: 30
   📦 Inventory Updates: 30
   ✓ All orders processed by both systems
```

---

## Quick Commands Summary

```bash
# Setup (once)
sdk use java 23-tem
mvn clean compile

# Option 1: Manual Demo
# Terminal 1: Start cluster
mvn exec:java -Dexec.mainClass="demo.StartCluster"

# Terminal 2: Start consumer
mvn exec:java -Dexec.mainClass="demo.SimpleConsumer"

# Terminal 3: Send messages
mvn exec:java -Dexec.mainClass="demo.SimpleProducer" -Dexec.args="Your message here"

# Option 2: Full Demo
./run-demo.sh

# Show data files
tree data/
ls -lh data/broker-1/demo-topic-0/

# Cleanup
rm -rf data/
```

---

## Troubleshooting

**"invalid target release: 23"**
```bash
sdk use java 23-tem
java -version  # Must show 23
```

**"Port already in use"**
```bash
for port in 50051 50052 50053; do
    lsof -ti:$port | xargs kill -9 2>/dev/null
done
```

**"No messages received"**
- Make sure cluster is running (Terminal 1)
- Make sure you called `producer.close()` (auto-done in SimpleProducer)
- Wait 1-2 seconds after producing

---

## What Each Demo Shows

### Option 1 (Manual):
✅ Interactive produce/consume
✅ See messages in real-time
✅ Understand basic flow
✅ Good for explaining step-by-step

### Option 2 (Full):
✅ Complete e-commerce system
✅ Multiple consumer groups
✅ Load balancing across consumers
✅ Real-world scenario
✅ Shows all Kafka features
✅ Good for impressive demo
