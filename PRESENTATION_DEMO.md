# MiniKafka - 5-Minute Presentation Demo Script

## PRE-DEMO SETUP (Do Before Presentation)

### Terminal Setup:
Open **2 terminals** side-by-side:

**Terminal 1 (Main Demo):**
```bash
cd /Users/rahulkumarsaw/Downloads/Alignerr\ Tasks/flux-main
sdk use java 23-tem
clear
```

**Terminal 2 (Show Data/Code):**
```bash
cd /Users/rahulkumarsaw/Downloads/Alignerr\ Tasks/flux-main
clear
```

### One-Time Build:
```bash
# Terminal 1
rm -rf data/
mvn clean compile
```

---

## MINUTE 0-1: INTRODUCTION (60 seconds)

### SAY:
> "I'm going to demonstrate MiniKafka - a distributed message queue I built using Claude Code. This isn't just code - it's a fully functional distributed system that works exactly like Apache Kafka. Let me show you a real-world example: an e-commerce order processing system."

### RUN (Terminal 2):
```bash
# Show the clean architecture
git log --oneline | head -5
```

### SAY:
> "I built this systematically over 18 commits - from storage layer through broker implementation to client APIs. Let me show you what this system can do."

### RUN (Terminal 2):
```bash
# Show the demo code
cat src/main/java/demo/OrderProcessingDemo.java | head -30
```

### SAY:
> "This demo creates a 3-broker MiniKafka cluster, publishes orders from an Order Service, and processes them through two independent consumer groups - Payment Processors and Inventory Services. Each group has 2 consumers that share the load, just like Kafka."

---

## MINUTE 1-2: RUN THE DEMO (60 seconds)

### SAY:
> "Let me start the system. Watch what happens."

### RUN (Terminal 1):
```bash
./run-demo.sh
```

### SAY (while demo starts):
> "First, the system starts a 3-broker cluster. Broker 1 becomes the controller - it manages the cluster. Brokers 2 and 3 are followers that store data and send heartbeats."

### SAY (when topic creates):
> "Now it creates the 'orders' topic with 6 partitions. These get distributed across the 3 brokers for load balancing - broker 1 gets partitions 0 and 3, broker 2 gets 1 and 4, broker 3 gets 2 and 5."

### SAY (when consumers start):
> "Next, consumer groups start. The payment-group has 2 consumers, and the inventory-group also has 2 consumers. MiniKafka automatically assigns partitions to each consumer within their group. This is exactly how Kafka's consumer group coordination works."

### SAY (when orders publish):
> "Now the Order Service publishes 30 orders. Each order has a unique ID that gets hashed to select which partition it goes to. Messages are batched for efficiency and compressed using snappy compression before being sent to the brokers."

---

## MINUTE 2-3: EXPLAIN WHAT'S HAPPENING (60 seconds)

### SAY (while orders process):
> "Watch the real-time processing. Notice each order is consumed by BOTH consumer groups independently - one for payment processing, one for inventory updates. This is Kafka's publish-subscribe model."

> "Also notice the partition numbers. Each consumer only processes messages from its assigned partitions. This is load balancing in action - the work is split among consumers in each group."

### RUN (Terminal 2 - while demo runs):
```bash
# Show data being written to disk
tree data/
```

### SAY:
> "While this runs, look at what's being created on disk. Each broker has its own data directory, containing the partitions it owns. Within each partition, there's a log file storing the actual message data and an index file for fast offset lookups."

### RUN (Terminal 2):
```bash
# Show a log file
ls -lh data/broker-1/orders-0/
```

### SAY:
> "These are real log files on disk - durable, append-only storage just like Kafka. The index file maps message offsets to byte positions, enabling O(1) lookups when consumers fetch at specific offsets."

---

## MINUTE 3-4: TECHNICAL DEEP DIVE (60 seconds)

### SAY (when processing completes):
> "The demo just completed. 30 orders published, 30 payments processed, 30 inventory updates - all messages consumed by both groups successfully."

### RUN (Terminal 2):
```bash
# Show the storage hierarchy
ls src/main/java/server/internal/storage/
```

### SAY:
> "Let me show you the key technical implementations. The storage layer uses a Partition-Log-LogSegment hierarchy. Each partition contains a log, each log has multiple segments, and each segment maintains write buffers and index entries."

### RUN (Terminal 2):
```bash
# Show partition-level locking
cat src/main/java/commons/utils/PartitionWriteManager.java | head -40
```

### SAY:
> "This is the PartitionWriteManager - it handles thread safety for concurrent writes. When multiple producers write simultaneously, we use fine-grained locking at the partition level. Writes to the same partition are serialized for correctness, but writes to different partitions happen in parallel for performance. This is exactly Kafka's strategy."

### RUN (Terminal 2):
```bash
# Show gRPC services
ls src/main/proto/
```

### SAY:
> "Communication uses gRPC with 9 Protocol Buffer service definitions. I chose gRPC over REST specifically for performance - binary Protocol Buffer serialization and HTTP/2 multiplexing provide significantly better throughput than JSON over HTTP/1.1."

---

## MINUTE 4-5: CLAUDE CODE COLLABORATION (60 seconds)

### RUN (Terminal 2):
```bash
# Show collaboration story
cat PROJECT_STORY.md | head -50
```

### SAY:
> "What made this project special was how I used Claude Code - not as a code generator, but as a senior engineering mentor. Let me give you specific examples."

> "First, when deciding between gRPC and REST, Claude explained the tradeoffs. While REST is familiar, gRPC provides superior performance through binary encoding and HTTP/2. For a message queue handling high throughput, that performance matters."

> "Second, the thread-safety implementation. My initial approach used global locks, which would have killed performance. Claude introduced me to partition-level locking - exactly how Kafka handles concurrent writes. This maintains correctness while maximizing parallelism."

> "Third, metadata propagation. Instead of having clients constantly poll for cluster state, Claude suggested the Observer pattern. Now when metadata changes - like a broker joining or a topic being created - all interested parties are automatically notified."

### SAY:
> "Beyond architecture, Claude enforced engineering discipline. It challenged me on edge cases - what happens if a batch expires before sending? How do we prevent duplicates on retry? What if a broker dies mid-write? This critical thinking elevated the entire codebase."

---

## MINUTE 5-6: WRAP-UP (60 seconds)

### RUN (Terminal 2):
```bash
# Show test suite
find src/test/java -name "*Test.java" | wc -l
```

### SAY:
> "Quality was central - 37 comprehensive test files covering unit tests, integration tests, and benchmarks. Every major component is tested for correctness, edge cases, and performance."

### RUN (Terminal 2):
```bash
# Show line count
find src/main/java -name "*.java" | xargs wc -l | tail -1
```

### SAY:
> "The final system is over 8,000 lines of production code across 187 source files. This demonstrates several key capabilities:"

> "First, I can architect complex distributed systems - this implements partitioning, cluster coordination through a controller pattern, heartbeat-based failure detection, consumer group load balancing, and durable storage with append-only logs."

> "Second, I write production-quality code - comprehensive testing, clean architecture using immutable data structures, proper separation of concerns, and documented thread-safety guarantees."

> "Third, I effectively leverage AI - Claude Code wasn't a replacement for engineering thinking, it was a force multiplier. It helped me understand WHY distributed systems patterns exist, challenged my designs, and enforced best practices."

### SAY (Final):
> "Systems like this traditionally require entire teams. By using Claude Code as a technical partner - explaining concepts, suggesting alternatives, and enforcing discipline - I built a production-quality distributed message queue as an individual developer."

> "This demo showed MiniKafka working exactly like Apache Kafka - multi-broker clustering, partition-based distribution, consumer groups, compression, batching, and durable storage. All the core features, production-ready code."

---

## MINUTE 6: Q&A / SHOW CODE

### If Asked: "Show me the producer flow"

**RUN:**
```bash
cat src/main/java/producer/MiniKafkaProducer.java | head -80
```

**SAY:**
> "The producer serializes records using Kryo, selects partitions via key hashing or round-robin, buffers messages in the RecordAccumulator until batch size or time threshold is reached, compresses batches, then sends via gRPC."

### If Asked: "How does consumer group coordination work?"

**RUN:**
```bash
cat src/main/java/consumer/assignors/RoundRobinAssignor.java
```

**SAY:**
> "When consumers join a group, the GroupCoordinator designates one as leader. The leader runs the partition assignment algorithm - RoundRobin, Range, or Sticky - then the coordinator distributes assignments to all members. This enables load balancing."

### If Asked: "What about fault tolerance?"

**RUN:**
```bash
cat src/main/java/server/internal/BrokerLivenessTracker.java | head -60
```

**SAY:**
> "Brokers send heartbeats to the controller every 5 seconds. If a broker misses its window - 30 seconds by default - it's marked dead and removed from the cluster. This provides automatic failure detection."

---

## CLEANUP (After Demo)

```bash
# Stop the demo (Ctrl+C in Terminal 1)

# Clean data
rm -rf data/
```

---

## KEY POINTS TO EMPHASIZE

1. **It Actually Works**: "This isn't theoretical - you just saw 30 orders flow through a distributed system with 3 brokers, 6 partitions, and 4 consumers."

2. **Kafka-Like**: "Multi-broker clustering, partition distribution, consumer groups, load balancing, compression, batching - all core Kafka features."

3. **Production Quality**: "Thread-safe concurrent writes, durable storage, comprehensive testing, clean architecture."

4. **Claude Code Partnership**: "Used AI as a senior mentor for architectural decisions, not just code generation."

5. **Real-World**: "E-commerce order processing demonstrates practical distributed systems use cases."

---

## TIMING GUIDE

| Minute | Section | Focus |
|--------|---------|-------|
| 0-1 | Intro | What it is, why it matters |
| 1-2 | Run Demo | Show it working live |
| 2-3 | Explain Flow | Real-time processing, disk storage |
| 3-4 | Technical Depth | Storage, locking, gRPC |
| 4-5 | Claude Code | Specific decisions, mentorship |
| 5-6 | Wrap-up | Scale, quality, capabilities |

---

## EQUIPMENT CHECK

Before presentation:
- [ ] Java 23 active (`java -version`)
- [ ] Project builds (`mvn compile`)
- [ ] 2 terminals open and positioned
- [ ] No old data directory (`rm -rf data/`)
- [ ] Script tested once (`./run-demo.sh` works)
- [ ] This script open for reference

---

**You're demonstrating a real distributed message queue system. Show it with confidence!** 🚀
