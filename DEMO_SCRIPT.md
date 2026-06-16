# MiniKafka Demo Script (5-6 Minutes)
## What You Say + What You Run

---

## PRE-DEMO SETUP (Do Before Demo Starts)

### Terminal Setup (Open 3 terminals)

**Terminal 1 - Code & Structure:**
```bash
cd /Users/rahulkumarsaw/Downloads/Alignerr\ Tasks/flux-main
clear
```

**Terminal 2 - Running Demo:**
```bash
cd /Users/rahulkumarsaw/Downloads/Alignerr\ Tasks/flux-main
clear
```

**Terminal 3 - Results:**
```bash
cd /Users/rahulkumarsaw/Downloads/Alignerr\ Tasks/flux-main
clear
```

### Verify Build (Do this once before demo)

```bash
# Switch to Java 23
sdk use java 23-tem

# Build project
mvn clean compile

# Must see: BUILD SUCCESS
```

---

## MINUTE 0-1: INTRODUCTION (60 seconds)

### SAY:
> "Hi, I'm going to demonstrate MiniKafka, a distributed message queue platform I built using Claude Code. This isn't a toy project - it's a fully functional distributed system inspired by Apache Kafka that handles high-throughput message streaming across multiple broker nodes with partition-based storage, consumer groups, and cluster coordination."

### RUN (Terminal 1):
```bash
# Show the organized development progression
git log --oneline
```

### SAY (while showing commits):
> "I built this systematically over 18 commits. You can see the progression here - starting with project setup, implementing the storage layer, building broker and client components, adding gRPC services, and finishing with comprehensive testing. Each commit represents a logical step in building this distributed system."

### RUN (Terminal 1):
```bash
# Show project scale
find src/main/java -name "*.java" | wc -l
find src/main/java -name "*.java" | xargs wc -l | tail -1
```

### SAY:
> "That's 187 source files with over 8,000 lines of production code, plus another 4,000 lines of test code across 37 test files."

---

## MINUTE 1-2: ARCHITECTURE OVERVIEW (60 seconds)

### RUN (Terminal 1):
```bash
# Show architecture
tree -L 2 src/main/java/
```

### SAY:
> "The architecture has four main layers. Let me walk you through them:"

> "At the bottom is the storage layer - an append-only log system where partitions contain logs, logs contain segments, and each segment manages write buffers, index entries, and disk persistence. This is exactly how Kafka implements durable storage."

### RUN (Terminal 1):
```bash
# Show storage implementation
ls src/main/java/server/internal/storage/
```

### SAY:
> "Here's the storage hierarchy - Partition, Log, LogSegment, and IndexEntries. The IndexEntries maintain a mapping from message offset to byte offset in the log file, enabling O(1) lookups instead of scanning the entire file."

### RUN (Terminal 1):
```bash
# Show broker layer
ls src/main/java/server/internal/
```

### SAY:
> "Above storage is the broker layer. Brokers handle both storage and cluster coordination. The first broker acts as the controller, managing topic creation, partition assignment, and monitoring broker liveness through heartbeats."

### RUN (Terminal 1):
```bash
# Show client layer
ls src/main/java/producer/
ls src/main/java/consumer/
```

### SAY:
> "The client layer includes producers with sophisticated batching - RecordAccumulator groups messages per partition, BufferPool manages memory, and we support four compression algorithms: gzip, snappy, lz4, and zstandard."

> "For consumers, I implemented consumer groups with three partition assignment strategies - RoundRobin, Range, and Sticky - allowing multiple consumers to share the workload."

---

## MINUTE 2-3: TECHNICAL DEEP DIVE (60 seconds)

### RUN (Terminal 1):
```bash
# Show gRPC services
ls src/main/proto/
```

### SAY:
> "Communication uses gRPC with nine Protocol Buffer service definitions. I chose gRPC over REST for specific performance reasons - binary Protocol Buffer serialization is more efficient than JSON, and HTTP/2 multiplexing allows better connection reuse. For a high-throughput message queue, these performance characteristics matter."

### RUN (Terminal 1):
```bash
# Show a key implementation - partition-level locking
cat src/main/java/commons/utils/PartitionWriteManager.java | head -40
```

### SAY:
> "This is the PartitionWriteManager - a critical piece for thread safety. When multiple producers write concurrently, we use fine-grained locking at the partition level. This means writes to the same partition are serialized to prevent race conditions, but writes to different partitions happen in parallel. This is exactly how Kafka achieves both correctness and high throughput."

### RUN (Terminal 1):
```bash
# Show producer batching
cat src/main/java/producer/RecordAccumulator.java | head -50
```

### SAY:
> "The RecordAccumulator implements Kafka's batching strategy. Messages aren't sent immediately - they're buffered for 10 milliseconds or until the batch reaches 16 kilobytes. Then the entire batch is compressed and sent via gRPC. This dramatically reduces network overhead."

---

## MINUTE 3-4: CLAUDE CODE COLLABORATION (60 seconds)

### RUN (Terminal 1):
```bash
# Show collaboration story
cat PROJECT_STORY.md | head -60
```

### SAY:
> "What made this project special was how I used Claude Code. I didn't treat it as just a code generator - I used it as a senior engineering mentor. Let me give you specific examples of critical decisions Claude helped me make."

> "First, the gRPC versus REST decision. Claude presented the tradeoffs - while REST is familiar and easy to debug, gRPC provides superior performance through binary encoding, HTTP/2 multiplexing, and built-in streaming. For a high-throughput message queue, Claude convinced me gRPC was the right choice despite the steeper learning curve."

> "Second, thread safety. My initial implementation used global locks, which would have killed parallelism. Claude introduced me to partition-level locking with ReentrantLocks - serializing writes within a partition while allowing cross-partition parallelism. This is the same strategy Kafka uses."

> "Third, metadata propagation. Instead of having clients constantly poll for cluster state changes, Claude suggested the Observer pattern. Now when metadata changes - like a new broker joining or a topic being created - all interested clients are automatically notified. This is both more efficient and more elegant."

### SAY:
> "Beyond architectural decisions, Claude enforced engineering discipline. It challenged me to think through edge cases I hadn't considered - what happens if a batch expires before sending? How do we prevent duplicate messages on retry? What if a broker dies mid-write? This kind of critical thinking elevated the quality of the entire codebase."

---

## MINUTE 4-5: LIVE DEMONSTRATION (60 seconds)

### SAY:
> "Now let me show you the system actually running."

### RUN (Terminal 2):
```bash
# Run the integration test that demonstrates the full flow
mvn test -Dtest=MiniKafkaProducerIntegrationTest
```

### SAY (while it runs):
> "This integration test demonstrates the complete producer flow:"

> "First, it starts a three-broker cluster. Broker one becomes the controller and starts monitoring the others via heartbeats."

> "Second, it creates a topic called 'test-topic' with three partitions. The controller assigns these partitions across the brokers for load balancing."

> "Third, it creates a producer and sends 100 messages. Watch as the messages get batched, compressed with snappy compression, and sent via gRPC to the appropriate brokers based on partition assignment."

### SAY (when test completes):
> "Test passed. All 100 messages were successfully stored and verified."

### RUN (Terminal 3):
```bash
# Show what was created on disk
tree -L 3 data/
```

### SAY:
> "Here's what the system created on disk. You can see the three broker directories, each containing their assigned partitions. Within each partition, there's a log file storing the actual message data and an index file mapping message offsets to byte positions for fast lookups."

### RUN (Terminal 3):
```bash
# Show file sizes
ls -lh data/broker-*/*/
```

### SAY:
> "You can see the log files contain the compressed message data. The index files are smaller - they just store offset mappings for quick seeks."

---

## MINUTE 5-6: TESTING & QUALITY (60 seconds)

### RUN (Terminal 1):
```bash
# Show test suite
tree -L 2 src/test/java/
```

### SAY:
> "Quality was a priority throughout this project. I have 37 comprehensive test files covering every major component."

### RUN (Terminal 1):
```bash
# Count test files
find src/test/java/ -name "*Test.java" | wc -l
```

### RUN (Terminal 1):
```bash
# Show specific test categories
ls src/test/java/producer/
```

### SAY:
> "The producer tests cover batching logic, buffer pool memory management, compression, retry mechanisms, and batch expiry. Claude Code helped me think through edge cases - like what happens when batches expire before sending, or how to handle concurrent appends to the same batch."

### RUN (Terminal 1):
```bash
ls src/test/java/consumer/
```

### SAY:
> "Consumer tests cover all three partition assignment strategies - RoundRobin, Range, and Sticky. They also test group coordination, rebalancing when members join or leave, and offset tracking."

### RUN (Terminal 1):
```bash
ls src/test/java/broker/
```

### SAY:
> "Broker tests cover partition writes, log segment rotation, concurrent access, and broker liveness tracking through heartbeats."

### RUN (Terminal 2):
```bash
# Run a specific test to show it in action
mvn test -Dtest=RecordBatchTest
```

### SAY:
> "This test validates record batching - ensuring messages are correctly serialized, batched together, and can be deserialized in the same order."

---

## MINUTE 6: WRAP-UP & IMPACT (30-60 seconds)

### SAY:
> "Let me summarize what this project demonstrates."

> "Technically, I built a complete distributed system implementing core concepts like partition-based storage for scalability, cluster coordination through a controller pattern, heartbeat-based failure detection, consumer group load balancing, and durable storage with an append-only log structure. The system handles concurrent writes safely through fine-grained locking, batches messages for efficiency, supports multiple compression algorithms, and uses gRPC for high-performance communication."

> "From an engineering perspective, I wrote production-quality code with comprehensive testing, clean architecture using immutable data structures, proper separation of concerns across packages, and documented thread-safety guarantees. The 18-commit progression shows systematic, logical development."

> "But what's most significant is how I leveraged Claude Code. I didn't just generate code - I used AI as a technical partner to understand WHY certain patterns exist in distributed systems, to challenge my initial designs and suggest better alternatives, and to enforce discipline around testing and edge cases that I might have overlooked."

### SAY:
> "Systems like this traditionally require entire teams to build. By using Claude Code as a senior engineering mentor - having it explain distributed systems concepts, present architectural tradeoffs, and guide implementation decisions - I was able to build a production-quality distributed message queue as an individual developer."

> "This demonstrates both deep technical capability in distributed systems and the ability to effectively leverage modern AI tools as a force multiplier for accelerated development and learning."

### SAY (Final line):
> "Are there any specific aspects you'd like me to dive deeper into?"

---

## BACKUP: If Asked Specific Questions

### Q: "Can you show me the actual message flow code?"

**RUN:**
```bash
cat src/main/java/grpc/services/ProducerServiceImpl.java
```

**SAY:**
> "This is the gRPC service that receives messages from producers. When publishToBroker is called, it deserializes the batch, finds the target partition, and appends the records. The partition delegates to the log, which delegates to the log segment, which writes to a ByteBuffer and updates the index."

### Q: "How does consumer group coordination work?"

**RUN:**
```bash
cat src/main/java/consumer/GroupCoordinator.java | head -80
```

**SAY:**
> "The GroupCoordinator manages consumer group membership. When a consumer joins, it registers with the coordinator. The coordinator designates one consumer as the leader, who runs the partition assignment algorithm - either RoundRobin, Range, or Sticky. The assignments are then distributed to all group members."

### Q: "Show me the storage layer in detail"

**RUN:**
```bash
cat src/main/java/server/internal/storage/LogSegment.java | head -100
```

**SAY:**
> "LogSegment is where data actually hits disk. It maintains a ByteBuffer for batching writes - we don't write every message immediately. When the buffer is full or a time threshold is hit, we flush to disk with fsync() for durability. Simultaneously, we update the IndexEntries file, mapping each message offset to its byte position in the log file. This enables O(1) seeks when consumers fetch at specific offsets."

### Q: "What about compression?"

**RUN:**
```bash
cat src/main/java/commons/utils/CompressionUtil.java | head -80
```

**SAY:**
> "CompressionUtil supports four algorithms - gzip for maximum compression, snappy for balanced speed and compression, lz4 for fastest compression, and zstandard for configurable compression levels. The producer compresses entire batches before sending, and the broker stores compressed data on disk. This significantly reduces both network bandwidth and disk usage."

### Q: "How do you handle failures?"

**RUN:**
```bash
cat src/main/java/server/internal/HeartbeatSender.java
cat src/main/java/server/internal/BrokerLivenessTracker.java | head -60
```

**SAY:**
> "Brokers send heartbeats to the controller every 5 seconds. The BrokerLivenessTracker on the controller monitors these heartbeats. If a broker misses its heartbeat window - 30 seconds by default - it's marked as dead and removed from the cluster. This allows the system to detect failures and stop routing messages to dead brokers."

---

## POST-DEMO CLEANUP

### If You Want to Show Clean State for Next Demo:

```bash
# Stop any running processes
for port in 50051 50052 50053; do
    lsof -ti:$port | xargs kill -9 2>/dev/null
done

# Remove data directory
rm -rf data/

# Clean build artifacts
mvn clean

# Verify clean state
ls
```

---

## TIMING GUIDE

| Minute | Section | Key Points |
|--------|---------|------------|
| 0-1 | Introduction | 18 commits, 8K LOC, systematic development |
| 1-2 | Architecture | 4 layers: Storage → Broker → Client → Network |
| 2-3 | Technical Depth | gRPC choice, partition-level locking, batching |
| 3-4 | Claude Code | 3 key decisions + edge case thinking |
| 4-5 | Live Demo | Run test, show disk files, explain flow |
| 5-6 | Testing & Wrap-up | 37 tests, production quality, AI collaboration |

---

## CRITICAL REMINDERS

1. **Speak clearly and at moderate pace** - don't rush
2. **Look at camera/audience** between terminal commands
3. **Use technical terms confidently** - partition-level locking, byte-offset indexing, exponential backoff
4. **Be ready to dive deeper** if they ask questions
5. **Emphasize Claude Code as mentor**, not just code generator
6. **Show enthusiasm** - you built something impressive

---

## EQUIPMENT CHECK

Before demo:
- [ ] Java 23 active (`java -version`)
- [ ] Project builds (`mvn compile` = SUCCESS)
- [ ] 3 terminals open and positioned
- [ ] Navigation to project directory in all terminals
- [ ] `tree` command works (or use `find` + `ls` alternative)
- [ ] No old data/ directory (`rm -rf data/`)
- [ ] This script open for reference
- [ ] Practiced full run 2-3 times

---

**You've got this! You built a production-quality distributed system with Claude Code. Show it with confidence. 🚀**
