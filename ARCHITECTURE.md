# MiniKafka - Architecture & Design Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Core Components](#core-components)
4. [Data Flow](#data-flow)
5. [Storage Layer](#storage-layer)
6. [Network Protocol](#network-protocol)
7. [Configuration](#configuration)
8. [Building & Running](#building--running)

---

## Overview

**MiniKafka** is an educational distributed message queue platform inspired by Apache Kafka. Built for learning distributed systems concepts through hands-on implementation.

### Project Goals
- Understand core distributed systems concepts (replication, partitioning, consensus)
- Build a functional end-to-end message broker
- Explore realistic storage internals (append-only logs, segmentation, indexing)
- Learn by building, not for production use

### Key Differences from Apache Kafka
| Feature | Apache Kafka | MiniKafka |
|---------|-------------|-----------|
| **Protocol** | Custom TCP protocol | gRPC + Protocol Buffers |
| **Coordination** | ZooKeeper/KRaft | Simplified controller (first node) |
| **Target** | Production-grade | Educational |
| **Complexity** | Full feature set | Core features only |
| **Replication** | Configurable | Max 3 replicas |
| **Language** | Scala/Java | Java 23 |

---

## Architecture

### High-Level System Design

```
┌─────────────────────────────────────────────────────────────┐
│                      PRODUCERS                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │Producer 1│  │Producer 2│  │Producer N│                  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘                  │
│       │             │             │                          │
│       └─────────────┴─────────────┘                          │
│                     │                                        │
│              (gRPC Publish)                                  │
└─────────────────────┼────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                   MINIKAFKA CLUSTER                          │
│  ┌───────────────────────────────────────────────────────┐  │
│  │               CONTROLLER (Broker 1)                    │  │
│  │  • Topic Management    • Metadata Propagation         │  │
│  │  • Partition Assignment • Broker Registration         │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Broker 1   │  │   Broker 2   │  │   Broker N   │      │
│  │              │  │              │  │              │      │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │      │
│  │ │Partition0│ │  │ │Partition1│ │  │ │Partition2│ │      │
│  │ │  (Log)   │ │  │ │  (Log)   │ │  │ │  (Log)   │ │      │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │      │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │      │
│  │ │Partition3│ │  │ │Partition4│ │  │ │Partition5│ │      │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│         │                  │                  │              │
│         └──────────────────┴──────────────────┘              │
│                            │                                 │
│                     (Heartbeats)                             │
│                            │                                 │
│  ┌─────────────────────────────────────────────────────┐    │
│  │        METADATA SUBSYSTEM (Observer Pattern)        │    │
│  │  • ClusterSnapshot Cache                            │    │
│  │  • Periodic Updates (5 min)                         │    │
│  │  • Listener Notifications                           │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────┬────────────────────────────────────────┘
                      │
                      │ (gRPC Fetch)
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                      CONSUMERS                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │           CONSUMER GROUP "group-1"                   │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐          │   │
│  │  │Consumer 1│  │Consumer 2│  │Consumer N│          │   │
│  │  │ Part 0,3 │  │ Part 1,4 │  │ Part 2,5 │          │   │
│  │  └──────────┘  └──────────┘  └──────────┘          │   │
│  │  (Partition Assignment via Assignor)                │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Component Layers

```
┌─────────────────────────────────────────────────────────┐
│  CLIENT LAYER                                           │
│  • MiniKafkaProducer   • MiniKafkaConsumer   • MiniKafkaAdminClient   │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼ (gRPC)
┌─────────────────────────────────────────────────────────┐
│  NETWORK LAYER (gRPC Services)                          │
│  • ProducerService    • ConsumerService                 │
│  • MetadataService    • GroupCoordinatorService         │
│  • ControllerService  • HeartbeatService                │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│  CLUSTER COORDINATION LAYER                             │
│  • Cluster      • Controller      • BrokerLivenessTracker│
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│  BROKER LAYER                                           │
│  • Broker       • Partition Assignment                  │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│  STORAGE LAYER                                          │
│  • Partition → Log → LogSegment → IndexEntries         │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│  DISK PERSISTENCE                                       │
│  • Log Files (.log)   • Index Files (.index)            │
└─────────────────────────────────────────────────────────┘
```

---

## Core Components

### 1. Producer Architecture

**Purpose:** Client-side message production with batching, compression, and retry logic

#### MiniKafkaProducer
```java
// Primary producer implementation
public class MiniKafkaProducer implements Producer {
    - RecordAccumulator accumulator    // Batches records
    - Metadata metadata                 // Cluster metadata cache
    - PartitionSelector selector        // Partition selection logic
    - ProducerConfig config             // Configuration
}
```

**Key Features:**
- **Batching:** RecordAccumulator groups messages per topic-partition
- **Compression:** Supports gzip, snappy, lz4, zstandard
- **Memory Management:** BufferPool prevents unbounded memory usage
- **Metadata Awareness:** Subscribes to cluster changes via MetadataListener
- **Retry Logic:** Configurable retries on failures

#### Producer Message Flow
```
User Code
    │
    ▼
ProducerRecord(key, value, topic)
    │
    ▼
Serialization (Kryo) → IntermediaryRecord
    │
    ▼
Partition Selection
    │ (explicit partition OR key hash OR round-robin)
    ▼
RecordAccumulator.append()
    │
    ▼
RecordBatch (per topic-partition)
    │ (buffered until batch.size or linger.ms)
    ▼
Compression (optional)
    │
    ▼
Drain batches per broker
    │
    ▼
gRPC: PublishToBroker.Send()
    │
    ▼
Acknowledgment ← Broker
```

#### Configuration (`ProducerConfig.java`)
```properties
# Batching
batch.size=16384                    # Batch size in bytes
linger.ms=10                        # Wait time before sending

# Memory
buffer.memory=33554432              # Total memory for buffering

# Compression
compression.type=none               # none|gzip|snappy|lz4|zstd

# Network
bootstrap.servers=localhost:9092,localhost:9093
request.timeout.ms=30000

# Reliability
max.in.flight.requests.per.connection=5
retries=3
```

---

### 2. Consumer Architecture

**Purpose:** Client-side message consumption with consumer groups and offset management

#### MiniKafkaConsumer
```java
public class MiniKafkaConsumer implements Consumer {
    - String groupId                    // Consumer group
    - GroupCoordinator coordinator      // Group coordination
    - PartitionAssignor assignor        // Partition assignment strategy
    - Map<TopicPartition, Long> offsets // Offset tracking
}
```

**Key Features:**
- **Consumer Groups:** Multiple consumers share partition load
- **Partition Assignment:** Pluggable assignors (RoundRobin, Range, Sticky)
- **Offset Management:** Currently in-memory (TODO: persistent commits)
- **Heartbeats:** Periodic heartbeats to group coordinator
- **Rebalancing:** Partition reassignment on group membership changes

#### Consumer Group Protocol
```
Consumer Startup
    │
    ▼
JoinGroup RPC → GroupCoordinator
    │ (consumer sends subscribed topics)
    ▼
GroupCoordinator assigns leader
    │
    ▼
Leader Consumer runs PartitionAssignor
    │ (computes partition → consumer mapping)
    ▼
SyncGroup RPC → GroupCoordinator
    │ (leader sends assignments)
    ▼
GroupCoordinator distributes assignments to all members
    │
    ▼
Consumers receive assigned partitions
    │
    ▼
poll() loop starts
    │
    ▼
FetchMessage RPC → Broker
    │
    ▼
ConsumerRecord returned to application
```

#### Partition Assignors

**RoundRobinAssignor** - Even distribution across consumers
```
Topics: T1(3 partitions), T2(2 partitions)
Consumers: C1, C2

Assignment:
C1 → T1-P0, T1-P2, T2-P1
C2 → T1-P1, T2-P0
```

**RangeAssignor** - Partition ranges per topic
```
Topics: T1(4 partitions), T2(3 partitions)
Consumers: C1, C2

Assignment:
C1 → T1-P0, T1-P1, T2-P0, T2-P1
C2 → T1-P2, T1-P3, T2-P2
```

**StickyAssignor** - Minimizes partition movement on rebalance
- Preserves previous assignments where possible
- Balances load while reducing churn

---

### 3. Broker Architecture

**Purpose:** Storage and serving of messages, cluster coordination

#### Broker
```java
public class Broker implements Controller {
    - Map<String, Map<Integer, Partition>> partitionsByTopic
    - boolean isActiveController
    - BrokerMetadata metadata
    - BrokerServer grpcServer
    - HeartbeatSender heartbeatSender
}
```

**Dual Role:**
- **Normal Broker:** Stores partitions, serves read/write requests
- **Controller:** First broker acts as controller (additional responsibilities)

#### Controller Responsibilities
1. **Topic Management:** Create/delete topics
2. **Partition Assignment:** Distribute partitions across brokers
3. **Broker Registration:** Track cluster membership
4. **Metadata Propagation:** Broadcast cluster state changes
5. **Heartbeat Monitoring:** Detect broker failures (BrokerLivenessTracker)

#### Broker Liveness Mechanism
```
Follower Broker
    │
    │ (Every heartbeat.interval.ms = 5000ms)
    ▼
HeartbeatSender.sendHeartbeat()
    │
    ▼
gRPC → Controller.Heartbeat()
    │
    ▼
BrokerLivenessTracker.recordHeartbeat(brokerId)
    │
    ▼
Update lastHeartbeatTime
    │
    │ (Background thread checks every 30s)
    ▼
Check: currentTime - lastHeartbeatTime > timeout (30000ms)?
    │
    ├─ Yes → Mark broker as DEAD, trigger metadata update
    └─ No  → Broker still ALIVE
```

---

### 4. Storage Layer

**Purpose:** Durable, append-only log storage with fast reads via indexing

#### Storage Hierarchy
```
Broker
  │
  ├─ Topic "orders"
  │    ├─ Partition 0 ─────┐
  │    │                   │
  │    ├─ Partition 1      │
  │    └─ Partition 2      │
  │                         │
  └─ Topic "payments"       │
       ├─ Partition 0       │
       └─ Partition 1       │
                            │
                            ▼
                    ┌─────────────┐
                    │  PARTITION  │
                    │  (Queue)    │
                    └──────┬──────┘
                           │
                           │ owns one Log
                           ▼
                    ┌─────────────┐
                    │     LOG     │
                    │ (Multiple   │
                    │  Segments)  │
                    └──────┬──────┘
                           │
                ┌──────────┼──────────┐
                │          │          │
                ▼          ▼          ▼
         ┌───────────┬───────────┬───────────┐
         │ Segment 0 │ Segment 1 │ Segment 2 │
         │ (closed)  │ (closed)  │ (active)  │
         ├───────────┼───────────┼───────────┤
         │00000.log  │10000.log  │20000.log  │
         │00000.index│10000.index│20000.index│
         └───────────┴───────────┴───────────┘
```

#### Partition
```java
// Fundamental append-only queue
public class Partition {
    - Log log                           // Ordered log segments
    - PartitionWriteManager writeManager // Thread-safe writes

    public void append(RecordBatch batch) {
        writeManager.lockForWrite();
        log.append(batch);
        writeManager.unlockWrite();
    }
}
```

#### Log
```java
// Manages multiple ordered segments
public class Log {
    - List<LogSegment> segments         // Ordered by base offset
    - LogSegment activeSegment          // Currently accepting writes

    public void append(RecordBatch batch) {
        if (activeSegment.size() > MAX_SEGMENT_SIZE) {
            rollSegment();              // Create new segment
        }
        activeSegment.append(batch);
    }
}
```

#### LogSegment
```java
// Immutable once closed
public class LogSegment {
    - long baseOffset                   // Starting offset
    - ByteBuffer buffer                 // Write buffer
    - IndexEntries index                // Offset → byte position

    public void append(RecordBatch batch) {
        buffer.put(serialize(batch));
        index.addEntry(offset, byteOffset);

        if (shouldFlush()) {
            flushToDisk();
        }
    }
}
```

#### IndexEntries
```java
// Fast offset lookups
// Maps: message offset → byte offset in log file
Map<Long, Long> offsetIndex;

// Example:
// offset 100 → byte 0
// offset 105 → byte 1024
// offset 110 → byte 2048

// Read at offset 105: seek to byte 1024, read from there
```

**Why Indexing Matters:**
- Without index: Must scan entire log file to find offset
- With index: Direct seek to byte position → O(1) lookup

---

### 5. Metadata Subsystem

**Purpose:** Cluster-wide metadata caching and propagation using Observer pattern

#### Metadata (Singleton)
```java
public class Metadata {
    - ClusterSnapshot snapshot          // Cached cluster state
    - List<MetadataListener> listeners  // Observers
    - ScheduledExecutorService executor // Periodic updates

    // Updates every 5 minutes by default
    public void startPeriodicUpdates() {
        executor.scheduleAtFixedRate(
            this::fetchAndUpdateMetadata,
            0, 5, TimeUnit.MINUTES
        );
    }

    private void fetchAndUpdateMetadata() {
        ClusterSnapshot newSnapshot = fetchFromController();
        if (!newSnapshot.equals(snapshot)) {
            snapshot = newSnapshot;
            notifyListeners();          // Observer pattern
        }
    }
}
```

#### Metadata Records (Immutable Snapshots)

**ClusterSnapshot**
```java
public record ClusterSnapshot(
    ControllerMetadata controller,
    List<BrokerMetadata> brokers,
    List<TopicMetadata> topics
) {}
```

**BrokerMetadata**
```java
public record BrokerMetadata(
    int brokerId,
    String host,
    int port,
    BrokerState state,                  // ALIVE | DEAD
    Map<String, List<Integer>> partitions
) {}
```

**TopicMetadata**
```java
public record TopicMetadata(
    String topicName,
    int numPartitions,
    int replicationFactor,
    Map<Integer, PartitionMetadata> partitions
) {}
```

---

## Data Flow

### Producer Write Path

```
┌─────────────────────────────────────────────────────────────┐
│ 1. APPLICATION CODE                                         │
│    producer.send(new ProducerRecord("topic", key, value))   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. SERIALIZATION                                            │
│    Kryo → IntermediaryRecord                                │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. PARTITION SELECTION                                      │
│    PartitionSelector.select()                               │
│    - Explicit partition (if provided in record)             │
│    - Hash(key) % numPartitions (if key exists)              │
│    - Round-robin (default)                                  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. RECORD ACCUMULATOR                                       │
│    RecordAccumulator.append(topic, partition, record)       │
│    - Add to RecordBatch for topic-partition                 │
│    - Allocate memory from BufferPool                        │
│    - Track in-flight batches                                │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ (Wait until batch.size or linger.ms)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. BATCH PREPARATION                                        │
│    - Close batch (mark as ready)                            │
│    - Compress batch (if compression.type != none)           │
│    - Drain batches per broker (round-robin)                 │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. NETWORK SEND (gRPC)                                      │
│    ProducerServiceStub.publishToBroker(request)             │
│    - Target broker determined from metadata                 │
│    - Request contains: topic, partition, serialized batch   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 7. BROKER RECEIVES (ProducerServiceImpl)                    │
│    - Validate request                                       │
│    - Deserialize batch                                      │
│    - Forward to Partition                                   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 8. PARTITION APPEND                                         │
│    partition.append(batch)                                  │
│    - Acquire write lock (PartitionWriteManager)             │
│    - Append to Log                                          │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 9. LOG APPEND                                               │
│    log.append(batch)                                        │
│    - Check if active segment needs rotation                 │
│    - Append to active LogSegment                            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 10. LOG SEGMENT WRITE                                       │
│     logSegment.append(batch)                                │
│     - Write to ByteBuffer                                   │
│     - Update IndexEntries (offset → byte position)          │
│     - Flush to disk (when buffer full or time threshold)    │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 11. DISK PERSISTENCE                                        │
│     - Write to .log file                                    │
│     - Write to .index file                                  │
│     - fsync() for durability                                │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 12. ACKNOWLEDGMENT                                          │
│     - Response sent back through gRPC                       │
│     - Producer receives confirmation                        │
│     - BufferPool memory released                            │
└─────────────────────────────────────────────────────────────┘
```

### Consumer Read Path

```
┌─────────────────────────────────────────────────────────────┐
│ 1. CONSUMER INITIALIZATION                                  │
│    consumer.subscribe(Arrays.asList("topic"))               │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. JOIN GROUP                                               │
│    GroupCoordinator.joinGroup(groupId, topics)              │
│    - gRPC: JoinGroup RPC to controller                      │
│    - Controller assigns leader consumer                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. PARTITION ASSIGNMENT (Leader Consumer Only)              │
│    LeaderAssignmentPlanner.computeAssignment()              │
│    - Run configured PartitionAssignor                       │
│    - Distribute partitions across consumers                 │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. SYNC GROUP                                               │
│    GroupCoordinator.syncGroup(assignments)                  │
│    - Leader sends assignments to controller                 │
│    - Controller distributes to all consumers                │
│    - Each consumer receives assigned partitions             │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. POLL LOOP                                                │
│    consumer.poll(Duration.ofMillis(1000))                   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. FETCH REQUEST (per assigned partition)                   │
│    ConsumerServiceStub.fetchMessage(topic, partition, offset)│
│    - Use tracked offset (or earliest if first fetch)        │
│    - gRPC request to broker owning partition                │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 7. BROKER FETCH (ConsumerServiceImpl)                       │
│    - Locate partition                                       │
│    - Validate offset                                        │
│    - Read from Partition                                    │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 8. PARTITION READ                                           │
│    partition.read(offset)                                   │
│    - Acquire read lock                                      │
│    - Delegate to Log                                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 9. LOG READ                                                 │
│    log.read(offset)                                         │
│    - Find LogSegment containing offset                      │
│    - Read from segment                                      │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 10. LOG SEGMENT READ                                        │
│     logSegment.read(offset)                                 │
│     - Lookup offset in IndexEntries → byte position         │
│     - Seek to byte position in .log file                    │
│     - Read record batch                                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 11. DESERIALIZATION                                         │
│     - Decompress batch (if compressed)                      │
│     - Deserialize into ConsumerRecord objects               │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 12. RETURN TO APPLICATION                                   │
│     - gRPC response with records                            │
│     - Consumer updates tracked offset                       │
│     - Application processes ConsumerRecords                 │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 13. OFFSET COMMIT (Manual)                                  │
│     consumer.commitOffsets(offsets)                         │
│     - Currently in-memory only                              │
│     - TODO: Persistent offset storage                       │
└─────────────────────────────────────────────────────────────┘
```

---

## Network Protocol

### gRPC Service Definitions

MiniKafka uses 9 Protocol Buffer files defining gRPC services:

#### 1. Producer Service (`publish.proto`)
```protobuf
service ProducerService {
  rpc PublishToBroker(PublishRequest) returns (PublishResponse);
}

message PublishRequest {
  string topic = 1;
  int32 partition = 2;
  bytes serialized_batch = 3;
}
```

#### 2. Consumer Service (`consume.proto`)
```protobuf
service ConsumerService {
  rpc FetchMessage(FetchRequest) returns (FetchResponse);
}

message FetchRequest {
  string topic = 1;
  int32 partition = 2;
  int64 offset = 3;
}
```

#### 3. Consumer Group Service (`consumer_group.proto`)
```protobuf
service GroupCoordinatorService {
  rpc JoinGroup(JoinGroupRequest) returns (JoinGroupResponse);
  rpc SyncGroup(SyncGroupRequest) returns (SyncGroupResponse);
  rpc LeaveGroup(LeaveGroupRequest) returns (LeaveGroupResponse);
}
```

#### 4. Metadata Service (`metadata.proto`)
```protobuf
service MetadataService {
  rpc GetMetadata(MetadataRequest) returns (MetadataResponse);
}

message MetadataResponse {
  repeated BrokerInfo brokers = 1;
  repeated TopicInfo topics = 2;
}
```

#### 5. Controller Service (`controller.proto`)
```protobuf
service ControllerService {
  rpc RegisterBroker(RegisterBrokerRequest) returns (RegisterBrokerResponse);
  rpc CreateTopic(CreateTopicRequest) returns (CreateTopicResponse);
  rpc AssignPartitions(AssignPartitionsRequest) returns (AssignPartitionsResponse);
}
```

#### 6. Heartbeat Service (`heartbeat.proto`)
```protobuf
service HeartbeatService {
  rpc SendHeartbeat(HeartbeatRequest) returns (HeartbeatResponse);
}

message HeartbeatRequest {
  int32 broker_id = 1;
  int64 timestamp = 2;
}
```

#### 7. Admin Service (`admin.proto`)
```protobuf
service AdminService {
  rpc CreateTopics(CreateTopicsRequest) returns (CreateTopicsResponse);
  rpc DeleteTopics(DeleteTopicsRequest) returns (DeleteTopicsResponse);
  rpc ListTopics(ListTopicsRequest) returns (ListTopicsResponse);
}
```

#### 8. Offset Service (`offset.proto`)
```protobuf
service OffsetService {
  rpc CommitOffset(CommitOffsetRequest) returns (CommitOffsetResponse);
  rpc FetchOffset(FetchOffsetRequest) returns (FetchOffsetResponse);
}
```

#### 9. Common Types (`common.proto`)
```protobuf
// Shared message types
message TopicPartition {
  string topic = 1;
  int32 partition = 2;
}

message RecordBatch {
  repeated Record records = 1;
  CompressionType compression = 2;
}

enum CompressionType {
  NONE = 0;
  GZIP = 1;
  SNAPPY = 2;
  LZ4 = 3;
  ZSTD = 4;
}
```

### gRPC Server Configuration

**BrokerServer.java**
```java
public class BrokerServer {
    private final Server server;

    public BrokerServer(Broker broker, int port) {
        this.server = ServerBuilder.forPort(port)
            .addService(new ProducerServiceImpl(broker))
            .addService(new ConsumerServiceImpl(broker))
            .addService(new MetadataServiceImpl(broker))
            .addService(new GroupCoordinatorServiceImpl(broker))
            .addService(new ControllerServiceImpl(broker))
            .addService(new HeartbeatServiceImpl(broker))
            .addService(new CreateTopicsServiceImpl(broker))
            .build();
    }
}
```

---

## Configuration

### Broker Configuration

**BrokerConfig.java**
```java
public class BrokerConfig {
    // Heartbeat settings
    private int heartbeatIntervalMs = 5000;        // 5 seconds
    private int heartbeatTimeoutMs = 30000;        // 30 seconds
    private int heartbeatCheckIntervalMs = 30000;  // 30 seconds

    // Storage settings
    private long segmentSize = 1073741824;         // 1 GB
    private int indexIntervalBytes = 4096;         // 4 KB

    // Network
    private String host;
    private int port;
    private int brokerId;

    // Replication
    private int defaultReplicationFactor = 1;
    private int maxReplicationFactor = 3;
}
```

**Configuration Validation:**
```java
public class BrokerConfigValidator {
    public void validate(BrokerConfig config) {
        if (config.getHeartbeatIntervalMs() <= 0)
            throw new IllegalArgumentException("heartbeat.interval.ms must be > 0");

        if (config.getHeartbeatTimeoutMs() <= config.getHeartbeatIntervalMs())
            throw new IllegalArgumentException("heartbeat.timeout.ms must be > heartbeat.interval.ms");
    }
}
```

### Producer Configuration

**ProducerConfig.java**
```properties
# Required
bootstrap.servers=localhost:9092,localhost:9093,localhost:9094

# Batching
batch.size=16384                    # 16 KB
linger.ms=10                        # 10 ms

# Memory
buffer.memory=33554432              # 32 MB

# Compression
compression.type=none               # none|gzip|snappy|lz4|zstd

# Network
request.timeout.ms=30000            # 30 seconds
max.in.flight.requests.per.connection=5

# Reliability
retries=3
retry.backoff.ms=100

# Serialization
key.serializer=dev.minikafka.commons.serializers.StringSerializer
value.serializer=dev.minikafka.commons.serializers.StringSerializer
```

### Consumer Configuration

**ConsumerConfig.java**
```properties
# Required
bootstrap.servers=localhost:9092,localhost:9093,localhost:9094
group.id=my-consumer-group

# Partition assignment
partition.assignment.strategy=RoundRobin  # RoundRobin|Range|Sticky

# Fetch settings
fetch.min.bytes=1
fetch.max.wait.ms=500
max.partition.fetch.bytes=1048576          # 1 MB

# Offset
auto.offset.reset=earliest                 # earliest|latest|none
enable.auto.commit=false                   # Manual commits for now

# Session
session.timeout.ms=10000                   # 10 seconds
heartbeat.interval.ms=3000                 # 3 seconds

# Deserialization
key.deserializer=dev.minikafka.commons.serializers.StringDeserializer
value.deserializer=dev.minikafka.commons.serializers.StringDeserializer
```

---

## Building & Running

### Prerequisites

**Required:**
- **Java 23** (MiniKafka uses Java 23 features)
- **Maven 3.8+**
- **Protocol Buffers Compiler** (`protoc`)
- **gRPC Java Plugin** (`protoc-gen-grpc-java`)

**Mac/Apple Silicon:**
```bash
brew install protobuf grpc protoc-gen-grpc-java
```

**Linux (Ubuntu/Debian):**
```bash
# Install protoc
sudo apt update
sudo apt install -y protobuf-compiler

# Install gRPC Java plugin
wget https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/1.71.0/protoc-gen-grpc-java-1.71.0-linux-x86_64.exe
chmod +x protoc-gen-grpc-java-1.71.0-linux-x86_64.exe
sudo mv protoc-gen-grpc-java-1.71.0-linux-x86_64.exe /usr/local/bin/protoc-gen-grpc-java
```

### Building

```bash
# Clean and compile (generates Protocol Buffer classes)
mvn clean compile

# Run tests
mvn test

# Package JAR
mvn package

# Skip tests
mvn package -DskipTests
```

**Generated Code Locations:**
- Protocol Buffer messages: `target/generated-sources/protobuf/java/`
- gRPC service stubs: `target/generated-sources/protobuf/grpc-java/`

### Running MiniKafka Cluster

**Option 1: Programmatic Cluster Startup (Recommended for Testing)**

```java
import dev.minikafka.admin.MiniKafkaAdminClient;

public class Main {
    public static void main(String[] args) {
        // Create admin client
        MiniKafkaAdminClient admin = MiniKafkaAdminClient.create("localhost:9092,localhost:9093,localhost:9094");

        // Start 3-broker cluster
        admin.startCluster();

        // Create topic with 6 partitions
        admin.createTopic("orders", 6);

        System.out.println("MiniKafka cluster running...");
    }
}
```

**Option 2: Manual Broker Startup**

```java
import dev.minikafka.server.internal.Broker;
import dev.minikafka.server.config.BrokerConfig;

// Start broker 1 (controller)
BrokerConfig config1 = new BrokerConfig();
config1.setBrokerId(1);
config1.setHost("localhost");
config1.setPort(9092);
config1.setHeartbeatIntervalMs(5000);

Broker broker1 = new Broker(config1, true); // isController=true
broker1.start();

// Start broker 2 (follower)
BrokerConfig config2 = new BrokerConfig();
config2.setBrokerId(2);
config2.setHost("localhost");
config2.setPort(9093);

Broker broker2 = new Broker(config2, false);
broker2.start();

// Start broker 3 (follower)
BrokerConfig config3 = new BrokerConfig();
config3.setBrokerId(3);
config3.setHost("localhost");
config3.setPort(9094);

Broker broker3 = new Broker(config3, false);
broker3.start();
```

### Running Producer

```java
import dev.minikafka.producer.MiniKafkaProducer;
import dev.minikafka.producer.ProducerRecord;
import dev.minikafka.producer.ProducerConfig;

public class ProducerExample {
    public static void main(String[] args) {
        // Create config
        ProducerConfig config = new ProducerConfig();
        config.setBootstrapServers("localhost:9092,localhost:9093,localhost:9094");
        config.setBatchSize(16384);
        config.setLingerMs(10);
        config.setCompressionType("snappy");

        // Create producer
        MiniKafkaProducer producer = new MiniKafkaProducer(config);

        // Send messages
        for (int i = 0; i < 1000; i++) {
            ProducerRecord record = new ProducerRecord(
                "orders",                       // topic
                "order-" + i,                   // key
                "Order details for " + i        // value
            );

            producer.send(record);
        }

        // Cleanup
        producer.close();
    }
}
```

### Running Consumer

```java
import dev.minikafka.consumer.MiniKafkaConsumer;
import dev.minikafka.consumer.ConsumerRecord;
import dev.minikafka.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Arrays;

public class ConsumerExample {
    public static void main(String[] args) {
        // Create config
        ConsumerConfig config = new ConsumerConfig();
        config.setBootstrapServers("localhost:9092,localhost:9093,localhost:9094");
        config.setGroupId("order-processors");
        config.setPartitionAssignmentStrategy("RoundRobin");
        config.setAutoOffsetReset("earliest");

        // Create consumer
        MiniKafkaConsumer consumer = new MiniKafkaConsumer(config);

        // Subscribe to topics
        consumer.subscribe(Arrays.asList("orders"));

        // Poll loop
        while (true) {
            List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord record : records) {
                System.out.printf("Consumed: key=%s, value=%s, offset=%d%n",
                    record.key(), record.value(), record.offset());
            }

            // Commit offsets
            if (!records.isEmpty()) {
                consumer.commitOffsets();
            }
        }
    }
}
```

### Running Benchmarks

```bash
# Producer benchmark
mvn exec:java -Dexec.mainClass="dev.minikafka.benchmark.ProducerBenchmark"

# Consumer benchmark
mvn exec:java -Dexec.mainClass="dev.minikafka.benchmark.ConsumerBenchmark"

# JMH benchmarks
mvn clean verify
java -jar target/benchmarks.jar
```

---

## Testing

### Running Tests

```bash
# All tests
mvn test

# Specific test class
mvn test -Dtest=ProducerServiceImplTest

# Integration tests only
mvn verify -Pit

# With coverage report
mvn clean verify jacoco:report
# Report: target/site/jacoco/index.html
```

### Test Categories

**Unit Tests (35 test classes):**
- Producer: RecordAccumulator, RecordBatch, BufferPool, PartitionSelector
- Consumer: Partition assignors (RoundRobin, Range, Sticky)
- Broker: Partition, Log, LogSegment, IndexEntries
- gRPC: All service implementations
- Commons: Compression, Headers, Serializers

**Integration Tests:**
- `MiniKafkaProducerIntegrationTest` - End-to-end producer flow
- `BufferPoolIntegrationTest` - Memory management
- `HeartbeatIntegrationTest` - Broker liveness tracking

**Benchmarks (JMH):**
- `ProducerBenchmark` - Throughput and latency
- `ConsumerBenchmark` - Read performance

---

## Current Limitations & TODOs

### Implemented ✅
- Core broker functionality
- Append-only log storage with segmentation and indexing
- Producer batching, compression, retry
- Consumer groups with partition assignment
- Multiple partition assignor strategies (RoundRobin, Range, Sticky)
- Metadata subsystem with Observer pattern
- Broker heartbeat and liveness tracking
- gRPC communication layer
- Admin client for topic management
- Thread-safe concurrent writes

### In Progress / TODO ⚠️
- **Offset commit persistence** - Currently in-memory only
- **Proper controller election** - Currently first broker by default
- **Full replication support** - Infrastructure exists, not fully implemented
- **Partition reassignment** - For load balancing when brokers added/removed
- **Compaction** - Log cleanup for key-based retention
- **Additional admin operations** - Delete topics, alter configs
- **Metrics and monitoring** - JMX/Prometheus integration
- **Security** - SSL/TLS, SASL authentication, ACLs

### Known Issues
- Mac/Apple Silicon requires manual Protocol Buffer installation
- Offset commits not durable across consumer restarts
- Controller election is deterministic (first broker), not fault-tolerant
- No log retention policy implementation yet

---

## Contributing

### Code Structure Guidelines
- **Package organization:** Layer-based (producer, consumer, broker, storage, etc.)
- **Immutability:** Prefer immutable data structures (Java records)
- **Thread safety:** Explicit locking where needed (PartitionWriteManager)
- **Testing:** Unit tests required for new features
- **Documentation:** JavaDoc for public APIs

### Building New Features
1. Design: Document approach in issue/discussion
2. Implement: Follow existing patterns
3. Test: Unit + integration tests
4. Benchmark: Performance test if relevant
5. Document: Update this file if architecture changes

---

## References

### Kafka Documentation
- [Kafka Protocol](https://kafka.apache.org/protocol.html)
- [Log Internals](https://kafka.apache.org/documentation/#design_log)
- [Consumer Groups](https://kafka.apache.org/documentation/#consumergroups)
- [Producer Internals](https://kafka.apache.org/documentation/#producerapi)

### MiniKafka Differences
- **gRPC vs Custom Protocol:** Easier development, good performance, familiar tooling
- **Simplified Controller:** No ZooKeeper/KRaft complexity
- **Educational Focus:** Readable code over optimization
- **Modern Java:** Java 23 features (records, pattern matching)

---

## License

(Add your license here - Apache 2.0, MIT, etc.)

---

## Contributors

(See CONTRIBUTORS.md or Git history)

---

**Last Updated:** 2025-05-30
**MiniKafka Version:** Educational (not versioned)
**Maintained by:** (Your name/organization)
