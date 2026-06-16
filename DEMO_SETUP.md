# MiniKafka Demo - Setup & Run Commands

## Prerequisites

### 1. Java 23 Setup

The project REQUIRES Java 23 (uses preview features: unnamed variables).

```bash
# Install SDKMAN (if not already installed)
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"

# Install Java 23
sdk install java 23-tem

# Switch to Java 23
sdk use java 23-tem

# Verify
java -version
# Must show: openjdk version "23"
```

### 2. Restore pom.xml to Java 23

```bash
cd /Users/rahulkumarsaw/Downloads/Alignerr\ Tasks/flux-main

# Check current Java version in pom.xml
grep "maven.compiler.source" pom.xml

# If it shows 21, restore to 23:
sed -i.bak 's/<maven.compiler.source>21<\/maven.compiler.source>/<maven.compiler.source>23<\/maven.compiler.source>/' pom.xml
sed -i.bak 's/<maven.compiler.target>21<\/maven.compiler.target>/<maven.compiler.target>23<\/maven.compiler.target>/' pom.xml

# Verify change
grep "maven.compiler.source" pom.xml
# Should show: <maven.compiler.source>23</maven.compiler.source>
```

---

## Build the Project

```bash
# Clean and compile (generates gRPC stubs from .proto files)
mvn clean compile

# Expected output: BUILD SUCCESS
# This creates:
# - target/generated-sources/protobuf/java/proto/ (Protocol Buffer classes)
# - target/generated-sources/protobuf/grpc-java/proto/ (gRPC service stubs)
# - target/classes/ (compiled classes)
```

### Verify Build Success

```bash
# Check generated gRPC files
ls target/generated-sources/protobuf/grpc-java/proto/

# Should see files like:
# - ProducerServiceGrpc.java
# - ConsumerServiceGrpc.java
# - MetadataServiceGrpc.java
# - etc.

# Check compiled classes
ls target/classes/producer/
ls target/classes/server/internal/
```

---

## Run Tests (Optional but Recommended)

```bash
# Run all tests
mvn test

# Expected output:
# - Tests run: 100+
# - Failures: 0
# - Skipped: may have some

# Run specific test class
mvn test -Dtest=RecordBatchTest

# Run all producer tests
mvn test -Dtest=producer.*Test
```

---

## Demo Run Commands

### Option 1: Run Existing Integration Test (Easiest)

The project has integration tests that demonstrate the full flow:

```bash
# Run the producer integration test
mvn test -Dtest=MiniKafkaProducerIntegrationTest

# This test:
# 1. Starts a broker cluster
# 2. Creates a topic
# 3. Sends messages via producer
# 4. Verifies messages were stored
```

**What happens:**
- Creates broker on localhost:50051
- Creates topic "test-topic" with 3 partitions
- Sends 100 messages with compression
- Verifies all messages stored correctly

### Option 2: Create and Run Demo Class

Create `src/main/java/Demo.java`:

```java
import admin.MiniKafkaAdminClient;
import admin.NewTopic;
import producer.MiniKafkaProducer;
import producer.ProducerRecord;
import server.internal.Cluster;

import java.util.Arrays;
import java.util.Properties;

public class Demo {
    public static void main(String[] args) throws Exception {
        System.out.println("=== MiniKafka Demo Starting ===\n");

        // Step 1: Start cluster with 3 brokers
        System.out.println("Step 1: Starting 3-broker cluster...");
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:50051,localhost:50052,localhost:50053");
        props.put("broker.count", "3");

        MiniKafkaAdminClient admin = MiniKafkaAdminClient.create(props);

        // CRITICAL: Wait for cluster to fully initialize
        Thread.sleep(3000);
        System.out.println("✓ Cluster started with 3 brokers\n");

        // Step 2: Create topic
        System.out.println("Step 2: Creating topic 'demo-topic' with 6 partitions...");
        admin.createTopics(Arrays.asList(new NewTopic("demo-topic", 6, 1)));

        // CRITICAL: Wait for topic creation to complete
        Thread.sleep(1000);
        System.out.println("✓ Topic created\n");

        // Step 3: Create producer and send messages
        System.out.println("Step 3: Sending 20 messages...");
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:50051,localhost:50052,localhost:50053");
        producerProps.put("compression.type", "snappy");

        MiniKafkaProducer<String, String> producer = new MiniKafkaProducer<>(producerProps);

        for (int i = 0; i < 20; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "demo-topic",
                "key-" + i,
                "Message #" + i + " - Hello from MiniKafka!"
            );
            producer.send(record);
            System.out.println("  → Sent: " + record.value());
        }

        // CRITICAL: Must close producer to flush all batches
        producer.close();
        System.out.println("✓ All messages sent and flushed\n");

        System.out.println("=== Demo Complete ===");
        System.out.println("Messages distributed across 6 partitions");
        System.out.println("Stored on disk in data/ directory");

        // Cleanup
        System.exit(0);
    }
}
```

**Run the demo:**
```bash
# Compile
mvn compile

# Run
mvn exec:java -Dexec.mainClass="Demo"

# Or run directly with java
java -cp target/classes:target/dependency/* Demo
```

---

## Understanding What Happens

### When Cluster Starts:

1. **Broker 1** (port 50051) - Acts as CONTROLLER
   - Starts gRPC server
   - Initializes as active controller
   - Starts heartbeat monitoring thread

2. **Broker 2** (port 50052) - FOLLOWER
   - Starts gRPC server
   - Registers with controller
   - Sends periodic heartbeats

3. **Broker 3** (port 50053) - FOLLOWER
   - Starts gRPC server
   - Registers with controller
   - Sends periodic heartbeats

**Files created:**
```
data/
├── broker-1/
│   └── (partitions assigned to broker 1)
├── broker-2/
│   └── (partitions assigned to broker 2)
└── broker-3/
    └── (partitions assigned to broker 3)
```

### When Topic is Created:

**Topic:** "demo-topic" with 6 partitions, replication factor 1

**Controller assigns partitions to brokers:**
```
Broker 1: partitions 0, 3
Broker 2: partitions 1, 4
Broker 3: partitions 2, 5
```

**Each partition creates storage:**
```
data/broker-1/demo-topic-0/
├── 00000000000000000000.log    (log segment file)
└── 00000000000000000000.index  (index file)

data/broker-1/demo-topic-3/
├── 00000000000000000000.log
└── 00000000000000000000.index
```

### When Producer Sends Messages:

**Flow for each message:**

1. **ProducerRecord created** with topic, key, value
   ```java
   new ProducerRecord("demo-topic", "key-1", "Message #1")
   ```

2. **Serialization** (Kryo)
   - Key → byte[]
   - Value → byte[]
   - Creates IntermediaryRecord with metadata

3. **Partition Selection**
   - Has explicit partition? → Use it
   - Has key? → Hash key % numPartitions
   - Neither? → Round-robin

4. **Batching** (RecordAccumulator)
   - Adds to batch for target partition
   - Waits 10ms (linger.ms) OR batch fills to 16KB
   - Compresses batch (snappy)

5. **gRPC Send**
   - PublishToBroker RPC
   - Sends to broker owning that partition

6. **Broker Receives**
   - ProducerServiceImpl handles request
   - Calls Partition.append(batch)

7. **Storage Write**
   - Partition → Log → LogSegment
   - Writes to ByteBuffer
   - Updates IndexEntries (offset → byteOffset)
   - Flushes to disk when buffer full

**Disk structure after writes:**
```
00000000000000000000.log:
[record1 bytes][record2 bytes][record3 bytes]...

00000000000000000000.index:
offset=0 → byteOffset=0
offset=1 → byteOffset=256
offset=2 → byteOffset=512
```

---

## Viewing Results

### Check Stored Data

```bash
# See data directory structure
tree data/

# Expected structure:
# data/
# ├── broker-1/
# │   ├── demo-topic-0/
# │   │   ├── 00000000000000000000.log
# │   │   └── 00000000000000000000.index
# │   └── demo-topic-3/
# │       ├── 00000000000000000000.log
# │       └── 00000000000000000000.index
# ├── broker-2/
# │   ├── demo-topic-1/
# │   └── demo-topic-4/
# └── broker-3/
#     ├── demo-topic-2/
#     └── demo-topic-5/
```

### Check Log File Size

```bash
# See size of log files
ls -lh data/broker-1/demo-topic-0/

# Example output:
# -rw-r--r-- 1 user staff 5.2K ... 00000000000000000000.log
# -rw-r--r-- 1 user staff  128 ... 00000000000000000000.index
```

---

## Troubleshooting

### Issue: "invalid target release: 23"

**Cause:** Not using Java 23

**Fix:**
```bash
sdk use java 23-tem
java -version  # Must show 23
mvn clean compile
```

### Issue: "Port already in use"

**Cause:** Previous broker still running

**Fix:**
```bash
# Kill process on port 50051
lsof -ti:50051 | xargs kill -9

# Kill all on ports 50051-50053
for port in 50051 50052 50053; do
    lsof -ti:$port | xargs kill -9 2>/dev/null
done
```

### Issue: Protocol Buffer compilation fails

**Cause:** Missing protoc or grpc plugin

**Fix (macOS):**
```bash
brew install protobuf grpc protoc-gen-grpc-java
mvn clean compile
```

### Issue: Tests fail with timeout

**Cause:** Insufficient wait time after cluster/topic creation

**Fix:** In test code, ensure:
```java
// After cluster creation
Thread.sleep(3000);  // Wait 3 seconds

// After topic creation
Thread.sleep(1000);  // Wait 1 second
```

### Issue: Messages not stored

**Cause:** Producer not closed (batches not flushed)

**Fix:**
```java
producer.send(record);
// ...
producer.close();  // MUST call this to flush batches
```

---

## Clean Up

### Remove Generated Files

```bash
# Remove compiled classes and generated code
mvn clean

# Remove data directory
rm -rf data/

# Start fresh
mvn clean compile
```

### Reset Git (if needed)

```bash
# If you made changes to pom.xml or other files
git checkout pom.xml
git clean -fd
```

---

## Summary of Critical Commands

```bash
# Prerequisites
sdk use java 23-tem
cd /Users/rahulkumarsaw/Downloads/Alignerr\ Tasks/flux-main

# Build
mvn clean compile

# Run tests
mvn test

# Run demo (if you created Demo.java)
mvn exec:java -Dexec.mainClass="Demo"

# Clean up
rm -rf data/
mvn clean
```

---

## What You'll See During Demo

**Console output:**
```
=== MiniKafka Demo Starting ===

Step 1: Starting 3-broker cluster...
[INFO] Broker 1 started on port 50051 (CONTROLLER)
[INFO] Broker 2 started on port 50052
[INFO] Broker 3 started on port 50053
✓ Cluster started with 3 brokers

Step 2: Creating topic 'demo-topic' with 6 partitions...
[INFO] Topic created: demo-topic
[INFO] Partition assignment:
  Broker 1: [0, 3]
  Broker 2: [1, 4]
  Broker 3: [2, 5]
✓ Topic created

Step 3: Sending 20 messages...
  → Sent: Message #0 - Hello from MiniKafka!
  → Sent: Message #1 - Hello from MiniKafka!
  ...
  → Sent: Message #19 - Hello from MiniKafka!
✓ All messages sent and flushed

=== Demo Complete ===
Messages distributed across 6 partitions
Stored on disk in data/ directory
```

**Disk files created:**
```
data/
├── broker-1/
│   ├── demo-topic-0/
│   │   ├── 00000000000000000000.log    (~6-7 messages)
│   │   └── 00000000000000000000.index
│   └── demo-topic-3/
│       ├── 00000000000000000000.log    (~6-7 messages)
│       └── 00000000000000000000.index
├── broker-2/
│   ├── demo-topic-1/
│   │   ├── 00000000000000000000.log    (~3-4 messages)
│   │   └── 00000000000000000000.index
│   └── demo-topic-4/
│       ├── 00000000000000000000.log    (~3-4 messages)
│       └── 00000000000000000000.index
└── broker-3/
    ├── demo-topic-2/
    │   ├── 00000000000000000000.log    (~3-4 messages)
    │   └── 00000000000000000000.index
    └── demo-topic-5/
        ├── 00000000000000000000.log    (~3-4 messages)
        └── 00000000000000000000.index
```

Messages distributed via round-robin partition selection (no key specified).
