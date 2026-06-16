# MiniKafka Demo - Quick Start

## You Have 2 Files for Demo

1. **DEMO_COMMANDS.md** - All commands you need to run
2. **DEMO_SCRIPT.md** - What to say during presentation (5-6 min script)

---

## Setup (Once Before Demo)

```bash
# Install Java 23
sdk install java 23-tem
sdk use java 23-tem

# Navigate to project
cd /Users/rahulkumarsaw/Downloads/Alignerr\ Tasks/flux-main

# Build
mvn clean compile
```

---

## Choose Your Demo Style

### Option A: Interactive Demo (Impressive)

**Best for showing real-time Kafka behavior**

Open 3 terminals and run:

**Terminal 1:**
```bash
mvn exec:java -Dexec.mainClass="demo.StartCluster"
# Starts 3-broker cluster + creates topic
```

**Terminal 2:**
```bash
mvn exec:java -Dexec.mainClass="demo.SimpleConsumer"
# Consumes messages in real-time
```

**Terminal 3:**
```bash
# Send messages
mvn exec:java -Dexec.mainClass="demo.SimpleProducer" -Dexec.args="Hello MiniKafka"
mvn exec:java -Dexec.mainClass="demo.SimpleProducer" -Dexec.args="Message 2"
mvn exec:java -Dexec.mainClass="demo.SimpleProducer" -Dexec.args="Message 3"

# Show data on disk
tree data/
ls -lh data/broker-1/demo-topic-0/
```

✅ **You'll see messages flow in real-time from Terminal 3 → Terminal 2**
✅ **You'll see data files created on disk**
✅ **Shows MiniKafka working exactly like Kafka**

---

### Option B: Full E-Commerce Demo (Comprehensive)

**Best for showing all features at once**

```bash
./run-demo.sh
```

Shows complete order processing system:
- 3-broker cluster
- 30 orders published
- 2 consumer groups (payment + inventory)
- 4 consumers total
- Real-time processing
- Load balancing

---

## For Your Presentation

1. **Open DEMO_SCRIPT.md** - Follow it minute-by-minute
2. **Use Option A** (Interactive) during presentation
3. **Use Option B** (Full demo) as backup or if you want automated flow

---

## What Gets Demonstrated

✅ Multi-broker cluster (3 brokers)
✅ Topic with partitions (distributed across brokers)
✅ Real-time producer → consumer message flow
✅ Durable storage (log files on disk)
✅ Partition-based distribution
✅ Consumer groups (Option B)
✅ Load balancing (Option B)
✅ Compression (snappy)
✅ Batching for efficiency

---

## Cleanup After Demo

```bash
# Stop running processes (Ctrl+C)
# Clean data
rm -rf data/
```

---

**That's it! Two files, two demo options, everything ready to go.** 🚀
