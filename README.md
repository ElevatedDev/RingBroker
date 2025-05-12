# RingBroker 

**RingBroker** is a high-performance, partitioned message broker combining an in-memory ring buffer with a persistent append-only log. It delivers exceptional throughput on commodity hardware, guarantees crash-safe durability, and supports seamless clustering—all with minimal operational overhead.

---

## Why RingBroker

- **Cheapest Solution vs Results**  
  Self-contained, no ZooKeeper, no KRaft, no external dependencies. Every byte is accounted for—no mystery behaviors.

- **Outrageous Speed**  
  Outperforms Kafka on single-partition workloads. Even faster with clustered partitioning and segment compaction.

- **RingBuffer Architecture**  
  Based on a Disruptor-style design, RingBroker linearly scales with nodes and partitions—handling millions of messages per second.

- **gRPC API**  
  Schema-aware admin via gRPC and Protobuf—language-agnostic and TLS-ready.

- **Lean Deployment**  
  Pure Java. No containers, no sidecars, no agents. One `jar`, one YAML, no drama.

---

## Key Features

- **High Throughput & Low Latency**  
  Millions of msg/s with sub-millisecond latencies using batched fsync and lock-free queues.

- **Crash-Safe Durability**  
  Write-ahead append-only logs with segment checksums, fast recovery.

- **Clustering Built In**  
  Brokers self-coordinate via YAML `clusterNodes`; no gossip or external service needed.

- **Schema-Aware Admin**  
  Topic creation supports embedded Protobuf descriptors, allowing message schema registration and enforcement.

- **Zero GC Hotpath**  
  Publish/consume path avoids allocations for consistent latency under pressure.

---

## Architectural Overview

```
Producers ──▶ ClusteredIngress ──▶ PartitionContext ──▶ RingBuffer & Ledger ──▶ Consumers
```

---

## Benchmarks

**Test Machine:**  
Intel i7 Ultra (1.40 GHz), 16 cores / 22 threads, SSD

| Broker         | Producer Throughput | Consumer Throughput | Notes                                       |
|----------------|---------------------|----------------------|---------------------------------------------|
| **RingBroker** | 6.11 M msg/s        | 3.21 M msg/s         | 16 partitions, batch 4096, single node      |
| Kafka          | ~1–3 M msg/s        | ~1–2 M msg/s         | Multinode tuning required                   |
| Redpanda       | ~2–3 M msg/s        | ~2–3 M msg/s         | C++, io_uring, good latency                 |
| NATS JetStream | ~160k msg/s         | ~160k msg/s          | Durable, Go-based, simpler semantics        |

---

## Getting Started

### 1. Build the Broker

```bash
git clone https://github.com/ElevatedDev/RingBroker.git
cd RingBroker
./gradlew clean build
```

### 2. Configure the Broker

Example `broker.yaml`:

```yaml
grpcPort:       9090
clusterSize:    3
nodeId:         0
totalPartitions: 16
ringSize:       1048576
segmentBytes:   134217728
ingressThreads: 8
batchSize:      4096
idempotentMode: true
ledgerPath:     /var/lib/ringbroker/data
topicsFile:     topics.yaml
clusterNodes:
  - id: 0
    host: broker-0.local
    port: 9090
  - id: 1
    host: broker-1.local
    port: 9090
  - id: 2
    host: broker-2.local
    port: 9090
```

Example `topics.yaml`:

```yaml
topics:
  - name: orders/created
    protoClass: com.example.events.OrderCreatedProto
```

### 3. Run It

```bash
java -jar build/libs/ringbroker.jar broker.yaml
```

---

## 🔧 Topic Management API (gRPC)

All admin operations go through `TopicAdminService` using gRPC.

### Create a Topic (with optional schema)

```java
DescriptorProto schema = DescriptorProto.newBuilder()
    .setName("OrderCreated")
    .build();

var request = CreateTopicRequest.newBuilder()
    .setTopic("orders/created")
    .setSchema(schema)
    .build();

TopicReply reply = adminStub.createTopic(request);
System.out.println("Success: " + reply.getSuccess());
```

### List Topics

```java
TopicListReply list = adminStub.listTopics(Empty.newBuilder().build());
list.getTopicsList().forEach(System.out::println);
```

### Describe a Topic

```java
var desc = adminStub.describeTopic(
    TopicRequest.newBuilder().setTopic("orders/created").build()
);

System.out.printf("Partitions: %d, Error: %s%n", desc.getPartitions(), desc.getError());
```

> gRPC stubs are generated from `topic_admin.proto` using `protoc`.

---

## ⚡ Data Plane (Pub/Sub/Fetch)

RingBroker uses **Netty-based custom transport** for all data-intensive operations like:

- Publishing events  
- Fetching messages  
- Subscribing to message streams  
- Committing and querying offsets  

➡️ **Refer to your Netty transport layer in** `io.ringbroker.transport.*` and `NettyServerRequestHandler` for full details.  
This design isolates high-throughput message flows from low-volume admin logic.

---

## Configuration Reference

| Property          | Type    | Description                              |
|------------------|---------|------------------------------------------|
| `grpcPort`       | int     | Port for gRPC admin interface            |
| `clusterSize`    | int     | Total number of nodes in the cluster     |
| `nodeId`         | int     | This node's ID (0-based)                 |
| `totalPartitions`| int     | Number of partitions globally            |
| `ringSize`       | int     | In-memory ring size (slots per partition)|
| `segmentBytes`   | long    | Segment file size (bytes)                |
| `ingressThreads` | int     | Thread count for ingress                 |
| `batchSize`      | int     | Messages per batch flush                 |
| `idempotentMode` | bool    | Enables deduplication                    |
| `ledgerPath`     | string  | Path to log segment files                |
| `topicsFile`     | string  | Path to static topics definition file    |
| `clusterNodes`   | list    | YAML array of `{id,host,port}` configs   |

---

## License

GPU © ElevatedDev — see [LICENSE](LICENSE)

---
