# RingBroker

RingBroker is a high-performance, partitioned message broker combining an in-memory ring buffer with a persistent append-only log. It delivers exceptional throughput on commodity hardware, guarantees crash-safe durability, and supports seamless clustering—all with minimal operational overhead.

---

## Why RingBroker

- **Cheapest Solution vs Results**
  By far the cheapest solution for your money, as it requires no external resources, and is self managed to the last byte, no KRaft, no ZooKeeper.

- **Speed**
  Completely outpaces Kafka on single shard/single partition, and gaps it even further when introducing clusters and more refined segment management.

- **gRPC Based**
  Even though the broker itself is based in Java, everything is wrapped through gRPC to offer its API to almost any modern programming language, with managed TSL certificates for connections.

- **Easy to use**
  Virtually everything is already managed by the initial configuration, you avoid extremely complex broker configurations and you get better performance than the other brokers.

- **Scales Linearly**
  RingBuffer architecture -- refering to Disruptor-like architecture scales Linearly with Node Clusters and Partitions, giving you the best possible processing speed for your hardware.

## Key Features

- **High Throughput & Low Latency**  
  Disruptor-style ring buffer plus batched disk writes, achieving millions of msgs/sec and sub-millisecond in-memory latencies.

- **Scalable Partitioning**  
  Pluggable partitioners (round-robin, key-based) enable linear horizontal scaling across N nodes.

- **Crash-Safe Durability**  
  Append-only segment files with CRC32 checksums and batch flushing, automatic recovery on restart.

- **Built-in Clustering**  
  YAML-driven clusterNodes list; cross-node partition ownership and gRPC forwarding require no external coordination service.

- **Idempotent Delivery**  
  Optional dedup tracking per partition for exactly-once semantics (critical in financial/audit scenarios).

- **gRPC-Protobuf APIs**  
  Unified publish/subscribe, pull (Fetch), offset commit/fetch, and admin endpoints via gRPC; generates idiomatic Java/Go/Python clients.

- **Lean & Cost-Effective**  
  Pure-Java, zero-dependency broker—no ZK, no extra license—reducing hardware and operational costs compared to typical Kafka deployments.

---

## Architectural Overview

```
Producers ──▶ ClusteredIngress ──▶ PartitionContext ──▶ RingBuffer & Ledger ──▶ Consumers
```

1. **ClusteredIngress**  
   Routes each message to its partition owner (local vs remote) and forwards via gRPC when needed.  
2. **PartitionContext**  
   Uses a ring buffer for fast in-memory enqueue and dedicated writer thread for batched disk appends.  
3. **LedgerOrchestrator**  
   Manages segment files, checksums, and recovery logic.  
4. **Delivery**  
   Virtual threads and non-blocking backpressure deliver messages per-partition to subscribers.

---

## Benchmarks

**Test Environment:** Dell laptop, Intel i7 Ultra 1.40 GHz, 16 cores/22 threads, SSD

| Broker          | Producer Throughput | Consumer Throughput | Notes                                                      |
|------------------|---------------------|----------------------|-------------------------------------------------------------|
| **RingBroker**    | 5.46 M msg/s        | 3.21 M msg/s         | 16 partitions, 8 writer threads, batch=4096, disk-backed    |
| Apache Kafka     | ~1–3 M msg/s        | ~1–2 M msg/s         | JVM-based; OS page cache; millisecond-level tail latencies  |
| Redpanda         | ~2–3 M msg/s        | ~2–3 M msg/s         | Native C++; zero-GC; io_uring; lower tail latency           |
| NATS JetStream   | ~0.16 M msg/s       | ~0.16 M msg/s        | Go; durable mode; low latency over peak throughput          |

> **Cost & Ops:** RingBroker runs with zero external dependencies—no ZK, no extra license—reducing hardware and operational costs compared to typical Kafka deployments.

---

## Getting Started

### Prerequisites

- Java 21+
- `protoc` compiler (for regenerating stubs if needed)
- Gradle (optional) or use the provided `gradlew`

### Build & Run

1. **Clone and build**

```bash
git clone https://github.com/ElevatedDev/RingBroker.git
cd RingBroker
./gradlew clean build
```

2. **Prepare configuration** (e.g. `broker.yaml`):

```yaml
grpcPort:       9090
clusterSize:    3
nodeId:         0
totalPartitions: 16
ringSize:       1048576      # 1 << 20
segmentBytes:   134217728    # 128 MB
ingressThreads: 8
batchSize:      4096
idempotentMode: true
ledgerPath:     /var/lib/ringbroker/data
topicsFile:     topics.yaml
clusterNodes:
  - id: 0
    host: broker-0.example.com
    port: 9090
  - id: 1
    host: broker-1.example.com
    port: 9090
  - id: 2
    host: broker-2.example.com
    port: 9090
```

3. **Define topics** in `topics.yaml`:

```yaml
topics:
  - name: orders/created
    protoClass: com.example.events.OrderCreatedProto
```

4. **Launch broker**

```bash
java -jar build/libs/ringbroker.jar broker.yaml
```

---

## API Usage Examples

### Publish (gRPC)

```java
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.api.BrokerGrpc;

var channel = ManagedChannelBuilder.forAddress("localhost", 9090)
    .usePlaintext()
    .build();

var stub = BrokerGrpc.newBlockingStub(channel);

var msg = BrokerApi.Message.newBuilder()
    .setTopic("orders/created")
    .setRetries(0)
    .setPayload(yourProtoMsg.toByteArray())
    .setKey(ByteString.copyFromUtf8("order-123"))
    .build();

var reply = stub.publish(msg);
System.out.println("Published: " + reply.getSuccess());
```

### Subscribe (streaming) - for low throughput easy use.

```java
import io.grpc.stub.StreamObserver;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.api.BrokerGrpc;

var asyncStub = BrokerGrpc.newStub(channel);

asyncStub.subscribeTopic(
    BrokerApi.SubscribeRequest.newBuilder()
        .setTopic("orders/created")
        .setGroup("consumer-group-1")
        .build(),
    new StreamObserver<BrokerApi.MessageEvent>() {
        public void onNext(BrokerApi.MessageEvent ev) {
            // process ev.getPayload().toByteArray()
        }
        public void onError(Throwable t) {
            t.printStackTrace();
        }
        public void onCompleted() {
            System.out.println("Stream completed");
        }
    }
);
```

### Fetch (pull API) -- Near symmetrical consume/produce - no latency

```java
var fetchReq = BrokerApi.FetchRequest.newBuilder()
    .setTopic("orders/created")
    .setGroup("consumer-group-1")
    .setPartition(0)
    .setOffset(0)
    .setMaxMessages(100)
    .build();

var fetchReply = stub.fetch(fetchReq);
for (var ev : fetchReply.getMessagesList()) {
    // handle ev.getPayload()
}
```

### Commit & Query Offsets

```java
stub.commitOffset(
    BrokerApi.CommitRequest.newBuilder()
        .setTopic("orders/created")
        .setGroup("consumer-group-1")
        .setPartition(0)
        .setOffset(latestOffset)
        .build()
);

var committed = stub.fetchCommitted(
    BrokerApi.CommittedRequest.newBuilder()
        .setTopic("orders/created")
        .setGroup("consumer-group-1")
        .setPartition(0)
        .build()
);
System.out.println("Committed offset: " + committed.getOffset());
```

### Topic Management

```java
var adminStub = BrokerGrpc.newBlockingStub(channel);

// Create
var createReply = adminStub.createTopic(
    BrokerApi.TopicRequest.newBuilder()
        .setTopic("events/log")
        .build()
);

// Delete
var deleteReply = adminStub.deleteTopic(
    BrokerApi.TopicRequest.newBuilder()
        .setTopic("events/log")
        .build()
);
```

---

## Configuration Reference

| Property            | Type    | Required | Description                                 |
|---------------------|---------|----------|---------------------------------------------|
| `grpcPort`          | int     | yes      | gRPC listen port                            |
| `clusterSize`       | int     | yes      | Total nodes in cluster                      |
| `nodeId`            | int     | yes      | This node’s unique ID (0 ≤ nodeId < clusterSize) |
| `totalPartitions`   | int     | yes      | Total partitions across cluster             |
| `ringSize`          | int     | yes      | In-memory ring buffer size per partition    |
| `segmentBytes`      | long    | yes      | Disk segment file size (bytes)              |
| `ingressThreads`    | int     | yes      | Writer threads per partition                |
| `batchSize`         | int     | yes      | Messages per disk write batch               |
| `idempotentMode`    | boolean | yes      | Enable exactly-once deduplication           |
| `ledgerPath`        | string  | yes      | Filesystem path for on-disk segments        |
| `topicsFile`        | string  | yes      | Path to `topics.yaml`                       |
| `clusterNodes`      | list    | no       | List of `{id,host,port}` for each broker    |

---

## License

GPU © ElevatedDev — see [LICENSE](LICENSE)

---

_Repo: https://github.com/ElevatedDev/RingBroker_
