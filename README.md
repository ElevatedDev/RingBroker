# RingBroker

RingBroker is a high-performance, partitioned message broker designed for efficient and reliable messaging using a combined in-memory ring buffer and persistent append-only log architecture. It achieves exceptional throughput on standard hardware while providing robust durability, scalability through clustering, and message deduplication capabilities. RingBroker is especially suitable for applications requiring high throughput, low latency, and strong guarantees around message persistence and delivery.

## Key Features

- **High Throughput and Low Latency:**  
  Leverages a Disruptor-style ring buffer, achieving millions of messages per second with minimal latency and overhead.

- **Scalable Partitioning:**  
  Supports configurable partitioning strategies including round-robin and key-based partitioning, allowing for linear horizontal scaling.

- **Robust Data Durability:**  
  Implements durability with CRC32 checksums, batch-based disk writes, and automatic crash recovery mechanisms, ensuring data integrity.

- **Clustering Support:**  
  Provides clustering capabilities for distributed operation, enabling partition ownership management and inter-node replication.

- **Idempotent Message Delivery:**  
  Optional deduplication feature ensures exactly-once delivery semantics, preventing duplicate message processing.

- **Comprehensive gRPC API Suite:**  
  Includes APIs for publishing and subscribing to messages, administrative tasks (topic creation/deletion), and schema management.

- **Minimal External Dependencies:**  
  Lightweight, pure-Java implementation with minimal external dependencies, simplifying deployment and integration.

---

## Architectural Overview

RingBroker's architecture consists of several core components:

- **Clustered Ingress:** Handles incoming messages, determining the appropriate partition and node (local or remote).
- **PartitionContext:** Manages individual partitions, incorporating a ring buffer for message handling and LedgerOrchestrator for persistence.
- **LedgerOrchestrator:** Manages durable storage, using append-only logs with CRC32 checksums and ensuring data integrity through recovery procedures.
- **Delivery Threads:** Utilize virtual threads for efficient message delivery to consumers and replication pipelines without significant overhead.

```
Producers ──▶ ClusteredIngress ──▶ PartitionContext ──▶ RingBuffer & Ledger ──▶ Consumers
```

---

## Getting Started

### Building from Source

Clone the repository and build using Gradle:

```bash
git clone https://github.com/ElevatedDev/ringbroker.git
cd ringbroker
./gradlew clean build
```

### Running RingBroker

Configure via environment variables:

```bash
export BROKER_PORT=9090
export CLUSTER_SIZE=1
export NODE_ID=0
export TOTAL_PARTITIONS=8
export RING_SIZE=262144
export SEGMENT_SIZE=536870912
export WRITER_THREADS=4
export BATCH_SIZE=1024
export IDEMPOTENT_MODE=false
export INITIAL_TOPICS="orders/created,orders/created.DLQ"

java -jar build/libs/ringbroker.jar
```

---

## API Usage Examples

### Publishing a Message (Java)

```java
ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9090)
    .usePlaintext().build();
BrokerGrpc.BrokerBlockingStub client = BrokerGrpc.newBlockingStub(channel);

PublishReply reply = client.publish(
    Message.newBuilder()
        .setTopic("orders/created")
        .setRetries(0)
        .setKey(ByteString.copyFromUtf8("order-123"))
        .setPayload(yourPayload)
        .build()
);
System.out.println("Publish successful: " + reply.getSuccess());
```

### Admin API: Topic Management

```java
AdminGrpc.AdminBlockingStub admin = AdminGrpc.newBlockingStub(channel);

admin.createTopic(
    CreateTopicRequest.newBuilder()
        .setTopic("events/log")
        .setPartitions(16)
        .build()
);
```

### Schema Registry: Registering and Fetching Schemas

```java
SchemaRegistryGrpc.SchemaRegistryBlockingStub schemaClient =
    SchemaRegistryGrpc.newBlockingStub(channel);

schemaClient.registerSchema(
    RegisterSchemaRequest.newBuilder()
        .setTopic("orders/created")
        .setSchema(descriptorBytes)
        .build()
);

GetSchemaResponse response = schemaClient.getSchema(
    GetSchemaRequest.newBuilder().setTopic("orders/created").build()
);
```

---

## Configuration Reference

| Variable           | Default      | Description                                              |
|--------------------|--------------|----------------------------------------------------------|
| `BROKER_PORT`      | `9090`       | gRPC server port                                         |
| `CLUSTER_SIZE`     | `1`          | Number of nodes in the broker cluster                    |
| `NODE_ID`          | `0`          | Unique identifier for this broker node                   |
| `TOTAL_PARTITIONS` | `8`          | Total partitions available in the cluster                |
| `KEY_PARTITIONING` | `false`      | Partitioning strategy (key-based or round-robin)         |
| `RING_SIZE`        | `262144`     | Size of each partition's ring buffer                     |
| `SEGMENT_SIZE`     | `536870912`  | Size of disk segments for persistence                    |
| `WRITER_THREADS`   | `4`          | Threads for disk write operations per partition          |
| `BATCH_SIZE`       | `1024`       | Number of messages per disk write batch                  |
| `IDEMPOTENT_MODE`  | `false`      | Enables or disables message deduplication                |
| `INITIAL_TOPICS`   | (empty)      | Comma-separated list of topics to initialize on startup  |

RingBroker combines the performance advantages of in-memory systems with the durability and reliability of traditional persistent message brokers, offering an optimal solution for demanding messaging scenarios.
