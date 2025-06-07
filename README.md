# RingBroker: A High-Performance Distributed Messaging System

![Java](https://img.shields.io/badge/Java-21+-blue.svg)
![Maven](https://img.shields.io/badge/build-maven-red.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

RingBroker is a message broker engineered from the ground up for extreme throughput and low latency. It leverages modern Java features (including Virtual Threads), lock-free data structures, and a partitioned, replicated architecture to deliver uncompromising performance.

The system is designed to achieve throughputs of **10 million messages per second on commodity hardware** and over **20 million messages per second on cloud infrastructure (c6a.4xlarge)** on a single node, making it suitable for the most demanding data ingestion and streaming workloads.

## Core Architectural Principles

*   **Mechanical Sympathy:** The design minimizes kernel calls, context switches, cache misses, and garbage collection pressure by using virtual threads, memory-mapped files, `VarHandle`/`Unsafe` memory semantics, and object/buffer reuse.
*   **Partitioning for Scale:** Topics are split into partitions, which are distributed across the cluster, allowing for horizontal scaling of storage and throughput.
*   **Separation of Concerns:** Nodes can be configured with distinct roles—`INGESTION` for handling client traffic and `PERSISTENCE` for durable storage—allowing for specialized hardware and independent scaling of network/CPU vs disk I/O.
*   **Lock-Free & Allocation-Free Hot Paths:** The core message ingestion path aims to avoid blocking locks and heap allocations by using a custom MPMC queue (`SlotRing`) and pre-allocated, reusable batch buffers (`ByteBatch`).
*   **Replication for Durability:** Data is replicated across a configurable number of `PERSISTENCE` nodes to ensure fault tolerance. RingBroker uses a novel **Adaptive Replicator** to achieve quorum acknowledgements from the fastest replicas first, ensuring low tail latency without sacrificing durability guarantees.

---

## Architectural Overview

RingBroker's architecture is a sophisticated interplay of components designed for performance and resilience.

```mermaid
graph TD
    subgraph Client
        Publisher
    end

    subgraph "Primary Node (Owner of Partition P)"
        P_Netty[Netty Transport]
        P_CI{ClusteredIngress}
        P_Ingress[Ingress (Partition P)]
        P_Queue((SlotRing Queue))
        P_Writer[Writer Loop VT]
        P_Ledger[(Ledger - mmap)]
        P_RingBuffer([RingBuffer - In-Memory])
        P_Replicator{AdaptiveReplicator}
    end

    subgraph "Replica Node (Persistence)"
        R_Netty[Netty Transport]
        R_CI{ClusteredIngress}
        R_Ingress[Ingress]
        R_Ledger[(Ledger - mmap)]
    end

    Publisher -- "publish(msg)" --> P_Netty
    P_Netty --> P_CI
    
    P_CI -- "1. Local Write" --> P_Ingress
    P_Ingress --> P_Queue
    P_Queue --> P_Writer
    P_Writer -- persists --> P_Ledger
    P_Writer -- publishes --> P_RingBuffer
    
    P_CI -- "2. Replicate Async" --> P_Replicator
    P_Replicator -- "sendEnvelopeWithAck()" --> R_Netty
    R_Netty --> R_CI
    R_CI --> R_Ingress
    R_Ingress --> R_Ledger
    R_Netty -- "Ack" --> P_Replicator
```

## Key Features

-   **High-Throughput Ingestion:** Lock-free, batch-oriented pipeline from network to disk (`Ingress`, `SlotRing`, `ByteBatch`).
-   **Low-Latency Delivery:** In-memory `RingBuffer` with `WaitStrategy` for push-based pub/sub delivery, bypassing disk reads for active consumers.
-   **Partitioned & Replicated:** Horizontally scalable and fault-tolerant by design.
-   **Distinct Broker Roles:** Optimize `INGESTION` nodes for CPU and network, and `PERSISTENCE` nodes for I/O and storage.
-   **Adaptive Quorum:** `AdaptiveReplicator` achieves low-latency acknowledgements by prioritizing fast replicas using latency EWMA and `CompletableFuture`.
-   **Durable & Recoverable Ledger:** Memory-mapped, append-only logs (`LedgerSegment`) with CRC validation, background pre-allocation and startup recovery (`LedgerOrchestrator`).
-   **Idempotent Publishing:** Optional deduplication of messages based on key/payload hash (`ClusteredIngress.computeMessageId`) to prevent processing duplicates.
-  **Virtual Threads:** Extensive use of `Executors.newVirtualThreadPerTaskExecutor()` for `Delivery` subscribers, `Ingress` writer loops, asynchronous replication calls, and `LedgerOrchestrator` pre-allocation, enabling massive scalability for I/O-bound tasks.
-   **Dead-Letter Queue (DLQ):** Automatic routing of unprocessable (schema validation failure) or repeatedly failing messages.
-   **Dynamic Schema Validation:** Integrates with Google Protobuf `DynamicMessage` and `Descriptor` for per-topic message schema validation (`TopicRegistry`, `Ingress`).
-   **Modern Concurrency Primitives**: Use of `VarHandle` and `sun.misc.Unsafe` for low-level memory access, padding (`PaddedAtomicLong`, `PaddedSequence`) to prevent false sharing.
- **Netty Transport**: Non-blocking I/O for client (`NettyClusterClient`) and server (`NettyTransport`) communication with Protobuf encoding/decoding.

## Components Deep Dive

-   `io.ringbroker.broker.ingress.ClusteredIngress`: The main entry point. Orchestrates partitioning, forwarding, local writes, and replication based on node role.
-    `io.ringbroker.broker.ingress.Ingress`: Manages the single-partition ingestion pipeline: validation, DLQ, queuing (`SlotRing`), batching (`ByteBatch`), and writing to the `LedgerOrchestrator` and `RingBuffer`. Includes the `writerLoop`.
-   `io.ringbroker.broker.delivery.Delivery`: Manages subscriber threads (virtual), consuming from a `RingBuffer` and pushing to consumers.
-   `io.ringbroker.core.ring.RingBuffer`: A multi-producer, single-consumer ring buffer with batch claim/publish for high-speed, in-memory message exchange. Uses `Sequence`, `Barrier`, `WaitStrategy`.
-   `io.ringbroker.ledger.orchestrator.LedgerOrchestrator`: Manages the lifecycle of on-disk log segments: creation, rollover, background pre-allocation, crash recovery (`bootstrap`, `recoverSegmentFile`).
- `io.ringbroker.ledger.segment.LedgerSegment`: Represents a single, append-only, memory-mapped file with header, CRC, and offset metadata. Uses `Unsafe`.
-   `io.ringbroker.cluster.membership.replicator.AdaptiveReplicator`: Implements the smart, latency-aware (EWMA) replication strategy using `CompletableFuture`.
-   `io.ringbroker.cluster.membership.resolver.ReplicaSetResolver`: Uses `HashingProvider` (HRW hash) to determine the set of persistence nodes for a given partition ID.
- `io.ringbroker.cluster.client.impl.NettyClusterClient`: Client for broker-to-broker communication, handles sending `Envelope` and managing `CompletableFuture` for `ReplicationAck`.
-  `io.ringbroker.transport.type.NettyTransport` & `NettyServerRequestHandler`: The high-performance, non-blocking network server for client communication, decoding `BrokerApi.Envelope` and dispatching to `ClusteredIngress`.
-   `io.ringbroker.config.impl.BrokerConfig`: Loads the broker's configuration from a `broker.yaml` file.
-   `io.ringbroker.registry.TopicRegistry` / `grpc.services.*`: Manages topic schemas and provides a gRPC admin interface.

## Getting Started

### Prerequisites

-   Java 21+ (required for Virtual Threads)
-   Apache Maven

### Building

To build the project and its dependencies, run the following command from the root directory:

```bash
gradle generateProto
gradle build
```

For a quick test, you can run:

```bash 
gradle jmh
```