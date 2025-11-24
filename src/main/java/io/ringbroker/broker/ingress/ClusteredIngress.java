package io.ringbroker.broker.ingress;

import com.google.protobuf.ByteString;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.delivery.Delivery;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.membership.replicator.AdaptiveReplicator;
import io.ringbroker.cluster.membership.resolver.ReplicaSetResolver;
import io.ringbroker.cluster.partitioner.Partitioner;
import io.ringbroker.core.ring.RingBuffer;
import io.ringbroker.core.wait.WaitStrategy;
import io.ringbroker.offset.OffsetStore;
import io.ringbroker.registry.TopicRegistry;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

@Slf4j
@RequiredArgsConstructor
@Getter
public final class ClusteredIngress {

    // Shared completed future to avoid allocations when nothing async is pending.
    private static final CompletableFuture<Void> COMPLETED_FUTURE =
            CompletableFuture.completedFuture(null);

    // Instance-level executor so one instance's shutdown doesn't kill others.
    private final ExecutorService replicationExecutor;

    private final Partitioner partitioner;
    private final int totalPartitions;
    private final int myNodeId;
    private final int clusterSize;
    private final Map<Integer, Ingress> ingressMap;
    private final Map<Integer, RemoteBrokerClient> clusterNodes;
    private final boolean idempotentMode;
    private final Map<Integer, Set<Long>> seenMessageIds; // Long IDs, not String
    private final Map<Integer, Delivery> deliveryMap;
    private final OffsetStore offsetStore;
    private final TopicRegistry registry;
    private final BrokerRole myRole;
    private final ReplicaSetResolver replicaResolver;
    private final AdaptiveReplicator replicator;

    /* Factory method */
    public static ClusteredIngress create(final TopicRegistry registry,
                                          final Partitioner partitioner,
                                          final int totalPartitions,
                                          final int myNodeId,
                                          final int clusterSize,
                                          final Map<Integer, RemoteBrokerClient> clusterNodes,
                                          final Path baseDataDir,
                                          final int ringSize,
                                          final WaitStrategy waitStrategy,
                                          final long segmentCapacity,
                                          final int batchSize,
                                          final boolean idempotentMode,
                                          final OffsetStore offsetStore,
                                          final BrokerRole brokerRole,
                                          final ReplicaSetResolver replicaResolver,
                                          final AdaptiveReplicator replicator) throws IOException {

        final Map<Integer, Ingress> ingressMap = new HashMap<>();
        final Map<Integer, Delivery> deliveryMap = new HashMap<>();
        final Map<Integer, Set<Long>> seenMessageIds = idempotentMode
                ? new HashMap<>()
                : Collections.emptyMap();

        for (int pid = 0; pid < totalPartitions; pid++) {
            if (Math.floorMod(pid, clusterSize) == myNodeId) {
                final Path partDir = baseDataDir.resolve("partition-" + pid);
                final RingBuffer<byte[]> ring = new RingBuffer<>(ringSize, waitStrategy);
                final boolean forceDurable = (brokerRole == BrokerRole.PERSISTENCE);

                final Ingress ingress = Ingress.create(
                        registry, ring, partDir, segmentCapacity, batchSize, forceDurable
                );
                ingressMap.put(pid, ingress);

                final Delivery delivery = new Delivery(ring);
                deliveryMap.put(pid, delivery);

                if (idempotentMode) {
                    // Concurrent set for dedupe IDs
                    seenMessageIds.put(pid, ConcurrentHashMap.newKeySet());
                }
            }
        }

        // Instance-specific virtual-thread executor for replication tasks.
        final ExecutorService replicationExecutor =
                Executors.newVirtualThreadPerTaskExecutor();

        return new ClusteredIngress(
                replicationExecutor,
                partitioner,
                totalPartitions,
                myNodeId,
                clusterSize,
                ingressMap,
                clusterNodes,
                idempotentMode,
                seenMessageIds,
                deliveryMap,
                offsetStore,
                registry,
                brokerRole,
                replicaResolver,
                replicator
        );
    }

    /**
     * Publishes a message asynchronously.
     * Note: strictly an INSTANCE method to access 'myRole'.
     */
    public CompletableFuture<Void> publish(final String topic, final byte[] key, final byte[] payload) {
        final long defaultCorrelationId = (myRole == BrokerRole.INGESTION) ? System.nanoTime() : 0L;
        return publish(defaultCorrelationId, topic, key, 0, payload);
    }

    public CompletableFuture<Void> publish(final long correlationId,
                                           final String topic,
                                           final byte[] key,
                                           final int retries,
                                           final byte[] payload) {

        final int partitionId = partitioner.selectPartition(key, totalPartitions);

        // Inline floorMod for a tiny bit less overhead
        int ownerNode = partitionId % clusterSize;
        if (ownerNode < 0) {
            ownerNode += clusterSize;
        }

        // Single-node fast path: no envelope, no replication, just local publish.
        if (clusterSize == 1 && myRole == BrokerRole.INGESTION) {
            try {
                handleLocalPublish(partitionId, topic, retries, payload, key);
                return COMPLETED_FUTURE;
            } catch (final Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        // Owner is this node.
        if (ownerNode == myNodeId) {
            try {
                handleLocalPublish(partitionId, topic, retries, payload, key);
            } catch (final Exception e) {
                return CompletableFuture.failedFuture(e);
            }

            // Determine replicas. Avoid mutating a possibly unmodifiable list.
            final List<Integer> resolved = replicaResolver.replicas(partitionId);
            if (resolved.isEmpty()) {
                return COMPLETED_FUTURE;
            }

            final List<Integer> replicas = new ArrayList<>(resolved.size());
            for (int nodeId : resolved) {
                if (nodeId != myNodeId) {
                    replicas.add(nodeId);
                }
            }

            if (replicas.isEmpty()) {
                return COMPLETED_FUTURE;
            }

            // Only now build the protobuf envelope, since we actually need it.
            final BrokerApi.Envelope envelope = buildEnvelope(
                    correlationId, topic, key, payload, partitionId, retries
            );

            final CompletableFuture<Void> future = new CompletableFuture<>();
            replicationExecutor.submit(() -> {
                try {
                    replicator.replicate(envelope, replicas);
                    future.complete(null);
                } catch (final Exception e) {
                    future.completeExceptionally(e);
                }
            });
            return future;
        }

        // Owner is a remote node: we need an envelope.
        final RemoteBrokerClient ownerClient = clusterNodes.get(ownerNode);
        if (ownerClient == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("No client for owner node " + ownerNode)
            );
        }

        final BrokerApi.Envelope envelope = buildEnvelope(
                correlationId, topic, key, payload, partitionId, retries
        );

        // Here sendEnvelopeWithAck returns a ReplicationAck (semantic unchanged)
        return ownerClient.sendEnvelopeWithAck(envelope).thenApply(ack -> {
            if (ack.getStatus() != BrokerApi.ReplicationAck.Status.SUCCESS) {
                throw new RuntimeException("Forwarding failed: " + ack.getStatus());
            }
            return null;
        });
    }

    private static BrokerApi.Envelope buildEnvelope(final long correlationId,
                                                    final String topic,
                                                    final byte[] key,
                                                    final byte[] payload,
                                                    final int partitionId,
                                                    final int retries) {

        final BrokerApi.Message.Builder msgBuilder = BrokerApi.Message.newBuilder()
                .setTopic(topic)
                .setRetries(retries)
                .setKey(key == null ? ByteString.EMPTY : ByteString.copyFrom(key))
                .setPayload(ByteString.copyFrom(payload))
                .setPartitionId(partitionId);

        return BrokerApi.Envelope.newBuilder()
                .setCorrelationId(correlationId)
                .setPublish(msgBuilder.build())
                .build();
    }

    public void handleLocalPublish(final int partitionId,
                                   final String topic,
                                   final int retries,
                                   final byte[] payload,
                                   final byte[] key) {
        if (idempotentMode) {
            final Set<Long> seen = seenMessageIds.get(partitionId);
            if (seen == null) {
                throw new IllegalStateException("Seen set missing for partition " + partitionId);
            }
            final long msgId = computeMessageId(partitionId, key, payload);
            if (!seen.add(msgId)) {
                // Duplicate detected, drop.
                return;
            }
        }

        final Ingress ingress = ingressMap.get(partitionId);
        if (ingress == null) {
            throw new IllegalStateException("No Ingress for partition " + partitionId);
        }
        ingress.publish(topic, retries, payload);
    }

    public void subscribeTopic(final String topic, final String group, final BiConsumer<Long, byte[]> handler) {
        if (!registry.contains(topic)) {
            throw new IllegalArgumentException("Unknown topic: " + topic);
        }

        for (final Map.Entry<Integer, Delivery> entry : deliveryMap.entrySet()) {
            final int partitionId = entry.getKey();
            final long committed = Math.max(0L, offsetStore.fetch(topic, group, partitionId));

            entry.getValue().subscribe(committed, (sequence, message) -> {
                handler.accept(sequence, message);
                // Per-message commit; with optimized OffsetStore this is still fine.
                offsetStore.commit(topic, group, partitionId, sequence);
            });
        }
    }

    public void shutdown() throws IOException {
        for (final Ingress ingress : ingressMap.values()) {
            ingress.close();
        }
        replicationExecutor.shutdown();
        try {
            if (!replicationExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Replication executor did not terminate within 30s");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Cheap-ish idempotent key: based on partition + hash(key) + hash(payload).
     * Still O(|key| + |payload|) to compute, but we avoid String allocation.
     */
    private long computeMessageId(final int partitionId, final byte[] key, final byte[] payload) {
        final int keyHash = (key != null ? Arrays.hashCode(key) : 0);
        final int payloadHash = Arrays.hashCode(payload);
        // Mix into a single 64-bit value.
        final int combined = 31 * keyHash + payloadHash;
        return (((long) partitionId) << 32) ^ (combined & 0xFFFF_FFFFL);
    }
}
