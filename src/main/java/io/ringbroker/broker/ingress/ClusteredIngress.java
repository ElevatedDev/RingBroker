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

    private static final ExecutorService REPLICATION_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();
    private static final CompletableFuture<Void> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);

    private final Partitioner partitioner;
    private final int totalPartitions;
    private final int myNodeId;
    private final int clusterSize;
    private final Map<Integer, Ingress> ingressMap;
    private final Map<Integer, RemoteBrokerClient> clusterNodes;
    private final boolean idempotentMode;
    private final Map<Integer, Set<String>> seenMessageIds;
    private final Map<Integer, Delivery> deliveryMap;
    private final OffsetStore offsetStore;
    private final TopicRegistry registry;
    private final BrokerRole myRole;
    private final ReplicaSetResolver replicaResolver;
    private final AdaptiveReplicator replicator;

    /* Factory method omitted for brevity, assume standard create() as before */
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
        final Map<Integer, Set<String>> seenMessageIds = idempotentMode
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
                    seenMessageIds.put(pid, ConcurrentHashMap.newKeySet());
                }
            }
        }

        return new ClusteredIngress(
                partitioner, totalPartitions, myNodeId, clusterSize, ingressMap,
                clusterNodes, idempotentMode, seenMessageIds, deliveryMap,
                offsetStore, registry, brokerRole, replicaResolver, replicator
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
        final int ownerNode = Math.floorMod(partitionId, clusterSize);

        if (clusterSize == 1 && myRole == BrokerRole.INGESTION) {
            try {
                handleLocalPublish(partitionId, topic, retries, payload, key);
                return COMPLETED_FUTURE;
            } catch (final Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        final BrokerApi.Message.Builder msgBuilder = BrokerApi.Message.newBuilder()
                .setTopic(topic)
                .setRetries(retries)
                .setKey(key == null ? ByteString.EMPTY : ByteString.copyFrom(key))
                .setPayload(ByteString.copyFrom(payload))
                .setPartitionId(partitionId);

        final BrokerApi.Envelope envelope = BrokerApi.Envelope.newBuilder()
                .setCorrelationId(correlationId)
                .setPublish(msgBuilder.build())
                .build();

        if (ownerNode == myNodeId) {
            try {
                handleLocalPublish(partitionId, topic, retries, payload, key);
            } catch (final Exception e) {
                return CompletableFuture.failedFuture(e);
            }

            final List<Integer> replicas = replicaResolver.replicas(partitionId);
            replicas.removeIf(id -> id == myNodeId);

            if (replicas.isEmpty()) {
                return COMPLETED_FUTURE;
            }

            final CompletableFuture<Void> future = new CompletableFuture<>();
            REPLICATION_EXECUTOR.submit(() -> {
                try {
                    replicator.replicate(envelope, replicas);
                    future.complete(null);
                } catch (final Exception e) {
                    future.completeExceptionally(e);
                }
            });
            return future;
        }

        final RemoteBrokerClient ownerClient = clusterNodes.get(ownerNode);
        if (ownerClient == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("No client for owner node " + ownerNode));
        }

        return ownerClient.sendEnvelopeWithAck(envelope).thenApply(ack -> {
            if (ack.getStatus() != BrokerApi.ReplicationAck.Status.SUCCESS) {
                throw new RuntimeException("Forwarding failed: " + ack.getStatus());
            }
            return null;
        });
    }

    public void handleLocalPublish(final int partitionId,
                                   final String topic,
                                   final int retries,
                                   final byte[] payload,
                                   final byte[] key) {
        if (idempotentMode) {
            final Set<String> seen = Objects.requireNonNull(
                    seenMessageIds.get(partitionId), "Seen set missing for partition " + partitionId);
            final String msgId = computeMessageId(partitionId, key, payload);
            if (!seen.add(msgId)) return;
        }

        final Ingress ingress = ingressMap.get(partitionId);
        if (ingress == null) throw new IllegalStateException("No Ingress for partition " + partitionId);
        ingress.publish(topic, retries, payload);
    }

    public void subscribeTopic(final String topic, final String group, final BiConsumer<Long, byte[]> handler) {
        if (!registry.contains(topic)) throw new IllegalArgumentException("Unknown topic: " + topic);

        for (final Map.Entry<Integer, Delivery> entry : deliveryMap.entrySet()) {
            final int partitionId = entry.getKey();
            final long committed = Math.max(0L, offsetStore.fetch(topic, group, partitionId));
            entry.getValue().subscribe(committed, (sequence, message) -> {
                handler.accept(sequence, message);
                offsetStore.commit(topic, group, partitionId, sequence);
            });
        }
    }

    public void shutdown() throws IOException {
        for (final Ingress ingress : ingressMap.values()) ingress.close();
        REPLICATION_EXECUTOR.shutdown();
    }

    private String computeMessageId(final int partitionId, final byte[] key, final byte[] payload) {
        final int keyHash = (key != null ? Arrays.hashCode(key) : 0);
        final int payloadHash = Arrays.hashCode(payload);
        return partitionId + "-" + keyHash + "-" + payloadHash;
    }
}