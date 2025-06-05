package io.ringbroker.broker.ingress;

import com.google.protobuf.ByteString;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.delivery.Delivery;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.membership.replicator.FlashReplicator;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * ClusteredIngress manages partitioned message publishing and subscription in a clustered broker setup.
 * <p>
 * Responsibilities include:
 * <ul>
 *   <li>Partition-aware publishing and subscription for topics</li>
 *   <li>Deduplication of messages in idempotent mode</li>
 *   <li>Batching, dead-letter queue (DLQ), and validation via Ingress</li>
 *   <li>Ring-buffer-based delivery for high throughput</li>
 *   <li>Coordination with remote broker nodes for forwarding messages</li>
 *   <li>Offset management and per-partition delivery</li>
 * </ul>
 * This class is intended to be used as the main entry point for message ingress and subscription in a clustered broker.
 */
@Slf4j
@RequiredArgsConstructor
@Getter
public final class ClusteredIngress {
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
    private final FlashReplicator replicator;

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
                                          final FlashReplicator replicator) throws IOException {

        final Map<Integer, Ingress> ingressMap = new HashMap<>();
        final Map<Integer, Delivery> deliveryMap = new HashMap<>();
        final Map<Integer, Set<String>> seenMessageIds = idempotentMode
                ? new HashMap<>()
                : Collections.emptyMap();

        for (int pid = 0; pid < totalPartitions; pid++) {
            if (Math.floorMod(pid, clusterSize) == myNodeId) { // Node owns this partition
                final Path partDir = baseDataDir.resolve("partition-" + pid);
                final RingBuffer<byte[]> ring = new RingBuffer<>(ringSize, waitStrategy);

                final boolean forceDurableWritesForLocalIngress = (brokerRole == BrokerRole.PERSISTENCE);

                final Ingress ingress = Ingress.create(
                        registry,
                        ring,
                        partDir,
                        segmentCapacity,
                        batchSize,
                        forceDurableWritesForLocalIngress
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
     * Publishes a message (non-retry shortcut, assuming correlationId needs to be handled).
     * This simplified publish might be problematic if correlationId is mandatory for all paths.
     * It's primarily for internal use or cases where correlation is managed differently.
     */
    public void publish(final String topic, final byte[] key, final byte[] payload) {
        final long defaultCorrelationId = (myRole == BrokerRole.INGESTION) ? System.nanoTime() : 0L;

        log.warn("ClusteredIngress.publish(topic,key,payload) called without explicit correlationId. Using default: {}", defaultCorrelationId);
        publish(defaultCorrelationId, topic, key, 0, payload);
    }

    /**
     * Publishes a message to the appropriate partition, handling retries, deduplication, and forwarding.
     *
     * @param correlationId The correlation ID from the incoming request or generated for replication.
     * @param topic    the topic to publish to
     * @param key      the message key used for partitioning
     * @param retries  the number of retry attempts for publishing (from original client)
     * @param payload  the message payload
     */
    public void publish(final long correlationId,
                        final String topic,
                        final byte[] key,
                        final int retries,
                        final byte[] payload) {

        final int partitionId = partitioner.selectPartition(key, totalPartitions);
        final int ownerNode = Math.floorMod(partitionId, clusterSize);

        // Scenario 1: This node is the designated primary owner for the partition.
        if (ownerNode == myNodeId) {
            log.debug("Node {}: Handling message locally for owned partition {}. CorrId: {}, Topic: {}",
                    myNodeId, partitionId, correlationId, topic);
            handleLocalPublish(partitionId, topic, retries, payload, key);

            // Now replicate to other persistence replicas (remove ourselves if present)
            final List<Integer> replicas = replicaResolver.replicas(partitionId);
            replicas.removeIf(id -> id == myNodeId);

            if (!replicas.isEmpty()) {
                final BrokerApi.Message.Builder msgBuilder = BrokerApi.Message.newBuilder()
                        .setTopic(topic)
                        .setRetries(retries)
                        .setKey(key == null ? ByteString.EMPTY : ByteString.copyFrom(key))
                        .setPayload(ByteString.copyFrom(payload))
                        .setPartitionId(partitionId);

                final BrokerApi.Envelope envToReplicate = BrokerApi.Envelope.newBuilder()
                        .setCorrelationId(correlationId)
                        .setPublish(msgBuilder.build())
                        .build();

                try {
                    replicator.replicate(envToReplicate, replicas);
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for replication quorum", ie);
                } catch (final Exception e) {
                    log.error("Node {}: Replication quorum failed for partition {} (corrId: {}). Error: {}",
                            myNodeId, partitionId, correlationId, e.getMessage(), e);
                    throw new RuntimeException("Replication quorum failed: " + e.getMessage(), e);
                }
            }
            return;
        }

        // Scenario 2: This node is INGESTION, and partition owned by another node.
        if (myRole == BrokerRole.INGESTION) {
            final BrokerApi.Message.Builder msgBuilder = BrokerApi.Message.newBuilder()
                    .setTopic(topic)
                    .setRetries(retries)
                    .setKey(key == null ? ByteString.EMPTY : ByteString.copyFrom(key))
                    .setPayload(ByteString.copyFrom(payload))
                    .setPartitionId(partitionId);

            final BrokerApi.Envelope envToReplicate = BrokerApi.Envelope.newBuilder()
                    .setCorrelationId(correlationId)
                    .setPublish(msgBuilder.build())
                    .build();

            final List<Integer> replicas = replicaResolver.replicas(partitionId);
            log.debug("INGESTION node {}: Replicating message for partition {} (corrId: {}) to replicas: {}",
                    myNodeId, partitionId, correlationId, replicas);
            try {
                replicator.replicate(envToReplicate, replicas);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for replication quorum", ie);
            } catch (final Exception e) {
                log.error("INGESTION node {}: Replication quorum failed for partition {} (corrId: {}). Error: {}",
                        myNodeId, partitionId, correlationId, e.getMessage(), e);
                throw new RuntimeException("Replication quorum failed: " + e.getMessage(), e);
            }
            return;
        }
        final RemoteBrokerClient client = clusterNodes.get(ownerNode);

        if (client == null) {
            log.error("Node {}: No RemoteBrokerClient for owner node {} (partition {}). Cannot forward message with corrId: {}.",
                    myNodeId, ownerNode, partitionId, correlationId);

            throw new IllegalStateException("Cannot forward message: No client for owner node " + ownerNode +
                    " for partition " + partitionId);
        }

        client.sendMessage(topic, key, payload);
    }

    /**
     * Handles a publish request when this node is the owner of {@code partitionId}.
     * Performs optional idempotent duplicate check and forwards to the local {@link Ingress}.
     */
    public void handleLocalPublish(final int partitionId,
                                   final String topic,
                                   final int retries,
                                   final byte[] payload,
                                   final byte[] key) {

        if (idempotentMode) {
            final Set<String> seen = Objects.requireNonNull(
                    seenMessageIds.get(partitionId), "Seen set missing for partition " + partitionId);

            final String msgId = computeMessageId(partitionId, key, payload);
            if (!seen.add(msgId)) {
                log.debug("Node {}: Duplicate message skipped: partition={}, id={}", myNodeId, partitionId, msgId);
                return;
            }
        }

        final Ingress ingress = ingressMap.get(partitionId);
        if (ingress == null) {
            log.error("Node {}: No local Ingress for owned partition {}. Message for topic {} may be lost.",
                    myNodeId, partitionId, topic);
            throw new IllegalStateException("Internal error: No Ingress for owned partition " + partitionId);
        }
        ingress.publish(topic, retries, payload);
    }


    /**
     * Subscribes to a topic across all owned partitions, starting from the committed offset for each.
     */
    public void subscribeTopic(final String topic,
                               final String group,
                               final BiConsumer<Long, byte[]> handler) {
        if (!registry.contains(topic)) {
            throw new IllegalArgumentException("Unknown topic: " + topic);
        }

        for (final Map.Entry<Integer, Delivery> entry : deliveryMap.entrySet()) {
            final int partitionId = entry.getKey();
            final Delivery delivery = entry.getValue();

            // Fetch committed offset, ensuring it's not negative.
            final long committed = Math.max(0L, offsetStore.fetch(topic, group, partitionId));

            delivery.subscribe(committed, (sequence, message) -> {
                handler.accept(sequence, message);
                offsetStore.commit(topic, group, partitionId, sequence);
            });
        }
    }

    /**
     * Shuts down all local Ingress instances, releasing any resources held.
     */
    public void shutdown() throws IOException {
        log.info("Node {}: Shutting down ClusteredIngress, closing all local Ingress instances.", myNodeId);
        for (final Ingress ingress : ingressMap.values()) {
            ingress.close();
        }
        log.info("Node {}: All local Ingress instances closed.", myNodeId);
    }

    /**
     * Computes a unique message identifier for deduplication purposes.
     */
    private String computeMessageId(final int partitionId, final byte[] key, final byte[] payload) {
        final int keyHash = (key != null ? Arrays.hashCode(key) : 0);
        final int payloadHash = Arrays.hashCode(payload);
        return partitionId + "-" + keyHash + "-" + payloadHash;
    }
}