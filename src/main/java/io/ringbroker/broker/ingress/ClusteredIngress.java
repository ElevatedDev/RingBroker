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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    /**
     * A shared virtual-thread executor for asynchronous replication tasks.
     */
    private static final ExecutorService VT_EXECUTOR =
            Executors.newVirtualThreadPerTaskExecutor();
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

    /**
     * Creates and initializes a {@code ClusteredIngress} instance with the provided configuration.
     *
     * @param registry        the topic registry for topic validation and schema lookup
     * @param partitioner     the partitioner to map each message key to a partition
     * @param totalPartitions the total number of partitions in the cluster
     * @param myNodeId        the ID of this node (0..clusterSize-1)
     * @param clusterSize     the total number of nodes in the cluster
     * @param clusterNodes    map from nodeId to RemoteBrokerClient for inter-node forwarding
     * @param baseDataDir     the base directory for ledger segment storage per-partition
     * @param ringSize        the size of each ring-buffer for in-memory delivery
     * @param waitStrategy    the wait strategy for each ring-buffer
     * @param segmentCapacity the capacity of each ledger segment (bytes)
     * @param batchSize       the maximum number of messages per batch for disk writes
     * @param idempotentMode  if true, deduplicate by messageId before writing
     * @param offsetStore     the OffsetStore to track committed offsets
     * @param brokerRole      this node’s role (INGESTION or PERSISTENCE)
     * @param replicaResolver resolves which persistence replicas to send to per partition
     * @param replicator      handles sending to replicas and awaiting quorum
     * @return a fully initialized {@code ClusteredIngress} instance
     * @throws IOException if any Ingress (per-partition) cannot be created
     */
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
     * This convenience method delegates to {@code publish(correlationId, topic, key, retries, payload)}
     * with a default correlationId of {@code System.nanoTime()} if INGESTION, or {@code 0L} if PERSISTENCE.
     *
     * @param topic   the topic to publish to
     * @param key     the message key used for partitioning
     * @param payload the message payload
     */
    public void publish(final String topic, final byte[] key, final byte[] payload) {
        final long defaultCorrelationId = (myRole == BrokerRole.INGESTION) ? System.nanoTime() : 0L;
        publish(defaultCorrelationId, topic, key, 0, payload);
    }

    /**
     * Publishes a message to the appropriate partition, handling retries, deduplication, local write,
     * and asynchronous forwarding or replication without blocking the caller on quorum.
     *
     * @param correlationId The correlation ID from the incoming request (or generated for replication).
     * @param topic         the topic to publish to
     * @param key           the message key used for partitioning
     * @param retries       the number of retry attempts for publishing (from original client)
     * @param payload       the message payload
     */
    public void publish(final long correlationId,
                        final String topic,
                        final byte[] key,
                        final int retries,
                        final byte[] payload) {

        final int partitionId = partitioner.selectPartition(key, totalPartitions);

        // If single-node INGESTION, treat as local owner with no replication
        if (clusterSize == 1 && myRole == BrokerRole.INGESTION) {
            handleLocalPublish(partitionId, topic, retries, payload, key);
            return;
        }

        final int ownerNode = Math.floorMod(partitionId, clusterSize);

        // Build the Envelope once for any forwarding/replication
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

        // ── SCENARIO 1: This node is the designated primary owner for the partition ──
        if (ownerNode == myNodeId) {
            log.debug("Node {}: Handling message locally for owned partition {}. CorrId: {}, Topic: {}",
                    myNodeId, partitionId, correlationId, topic);

            // 1) Local synchronous write to Ingress (and disk if PERSISTENCE)
            handleLocalPublish(partitionId, topic, retries, payload, key);

            // 2) Determine “other” persistence replicas (excluding ourselves)
            final List<Integer> replicas = replicaResolver.replicas(partitionId);
            replicas.removeIf(id -> id == myNodeId);

            // If there are no other replicas (single‐node or quorum=1), skip async replication
            if (replicas.isEmpty()) {
                log.debug("Node {}: No other replicas for partition {}. Skipping replication.",
                        myNodeId, partitionId);
                return;
            }

            // 3) Fire off asynchronous replication on a virtual thread
            VT_EXECUTOR.submit(() -> {
                try {
                    replicator.replicate(envelope, replicas);
                    log.debug("Node {}: Replication quorum achieved for partition {} (corrId={}).",
                            myNodeId, partitionId, correlationId);
                } catch (final Exception e) {
                    log.warn("Node {}: Async replication failed for partition {} (corrId={}): {}",
                            myNodeId, partitionId, correlationId, e.getMessage());
                }
            });
            return;
        }

        // ── SCENARIO 2: This node is INGESTION, but not the owner ──
        if (myRole == BrokerRole.INGESTION) {
            log.debug("Node {} (INGESTION): Forwarding message for partition {} → owner {} (corrId={}).",
                    myNodeId, partitionId, ownerNode, correlationId);

            final RemoteBrokerClient ownerClient = clusterNodes.get(ownerNode);
            if (ownerClient == null) {
                throw new IllegalStateException("Cannot forward message: no client for owner node " + ownerNode);
            }

            // Fire‐and‐forget forwarding; do not block on quorum here.
            ownerClient.sendEnvelope(envelope);
            return;
        }

        // ── SCENARIO 3: This node is PERSISTENCE, but not the owner ──
        log.warn("Node {} (PERSISTENCE): Received message for non‐owned partition {}. Forwarding to {} (corrId={}).",
                myNodeId, partitionId, ownerNode, correlationId);

        final RemoteBrokerClient client = clusterNodes.get(ownerNode);
        if (client == null) {
            throw new IllegalStateException(
                    "Cannot forward message: no client for owner node " + ownerNode +
                            " for partition " + partitionId);
        }

        // Fire‐and‐forget forwarding; never block on quorum.
        client.sendEnvelope(envelope);
    }

    /**
     * Handles a publish request when this node is the owner of {@code partitionId}.
     * Performs optional idempotent duplicate check, then synchronously writes to the local {@link Ingress}.
     *
     * @param partitionId the partition being written
     * @param topic       the topic name
     * @param retries     retry count for DLQ logic
     * @param payload     the message payload
     * @param key         the partitioning key
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
                log.debug("Node {}: Duplicate message skipped: partition={}, id={}",
                        myNodeId, partitionId, msgId);
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
     *
     * @param topic   the topic to subscribe to
     * @param group   the consumer group
     * @param handler a callback that receives (offset, payload) for each message
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

            final long committed = Math.max(0L, offsetStore.fetch(topic, group, partitionId));

            delivery.subscribe(committed, (sequence, message) -> {
                handler.accept(sequence, message);
                offsetStore.commit(topic, group, partitionId, sequence);
            });
        }
    }

    /**
     * Shuts down all local Ingress instances, releasing any resources held.
     * Also shuts down the virtual-thread executor used for background replication.
     *
     * @throws IOException if an I/O error occurs while closing Ingress instances
     */
    public void shutdown() throws IOException {
        log.info("Node {}: Shutting down ClusteredIngress, closing all local Ingress instances.", myNodeId);
        for (final Ingress ingress : ingressMap.values()) {
            ingress.close();
        }
        log.info("Node {}: All local Ingress instances closed.", myNodeId);

        // Shut down the virtual-thread executor gracefully
        VT_EXECUTOR.shutdown();
        try {
            if (!VT_EXECUTOR.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                VT_EXECUTOR.shutdownNow();
            }
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            VT_EXECUTOR.shutdownNow();
        }
        log.info("Virtual-thread replication executor shut down.");
    }

    /**
     * Computes a unique message identifier for deduplication purposes.
     *
     * @param partitionId the partition
     * @param key         the message key
     * @param payload     the message payload
     * @return a string id used to detect duplicates
     */
    private String computeMessageId(final int partitionId, final byte[] key, final byte[] payload) {
        final int keyHash = (key != null ? Arrays.hashCode(key) : 0);
        final int payloadHash = Arrays.hashCode(payload);
        return partitionId + "-" + keyHash + "-" + payloadHash;
    }
}
