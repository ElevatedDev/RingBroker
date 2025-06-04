package io.ringbroker.broker.ingress;

import io.ringbroker.broker.delivery.Delivery;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.partitioner.Partitioner;
import io.ringbroker.cluster.manager.ClusterManager;
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
    private final Map<Integer, Ingress> ingressMap;                     // one Ingress per local partition
    private final int clusterSize;
    private final Map<Integer, RemoteBrokerClient> clusterNodes;        // stubs to other brokers
    private final ClusterManager clusterManager;
    private final boolean idempotentMode;
    private final Map<Integer, Set<String>> seenMessageIds;             // only used if idempotentMode
    private final Map<Integer, Delivery> deliveryMap;                   // per-partition delivery
    private final OffsetStore offsetStore;
    private final TopicRegistry registry;

    public static ClusteredIngress create(final TopicRegistry registry,
                                          final Partitioner partitioner,
                                          final int totalPartitions,
                                          final int myNodeId,
                                          final int clusterSize,
                                          final Map<Integer, RemoteBrokerClient> clusterNodes,
                                          final Path baseDataDir,
                                          final int ringSize,
                                          final WaitStrategy waitStrategy,
                                          final long segmentSize,
                                          final int batchSize,
                                          final int replicationFactor,
                                          final boolean idempotentMode,
                                          final OffsetStore offsetStore) throws IOException {

        final Map<Integer, Ingress> ingressMap = new HashMap<>();
        final Map<Integer, Delivery> deliveryMap = new HashMap<>();
        final Map<Integer, Set<String>> seenMessageIds = idempotentMode
                ? new HashMap<>()
                : Collections.emptyMap();

        for (int pid = 0; pid < totalPartitions; pid++) {
            if (pid % clusterSize == myNodeId) {
                final Path partDir = baseDataDir.resolve("partition-" + pid);
                final RingBuffer<byte[]> ring = new RingBuffer<>(ringSize, waitStrategy);

                final Ingress ingress = Ingress.create(
                        registry,
                        ring,
                        partDir,
                        segmentSize,
                        batchSize
                );
                ingressMap.put(pid, ingress);

                final Delivery delivery = new Delivery(ring);
                deliveryMap.put(pid, delivery);

                if (idempotentMode) {
                    seenMessageIds.put(pid, ConcurrentHashMap.newKeySet());
                }
            }
        }

        final ClusterManager mgr = new ClusterManager(clusterNodes, myNodeId, replicationFactor);

        return new ClusteredIngress(
                partitioner,
                totalPartitions,
                myNodeId,
                ingressMap,
                clusterSize,
                clusterNodes,
                mgr,
                idempotentMode,
                seenMessageIds,
                deliveryMap,
                offsetStore,
                registry
        );
    }

    /**
     * Publishes a message (non-retry shortcut).
     */
    public void publish(final String topic, final byte[] key, final byte[] payload) {
        publish(topic, key, 0, payload);
    }

    /**
     * Publishes a message to the appropriate partition, handling retries, deduplication, and forwarding.
     * <p>
     * The partition is selected using the configured {@link Partitioner}. If the current node owns the partition,
     * the message is published locally, with optional idempotency checks to avoid duplicates. If another node owns
     * the partition, the message is forwarded to the appropriate remote broker client.
     * </p>
     *
     * @param topic    the topic to publish to
     * @param key      the message key used for partitioning
     * @param retries  the number of retry attempts for publishing
     * @param payload  the message payload
     */
    public void publish(final String topic, final byte[] key, final int retries, final byte[] payload) {
        final int partitionId = partitioner.selectPartition(key, totalPartitions);
        final int owner = clusterManager.getLeader(partitionId);

        if (owner == myNodeId) {
            if (idempotentMode) {
                final Set<String> seen = Objects.requireNonNull(seenMessageIds.get(partitionId));
                final String msgId = computeMessageId(partitionId, key, payload);

                if (!seen.add(msgId)) {
                    log.debug("Duplicate skipped: partition={} id={}", partitionId, msgId);
                    return;
                }
            }

            final Ingress ingress = ingressMap.get(partitionId);

            if (ingress == null) {
                log.error("No local Ingress for partition {}", partitionId);
                return;
            }

            ingress.publish(topic, retries, payload);
        } else {
            final RemoteBrokerClient client = clusterManager.clientFor(owner);

            if (client == null) {
                log.error("No RemoteBrokerClient for node {} (partition {})", owner, partitionId);
                return;
            }

            client.sendMessage(topic, key, payload);
        }
    }

    /**
     * Subscribes to a topic across all owned partitions, starting from the committed offset for each partition.
     * <p>
     * For each owned partition, retrieves the last committed offset for the given topic and group,
     * then subscribes to the corresponding Delivery instance starting from that offset. Each message
     * received is passed to the provided handler, and the offset is committed after processing.
     * </p>
     *
     * @param topic   the topic to subscribe to
     * @param group   the consumer group identifier
     * @param handler the handler to process each message (receives sequence number and message bytes)
     * @throws IllegalArgumentException if the topic is not registered
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

            final long committed = offsetStore.fetch(topic, group, partitionId);
            final long startOffset = Math.max(committed, 0L);

            delivery.subscribe(startOffset, (sequence, message) -> {
                handler.accept(sequence, message);
                offsetStore.commit(topic, group, partitionId, sequence);
            });
        }
    }

    /**
     * Shuts down all local Ingress instances, releasing any resources held.
     * <p>
     * This method should be called during broker shutdown to ensure proper cleanup.
     * </p>
     *
     * @throws IOException if an I/O error occurs while closing an Ingress
     */
    public void shutdown() throws IOException {
        for (final Ingress ingress : ingressMap.values()) {
            ingress.close();
        }
    }

    /**
     * Computes a unique message identifier for deduplication purposes.
     * <p>
     * The identifier is constructed using the partition ID, the hash of the key (if present),
     * and the hash of the payload. This helps ensure that messages with the same key and payload
     * in the same partition are considered duplicates in idempotent mode.
     * </p>
     *
     * @param partitionId the partition to which the message belongs
     * @param key         the message key (may be null)
     * @param payload     the message payload
     * @return a string representing the unique message identifier
     */
    private String computeMessageId(final int partitionId, final byte[] key, final byte[] payload) {
        final int keyHash = (key != null ? Arrays.hashCode(key) : 0);
        final int payloadHash = Arrays.hashCode(payload);

        return partitionId + "-" + keyHash + "-" + payloadHash;
    }
}
