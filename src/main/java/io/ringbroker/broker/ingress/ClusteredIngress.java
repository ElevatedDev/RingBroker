package io.ringbroker.broker.ingress;

import io.ringbroker.broker.delivery.Delivery;
import io.ringbroker.cluster.type.Partitioner;
import io.ringbroker.cluster.type.RemoteBrokerClient;
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
 * Cluster-aware ingress that handles partitioned publishing and per-topic subscription,
 * while preserving batching, DLQ, validation, and ring-based delivery via Ingress.
 */
@Slf4j
@RequiredArgsConstructor
@Getter
public final class ClusteredIngress {
    private final Partitioner partitioner;
    private final int totalPartitions;
    private final int myNodeId;
    private final int clusterSize;
    private final Map<Integer, Ingress> ingressMap;                     // one Ingress per local partition
    private final Map<Integer, RemoteBrokerClient> clusterNodes;        // stubs to other brokers
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
                                          final int threadsPerPartition,
                                          final int batchSize,
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
                        threadsPerPartition,
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
     * Full publish entry point with retries, dedup, and forwarding.
     */
    public void publish(final String topic, final byte[] key, final int retries, final byte[] payload) {
        final int partitionId = partitioner.selectPartition(key, totalPartitions);
        final int owner = Math.floorMod(partitionId, clusterSize);

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
            final RemoteBrokerClient client = clusterNodes.get(owner);
            if (client == null) {
                log.error("No RemoteBrokerClient for node {} (partition {})", owner, partitionId);
                return;
            }
            client.sendMessage(topic, key, payload);
        }
    }

    /**
     * Subscribes to a topic across all owned partitions, starting from committed offset.
     */
    public void subscribeTopic(final String topic,
                               final String group,
                               final BiConsumer<Long, byte[]> handler) {
        if (!registry.contains(topic)) {
            throw new IllegalArgumentException("Unknown topic: " + topic);
        }

        for (final Map.Entry<Integer, Delivery> e : deliveryMap.entrySet()) {
            final int partitionId = e.getKey();
            final Delivery delivery = e.getValue();

            final long committed = offsetStore.fetch(topic, group, partitionId);
            final long startOffset = Math.max(committed, 0L);

            delivery.subscribe(startOffset, (seq, msg) -> {
                handler.accept(seq, msg);
                offsetStore.commit(topic, group, partitionId, seq);
            });
        }
    }


    private String computeMessageId(final int partitionId, final byte[] key, final byte[] payload) {
        final int keyHash = (key != null ? Arrays.hashCode(key) : 0);
        final int payloadHash = Arrays.hashCode(payload);
        return partitionId + "-" + keyHash + "-" + payloadHash;
    }
}
