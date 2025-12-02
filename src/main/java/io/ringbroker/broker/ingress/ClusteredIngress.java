// src/main/java/io/ringbroker/broker/ingress/ClusteredIngress.java
package io.ringbroker.broker.ingress;

import com.google.protobuf.ByteString;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.delivery.Delivery;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.membership.replicator.AdaptiveReplicator;
import io.ringbroker.cluster.membership.resolver.ReplicaSetResolver;
import io.ringbroker.cluster.partitioner.Partitioner;
import io.ringbroker.core.lsn.Lsn;
import io.ringbroker.core.ring.RingBuffer;
import io.ringbroker.core.wait.WaitStrategy;
import io.ringbroker.offset.OffsetStore;
import io.ringbroker.registry.TopicRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

@Slf4j
@Getter
public final class ClusteredIngress {

    private static final CompletableFuture<Void> COMPLETED_FUTURE =
            CompletableFuture.completedFuture(null);

    private static final long EPOCH0 = 0L;
    private static final long APPEND_WAIT_TIMEOUT_MS = 10_000L;
    private static final long PARK_NANOS = 1_000L;

    private static final class EpochState {
        final AtomicLong lastSeqReserved;
        volatile boolean sealed;
        volatile long sealedEndSeq;

        EpochState(final long lastSeqInit) {
            this.lastSeqReserved = new AtomicLong(lastSeqInit);
            this.sealed = false;
            this.sealedEndSeq = -1L;
        }
    }

    private final ExecutorService replicationExecutor;

    private final Partitioner partitioner;
    private final int totalPartitions;
    private final int myNodeId;
    private final int clusterSize;

    private final ConcurrentMap<Integer, Ingress> ingressMap;
    private final ConcurrentMap<Integer, RemoteBrokerClient> clusterNodes;

    private final boolean idempotentMode;
    private final Map<Integer, Set<Long>> seenMessageIds;

    private final ConcurrentMap<Integer, Delivery> deliveryMap;

    private final OffsetStore offsetStore;
    private final TopicRegistry registry;
    private final BrokerRole myRole;
    private final ReplicaSetResolver replicaResolver;
    private final AdaptiveReplicator replicator;

    private final Path baseDataDir;
    private final int ringSize;
    private final WaitStrategy waitStrategy;
    private final long segmentCapacity;
    private final int batchSize;

    private final ConcurrentMap<Integer, EpochState> epoch0ByPartition = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, AtomicLong> nextSeqByPartition = new ConcurrentHashMap<>();

    private ClusteredIngress(final ExecutorService replicationExecutor,
                             final Partitioner partitioner,
                             final int totalPartitions,
                             final int myNodeId,
                             final int clusterSize,
                             final ConcurrentMap<Integer, Ingress> ingressMap,
                             final Map<Integer, RemoteBrokerClient> clusterNodes,
                             final boolean idempotentMode,
                             final Map<Integer, Set<Long>> seenMessageIds,
                             final ConcurrentMap<Integer, Delivery> deliveryMap,
                             final OffsetStore offsetStore,
                             final TopicRegistry registry,
                             final BrokerRole myRole,
                             final ReplicaSetResolver replicaResolver,
                             final AdaptiveReplicator replicator,
                             final Path baseDataDir,
                             final int ringSize,
                             final WaitStrategy waitStrategy,
                             final long segmentCapacity,
                             final int batchSize) {

        this.replicationExecutor = replicationExecutor;
        this.partitioner = partitioner;
        this.totalPartitions = totalPartitions;
        this.myNodeId = myNodeId;
        this.clusterSize = clusterSize;
        this.ingressMap = ingressMap;
        this.clusterNodes = new ConcurrentHashMap<>(clusterNodes);
        this.idempotentMode = idempotentMode;
        this.seenMessageIds = seenMessageIds;
        this.deliveryMap = deliveryMap;
        this.offsetStore = offsetStore;
        this.registry = registry;
        this.myRole = myRole;
        this.replicaResolver = replicaResolver;
        this.replicator = replicator;
        this.baseDataDir = baseDataDir;
        this.ringSize = ringSize;
        this.waitStrategy = waitStrategy;
        this.segmentCapacity = segmentCapacity;
        this.batchSize = batchSize;

        for (final var e : ingressMap.entrySet()) {
            final int pid = e.getKey();
            final Ingress ing = e.getValue();
            final long last = ing.getSegments().getHighWaterMark();
            epoch0ByPartition.put(pid, new EpochState(last));
            nextSeqByPartition.put(pid, new AtomicLong(last + 1));
        }
    }

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

        final ConcurrentMap<Integer, Ingress> ingressMap = new ConcurrentHashMap<>();
        final ConcurrentMap<Integer, Delivery> deliveryMap = new ConcurrentHashMap<>();
        final Map<Integer, Set<Long>> seenMessageIds = idempotentMode ? new ConcurrentHashMap<>() : Collections.emptyMap();

        for (int pid = 0; pid < totalPartitions; pid++) {
            if (Math.floorMod(pid, clusterSize) == myNodeId) {
                final Path partDir = baseDataDir.resolve("partition-" + pid);
                Files.createDirectories(partDir);

                final RingBuffer<byte[]> ring = new RingBuffer<>(ringSize, waitStrategy);
                final boolean forceDurable = (brokerRole == BrokerRole.PERSISTENCE);

                final Ingress ingress = Ingress.create(
                        registry, ring, partDir, segmentCapacity, batchSize, forceDurable
                );
                ingressMap.put(pid, ingress);

                deliveryMap.put(pid, new Delivery(ring));

                if (idempotentMode) {
                    seenMessageIds.put(pid, ConcurrentHashMap.newKeySet());
                }
            }
        }

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
                replicator,
                baseDataDir,
                ringSize,
                waitStrategy,
                segmentCapacity,
                batchSize
        );
    }

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

        int ownerNode = partitionId % clusterSize;
        if (ownerNode < 0) {
            ownerNode += clusterSize;
        }

        if (ownerNode == myNodeId) {
            if (idempotentMode && shouldDropDuplicate(partitionId, key, payload)) {
                return COMPLETED_FUTURE;
            }

            final long epoch = EPOCH0;
            final long seq = nextSeq(partitionId, epoch);

            final BrokerApi.Envelope appendEnv = BrokerApi.Envelope.newBuilder()
                    .setCorrelationId(correlationId)
                    .setAppend(BrokerApi.AppendRequest.newBuilder()
                            .setPartitionId(partitionId)
                            .setEpoch(epoch)
                            .setSeq(seq)
                            .setTopic(topic)
                            .setRetries(retries)
                            .setKey(key == null ? ByteString.EMPTY : ByteString.copyFrom(key))
                            .setPayload(ByteString.copyFrom(payload))
                            .build())
                    .build();

            final List<Integer> resolved = replicaResolver.replicas(partitionId);
            final List<Integer> replicas = new ArrayList<>(resolved.size());
            for (int nodeId : resolved) {
                if (nodeId != myNodeId) replicas.add(nodeId);
            }

            final CompletableFuture<Void> future = new CompletableFuture<>();
            replicationExecutor.submit(() -> {
                try {
                    final BrokerApi.ReplicationAck localAck =
                            appendAndWaitDurable(appendEnv.getAppend());

                    if (localAck.getStatus() != BrokerApi.ReplicationAck.Status.SUCCESS) {
                        throw new RuntimeException("Local append failed: " + localAck.getStatus() + " " + localAck.getErrorMessage());
                    }

                    if (!replicas.isEmpty()) {
                        replicator.replicate(appendEnv, replicas);
                    }

                    future.complete(null);
                } catch (final Exception e) {
                    future.completeExceptionally(e);
                }
            });
            return future;
        }

        final RemoteBrokerClient ownerClient = clusterNodes.get(ownerNode);
        if (ownerClient == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("No client for owner node " + ownerNode)
            );
        }

        final BrokerApi.Envelope envelope = buildPublishEnvelope(
                correlationId, topic, key, payload, partitionId, retries
        );

        return ownerClient.sendEnvelopeWithAck(envelope).thenApply(ack -> {
            if (ack.getStatus() != BrokerApi.ReplicationAck.Status.SUCCESS) {
                throw new RuntimeException("Forwarding failed: " + ack.getStatus());
            }
            return null;
        });
    }

    private static BrokerApi.Envelope buildPublishEnvelope(final long correlationId,
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

    private boolean shouldDropDuplicate(final int partitionId, final byte[] key, final byte[] payload) {
        final Set<Long> seen = seenMessageIds.get(partitionId);
        if (seen == null) {
            throw new IllegalStateException("Seen set missing for partition " + partitionId);
        }
        final long msgId = computeMessageId(partitionId, key, payload);
        return !seen.add(msgId);
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
                offsetStore.commit(topic, group, partitionId, sequence);
            });
        }
    }

    public CompletableFuture<BrokerApi.ReplicationAck> handleAppendAsync(final BrokerApi.AppendRequest a) {
        final CompletableFuture<BrokerApi.ReplicationAck> f = new CompletableFuture<>();
        replicationExecutor.submit(() -> {
            try {
                f.complete(appendAndWaitDurable(a));
            } catch (Throwable t) {
                f.complete(BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.ERROR_UNKNOWN)
                        .setErrorMessage(String.valueOf(t.getMessage()))
                        .build());
            }
        });
        return f;
    }

    public CompletableFuture<BrokerApi.ReplicationAck> handleAppendBatchAsync(final BrokerApi.AppendBatchRequest b) {
        final CompletableFuture<BrokerApi.ReplicationAck> f = new CompletableFuture<>();
        replicationExecutor.submit(() -> {
            try {
                f.complete(appendBatchAndWaitDurable(b));
            } catch (Throwable t) {
                f.complete(BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.ERROR_UNKNOWN)
                        .setErrorMessage(String.valueOf(t.getMessage()))
                        .build());
            }
        });
        return f;
    }

    public CompletableFuture<BrokerApi.ReplicationAck> handleEpochStatusAsync(final BrokerApi.EpochStatusRequest s) {
        return CompletableFuture.completedFuture(handleEpochStatus(s));
    }

    public CompletableFuture<BrokerApi.ReplicationAck> handleSealAsync(final BrokerApi.SealRequest s) {
        return CompletableFuture.completedFuture(handleSeal(s));
    }

    private BrokerApi.ReplicationAck handleEpochStatus(final BrokerApi.EpochStatusRequest s) {
        final int pid = s.getPartitionId();
        final long epoch = s.getEpoch();
        if (epoch != EPOCH0) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_INVALID_REQUEST)
                    .setErrorMessage("epoch not supported: " + epoch)
                    .build();
        }

        final Ingress ing = ingressMap.get(pid);
        final long persisted = (ing != null) ? ing.getSegments().getHighWaterMark() : -1L;

        return BrokerApi.ReplicationAck.newBuilder()
                .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                .setOffset(Lsn.encode(epoch, Math.max(-1L, persisted)))
                .build();
    }

    private BrokerApi.ReplicationAck handleSeal(final BrokerApi.SealRequest s) {
        final int pid = s.getPartitionId();
        final long epoch = s.getEpoch();
        if (epoch != EPOCH0) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_INVALID_REQUEST)
                    .setErrorMessage("epoch not supported: " + epoch)
                    .build();
        }

        final EpochState st = epoch0State(pid);
        final Ingress ing = getOrCreateIngress(pid);
        final long persisted = ing.getSegments().getHighWaterMark();

        st.sealed = true;
        st.sealedEndSeq = persisted;

        return BrokerApi.ReplicationAck.newBuilder()
                .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                .setOffset(Lsn.encode(epoch, persisted))
                .build();
    }

    private BrokerApi.ReplicationAck appendAndWaitDurable(final BrokerApi.AppendRequest a) {
        final int pid = a.getPartitionId();
        final long epoch = a.getEpoch();
        final long seq = a.getSeq();

        if (epoch != EPOCH0) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_INVALID_REQUEST)
                    .setErrorMessage("epoch not supported: " + epoch)
                    .build();
        }

        final String topic = a.getTopic();
        if (!registry.contains(topic)) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_INVALID_REQUEST)
                    .setErrorMessage("Unknown topic: " + topic)
                    .build();
        }

        final EpochState st = epoch0State(pid);

        if (st.sealed && seq > st.sealedEndSeq) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY)
                    .setErrorMessage("Epoch sealed at " + st.sealedEndSeq)
                    .build();
        }

        final Ingress ing = getOrCreateIngress(pid);

        while (true) {
            final long cur = st.lastSeqReserved.get();

            if (seq <= cur) {
                if (!awaitPersisted(ing, seq, APPEND_WAIT_TIMEOUT_MS)) {
                    return BrokerApi.ReplicationAck.newBuilder()
                            .setStatus(BrokerApi.ReplicationAck.Status.ERROR_PERSISTENCE_FAILED)
                            .setErrorMessage("timeout waiting for durable seq=" + seq)
                            .build();
                }
                return BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                        .setOffset(Lsn.encode(epoch, seq))
                        .build();
            }

            if (seq != cur + 1) {
                return BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY)
                        .setErrorMessage("Gap. expected=" + (cur + 1) + " got=" + seq)
                        .build();
            }

            if (!st.lastSeqReserved.compareAndSet(cur, seq)) continue;

            try {
                ing.publish(topic, a.getRetries(), a.getPayload().toByteArray());
            } catch (Throwable t) {
                st.lastSeqReserved.compareAndSet(seq, cur);
                return BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.ERROR_PERSISTENCE_FAILED)
                        .setErrorMessage(String.valueOf(t.getMessage()))
                        .build();
            }

            if (!awaitPersisted(ing, seq, APPEND_WAIT_TIMEOUT_MS)) {
                return BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.ERROR_PERSISTENCE_FAILED)
                        .setErrorMessage("timeout waiting for durable seq=" + seq)
                        .build();
            }

            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                    .setOffset(Lsn.encode(epoch, seq))
                    .build();
        }
    }

    private BrokerApi.ReplicationAck appendBatchAndWaitDurable(final BrokerApi.AppendBatchRequest b) {
        final int pid = b.getPartitionId();
        final long epoch = b.getEpoch();
        final long baseSeq = b.getBaseSeq();

        if (epoch != EPOCH0) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_INVALID_REQUEST)
                    .setErrorMessage("epoch not supported: " + epoch)
                    .build();
        }

        final String topic = b.getTopic();
        if (!registry.contains(topic)) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_INVALID_REQUEST)
                    .setErrorMessage("Unknown topic: " + topic)
                    .build();
        }

        final var payloads = b.getPayloadsList();
        final int n = payloads.size();
        if (n == 0) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                    .setOffset(Lsn.encode(epoch, baseSeq - 1))
                    .build();
        }

        final EpochState st = epoch0State(pid);
        final Ingress ing = getOrCreateIngress(pid);

        while (true) {
            final long cur = st.lastSeqReserved.get();

            if (baseSeq <= cur) {
                final long last = Math.max(cur, baseSeq + n - 1L);
                if (!awaitPersisted(ing, last, APPEND_WAIT_TIMEOUT_MS)) {
                    return BrokerApi.ReplicationAck.newBuilder()
                            .setStatus(BrokerApi.ReplicationAck.Status.ERROR_PERSISTENCE_FAILED)
                            .setErrorMessage("timeout waiting for durable seq=" + last)
                            .build();
                }
                return BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                        .setOffset(Lsn.encode(epoch, last))
                        .build();
            }

            final long expected = cur + 1;
            if (baseSeq != expected) {
                return BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY)
                        .setErrorMessage("Gap. expected=" + expected + " got=" + baseSeq)
                        .build();
            }

            final long last = cur + n;
            if (!st.lastSeqReserved.compareAndSet(cur, last)) continue;

            try {
                for (int i = 0; i < n; i++) {
                    ing.publish(topic, b.getRetries(), payloads.get(i).toByteArray());
                }
            } catch (Throwable t) {
                st.lastSeqReserved.compareAndSet(last, cur);
                return BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.ERROR_PERSISTENCE_FAILED)
                        .setErrorMessage(String.valueOf(t.getMessage()))
                        .build();
            }

            if (!awaitPersisted(ing, last, APPEND_WAIT_TIMEOUT_MS)) {
                return BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.ERROR_PERSISTENCE_FAILED)
                        .setErrorMessage("timeout waiting for durable seq=" + last)
                        .build();
            }

            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                    .setOffset(Lsn.encode(epoch, last))
                    .build();
        }
    }

    private boolean awaitPersisted(final Ingress ing, final long targetSeq, final long timeoutMs) {
        if (targetSeq < 0) return true;

        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        long spins = 0;

        while (System.nanoTime() < deadline) {
            if (ing.getSegments().getHighWaterMark() >= targetSeq) {
                return true;
            }
            if (spins < 1_000) {
                spins++;
                Thread.onSpinWait();
            } else {
                LockSupport.parkNanos(PARK_NANOS);
            }
        }
        return ing.getSegments().getHighWaterMark() >= targetSeq;
    }

    private Ingress getOrCreateIngress(final int partitionId) {
        final Ingress existing = ingressMap.get(partitionId);
        if (existing != null) return existing;

        final Ingress created = ingressMap.computeIfAbsent(partitionId, pid -> {
            try {
                final Path partDir = baseDataDir.resolve("partition-" + pid);
                Files.createDirectories(partDir);

                final RingBuffer<byte[]> ring = new RingBuffer<>(ringSize, waitStrategy);
                final boolean forceDurable = (myRole == BrokerRole.PERSISTENCE);

                final Ingress ingress = Ingress.create(
                        registry, ring, partDir, segmentCapacity, batchSize, forceDurable
                );

                deliveryMap.putIfAbsent(pid, new Delivery(ring));
                if (idempotentMode) {
                    seenMessageIds.computeIfAbsent(pid, __ -> ConcurrentHashMap.newKeySet());
                }

                final long last = ingress.getSegments().getHighWaterMark();
                epoch0ByPartition.putIfAbsent(pid, new EpochState(last));
                nextSeqByPartition.putIfAbsent(pid, new AtomicLong(last + 1));

                return ingress;
            } catch (Exception e) {
                throw new RuntimeException("Failed to create ingress for partition " + pid, e);
            }
        });

        epoch0ByPartition.computeIfAbsent(partitionId, pid -> {
            final long last = created.getSegments().getHighWaterMark();
            return new EpochState(last);
        });

        return created;
    }

    private EpochState epoch0State(final int partitionId) {
        return epoch0ByPartition.computeIfAbsent(partitionId, pid -> {
            final Ingress ing = ingressMap.get(pid);
            final long last = (ing != null) ? ing.getSegments().getHighWaterMark() : -1L;
            return new EpochState(last);
        });
    }

    private long nextSeq(final int partitionId, final long epoch) {
        if (epoch != EPOCH0) throw new IllegalStateException("epoch not supported: " + epoch);

        final AtomicLong a = nextSeqByPartition.computeIfAbsent(partitionId, pid -> {
            final Ingress ing = getOrCreateIngress(pid);
            final long last = ing.getSegments().getHighWaterMark();
            epoch0ByPartition.putIfAbsent(pid, new EpochState(last));
            return new AtomicLong(last + 1);
        });

        return a.getAndIncrement();
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

    private long computeMessageId(final int partitionId, final byte[] key, final byte[] payload) {
        final int keyHash = (key != null ? Arrays.hashCode(key) : 0);
        final int payloadHash = Arrays.hashCode(payload);
        final int combined = 31 * keyHash + payloadHash;
        return (((long) partitionId) << 32) ^ (combined & 0xFFFF_FFFFL);
    }
}
