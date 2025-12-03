// src/main/java/io/ringbroker/broker/ingress/ClusteredIngress.java
package io.ringbroker.broker.ingress;

import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.delivery.Delivery;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.membership.replicator.AdaptiveReplicator;
import io.ringbroker.cluster.membership.resolver.ReplicaSetResolver;
import io.ringbroker.cluster.metadata.*;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

@Slf4j
@Getter
public final class ClusteredIngress {

    private static final CompletableFuture<Void> COMPLETED_FUTURE =
            CompletableFuture.completedFuture(null);

    private static final long APPEND_WAIT_TIMEOUT_MS = 10_000L;
    private static final long PARK_NANOS = 1_000L;
    private static final long SEQ_ROLLOVER_THRESHOLD = (1L << 40) - 1_000_000L; // guard band before seq overflow
    private static final long BACKFILL_INTERVAL_MS = 5_000L;
    private static final int APPEND_RETRY_LIMIT = 1; // retry once on fencing/not-ready
    private final BackfillPlanner backfillPlanner;
    private final int backfillBatchSize = 64;
    private final ScheduledExecutorService backfillExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                final Thread t = new Thread(r, "backfill-worker");
                t.setDaemon(true);
                return t;
            });
    private final ConcurrentMap<Integer, PartitionEpochs> epochsByPartition = new ConcurrentHashMap<>();
    private final LogMetadataStore metadataStore;

    private final ExecutorService replicationExecutor;

    private final Partitioner partitioner;
    private final int totalPartitions;
    private final int myNodeId;
    private final int clusterSize;

    private final ConcurrentMap<Integer, Ingress> ingressMap;
    private final ConcurrentMap<Integer, RemoteBrokerClient> clusterNodes;

    private final boolean idempotentMode;
    private final Map<Integer, Set<Long>> seenMessageIds;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final ConcurrentMap<Integer, Delivery> deliveryMap;
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
                             final LogMetadataStore metadataStore,
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
        this.metadataStore = metadataStore;
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
        this.backfillPlanner = new BackfillPlanner(metadataStore);

        for (final var e : ingressMap.entrySet()) {
            final int pid = e.getKey();
            final Ingress ing = e.getValue();
            ing.setActiveEpoch(0L);
            final long last = ing.getVirtualLog().forEpoch(0L).getHighWaterMark();

            final List<Integer> placementNodes = replicaResolver.replicas(pid);
            final EpochPlacement placement = new EpochPlacement(0L, placementNodes, replicator.getAckQuorum());
            metadataStore.bootstrapIfAbsent(pid, placement, Math.max(0, last + 1));

            final PartitionEpochs pe = new PartitionEpochs();
            pe.highestSeenEpoch.set(FenceStore.loadHighest(baseDataDir.resolve("partition-" + pid)));
            pe.active = new EpochState(0L, last);
            pe.activePlacement = placement;
            loadFenceState(baseDataDir.resolve("partition-" + pid), pe);
            epochsByPartition.put(pid, pe);
        }

        backfillExecutor.scheduleAtFixedRate(this::backfillTick, 5_000L, BACKFILL_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

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
                                          final AdaptiveReplicator replicator,
                                          final LogMetadataStore metadataStore) throws IOException {

        final ConcurrentMap<Integer, Ingress> ingressMap = new ConcurrentHashMap<>();
        final ConcurrentMap<Integer, Delivery> deliveryMap = new ConcurrentHashMap<>();
        final Map<Integer, Set<Long>> seenMessageIds = idempotentMode ? new ConcurrentHashMap<>() : Collections.emptyMap();

        for (int pid = 0; pid < totalPartitions; pid++) {
            if (Math.floorMod(pid, clusterSize) == myNodeId) {
                final Path partDir = baseDataDir.resolve("partition-" + pid);
                Files.createDirectories(partDir);

                final RingBuffer<byte[]> ring = new RingBuffer<>(ringSize, waitStrategy);
                final boolean forceDurable = (brokerRole == BrokerRole.PERSISTENCE);

                final io.ringbroker.ledger.orchestrator.VirtualLog vLog =
                        new io.ringbroker.ledger.orchestrator.VirtualLog(partDir, (int) segmentCapacity);
                vLog.discoverOnDisk();
                final Ingress ingress = Ingress.create(
                        registry, ring, vLog, 0L, batchSize, forceDurable
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
                metadataStore,
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
        final LogMetadataStore metadataStore = new io.ringbroker.cluster.metadata.JournaledLogMetadataStore(baseDataDir.resolve("metadata"));
        return create(
                registry,
                partitioner,
                totalPartitions,
                myNodeId,
                clusterSize,
                clusterNodes,
                baseDataDir,
                ringSize,
                waitStrategy,
                segmentCapacity,
                batchSize,
                idempotentMode,
                offsetStore,
                brokerRole,
                replicaResolver,
                replicator,
                metadataStore
        );
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
                .setKey(key == null ? com.google.protobuf.ByteString.EMPTY : com.google.protobuf.UnsafeByteOperations.unsafeWrap(key))
                .setPayload(com.google.protobuf.UnsafeByteOperations.unsafeWrap(payload))
                .setPartitionId(partitionId);

        return BrokerApi.Envelope.newBuilder()
                .setCorrelationId(correlationId)
                .setPublish(msgBuilder.build())
                .build();
    }

    private void backfillTick() {
        for (final var entry : ingressMap.entrySet()) {
            final int pid = entry.getKey();
            final Ingress ing = entry.getValue();

            final LogConfiguration cfg = metadataStore.current(pid).orElse(null);
            if (cfg == null) continue;

            for (final io.ringbroker.cluster.metadata.EpochMetadata em : cfg.epochs()) {
                final long epoch = em.epoch();
                if (!em.isSealed()) continue;
                if (!em.placement().getStorageNodes().contains(myNodeId)) continue;
                if (ing.getVirtualLog().hasEpoch(epoch)) continue;

                // Try backfill from any other node in placement
                for (final int target : em.placement().getStorageNodesArray()) {
                    if (target == myNodeId) continue;
                    final RemoteBrokerClient client = clusterNodes.get(target);
                    if (client == null) continue;
                    try {
                        final BrokerApi.Envelope req = BrokerApi.Envelope.newBuilder()
                                .setBackfill(BrokerApi.BackfillRequest.newBuilder()
                                        .setPartitionId(pid)
                                        .setEpoch(epoch)
                                        .setOffset(0)
                                        .setMaxBytes(256 * 1024)
                                        .build())
                                .build();
                        final BrokerApi.BackfillReply reply = client.sendBackfill(req).get(5, TimeUnit.SECONDS);
                        if (!reply.getRedirectNodesList().isEmpty()) continue;
                        final byte[] payload = reply.getPayload().toByteArray();
                        if (payload.length == 0) continue;

                        // Payload is concatenated length-prefixed records
                        int pos = 0;
                        int count = 0;
                        final byte[][] batch = new byte[backfillBatchSize][];
                        while (pos + Integer.BYTES <= payload.length && count < backfillBatchSize) {
                            final int len = (payload[pos] & 0xFF) |
                                    ((payload[pos + 1] & 0xFF) << 8) |
                                    ((payload[pos + 2] & 0xFF) << 16) |
                                    ((payload[pos + 3] & 0xFF) << 24);
                            pos += Integer.BYTES;
                            if (pos + len > payload.length) break;
                            final byte[] rec = new byte[len];
                            System.arraycopy(payload, pos, rec, 0, len);
                            batch[count++] = rec;
                            pos += len;
                        }
                        if (count > 0) {
                            ing.appendBackfillBatch(epoch, batch, count);
                            backfillPlanner.markPresent(pid, epoch);
                        }
                        if (reply.getEndOfEpoch()) {
                            break;
                        }
                    } catch (final Exception ignored) {
                    }
                }
            }
        }
    }

    private void loadFenceState(final Path partitionDir, final PartitionEpochs pe) {
        try {
            Files.list(partitionDir)
                    .filter(p -> p.getFileName().toString().endsWith(".fence"))
                    .forEach(p -> {
                        final String name = p.getFileName().toString();
                        try {
                            final String epochStr = name.substring("epoch-".length(), name.indexOf(".fence"));
                            final long epoch = Long.parseLong(epochStr);
                            final FenceStore.PartitionFence fence = FenceStore.loadEpochFence(partitionDir, epoch);
                            if (fence != null) {
                                final PartitionEpochState pes = new PartitionEpochState();
                                pes.sealed.set(fence.sealed());
                                pes.sealedEndSeq = fence.sealedEndSeq();
                                pes.lastSeq.set(fence.lastSeq());
                                pe.epochFences.put(epoch, pes);
                                pe.highestSeenEpoch.accumulateAndGet(epoch, Math::max);
                            }
                        } catch (final Exception ignored) {
                        }
                    });
        } catch (final IOException ignored) {
        }
    }

    private void refreshEpochFromMetadata(final int partitionId) {
        final Optional<LogConfiguration> cfg = metadataStore.current(partitionId);
        if (cfg.isEmpty()) return;
        final EpochMetadata active = cfg.get().activeEpoch();
        final PartitionEpochs pe = partitionEpochs(partitionId);
        final long metaEpoch = active.epoch();
        EpochState st = pe.active;
        if (st == null || st.epochId < metaEpoch) {
            final Ingress ing = getOrCreateIngress(partitionId, metaEpoch);
            ing.setActiveEpoch(metaEpoch);
            final long last = ing.highWaterMark(metaEpoch);
            st = new EpochState(metaEpoch, last);
            pe.active = st;
            pe.highestSeenEpoch.accumulateAndGet(metaEpoch, Math::max);
            pe.lastTieBreaker.set(active.tieBreaker());
        }
        pe.activePlacement = active.placement();
    }

    private long computeTieBreaker(final int partitionId) {
        final Optional<LogConfiguration> cfg = metadataStore.current(partitionId);
        final long configVersion = cfg.map(LogConfiguration::configVersion).orElse(0L);
        // combine configVersion (monotonic) with node id for deterministic ordering
        return (configVersion + 1L) << 16 | (myNodeId & 0xFFFFL);
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

            return publishLocalWithRetry(correlationId, topic, key, retries, payload, partitionId);
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

        return forwardWithRetry(ownerClient, envelope, partitionId, 0);
    }

    public CompletableFuture<Void> publish(final String topic, final byte[] key, final byte[] payload) {
        final long defaultCorrelationId = (myRole == BrokerRole.INGESTION) ? System.nanoTime() : 0L;
        return publish(defaultCorrelationId, topic, key, 0, payload);
    }

    private CompletableFuture<Void> publishLocalWithRetry(final long correlationId,
                                                          final String topic,
                                                          final byte[] key,
                                                          final int retries,
                                                          final byte[] payload,
                                                          final int partitionId) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        attemptLocalAppend(future, correlationId, topic, key, retries, payload, partitionId, 0);
        return future;
    }

    private void attemptLocalAppend(final CompletableFuture<Void> target,
                                    final long correlationId,
                                    final String topic,
                                    final byte[] key,
                                    final int retries,
                                    final byte[] payload,
                                    final int partitionId,
                                    final int attempt) {
        replicationExecutor.submit(() -> {
            try {
                final PartitionEpochs pe = partitionEpochs(partitionId);
                final EpochState st = pe.active;
                final long seq = nextSeq(st);
                final long epoch = st.epochId;
                maybeTriggerRollover(partitionId, pe, st);

                final BrokerApi.Envelope appendEnv = BrokerApi.Envelope.newBuilder()
                        .setCorrelationId(correlationId)
                        .setAppend(BrokerApi.AppendRequest.newBuilder()
                                .setPartitionId(partitionId)
                                .setEpoch(epoch)
                                .setSeq(seq)
                                .setTopic(topic)
                                .setRetries(retries)
                                .setKey(key == null ? com.google.protobuf.ByteString.EMPTY : com.google.protobuf.UnsafeByteOperations.unsafeWrap(key))
                                .setPayload(com.google.protobuf.UnsafeByteOperations.unsafeWrap(payload))
                                .build())
                        .build();

                final EpochPlacement placementCache = pe.activePlacement;
                final int[] placementArr = placementCache != null
                        ? placementCache.getStorageNodesArray()
                        : ensureConfig(partitionId).activeEpoch().placement().getStorageNodesArray();
                final List<Integer> replicas = new ArrayList<>(placementArr.length);
                for (final int id : placementArr) {
                    if (id != myNodeId) replicas.add(id);
                }
                final int quorum = placementCache != null ? placementCache.getAckQuorum()
                        : ensureConfig(partitionId).activeEpoch().placement().getAckQuorum();

                final BrokerApi.ReplicationAck localAck =
                        appendAndWaitDurable(appendEnv.getAppend());

                if (localAck.getStatus() != BrokerApi.ReplicationAck.Status.SUCCESS) {
                    throw new RuntimeException("Local append failed: " + localAck.getStatus() + " " + localAck.getErrorMessage());
                }

                if (!replicas.isEmpty()) {
                    replicator.replicate(appendEnv, replicas, quorum);
                }

                target.complete(null);
            } catch (final Exception e) {
                if (attempt < APPEND_RETRY_LIMIT && isRetryable(e)) {
                    refreshEpochFromMetadata(partitionId);
                    attemptLocalAppend(target, correlationId, topic, key, retries, payload, partitionId, attempt + 1);
                    return;
                }
                target.completeExceptionally(e);
            }
        });
    }

    private boolean isRetryable(final Exception e) {
        final String msg = e.getMessage();
        if (msg == null) return false;
        return msg.contains("stale epoch") ||
                msg.contains("not ready") ||
                msg.contains("Gap");
    }

    private CompletableFuture<Void> forwardWithRetry(final RemoteBrokerClient client,
                                                     final BrokerApi.Envelope env,
                                                     final int partitionId,
                                                     final int attempt) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        client.sendEnvelopeWithAck(env).whenComplete((ack, err) -> {
            if (err != null) {
                if (attempt < APPEND_RETRY_LIMIT) {
                    refreshEpochFromMetadata(partitionId);
                    forwardWithRetry(client, env, partitionId, attempt + 1).whenComplete((v, e2) -> {
                        if (e2 != null) result.completeExceptionally(e2);
                        else result.complete(null);
                    });
                    return;
                }
                result.completeExceptionally(err);
                return;
            }
            if (ack.getStatus() != BrokerApi.ReplicationAck.Status.SUCCESS) {
                if (attempt < APPEND_RETRY_LIMIT && isRetryableStatus(ack)) {
                    refreshEpochFromMetadata(partitionId);
                    forwardWithRetry(client, env, partitionId, attempt + 1).whenComplete((v, e2) -> {
                        if (e2 != null) result.completeExceptionally(e2);
                        else result.complete(null);
                    });
                    return;
                }
                result.completeExceptionally(new RuntimeException("Forwarding failed: " + ack.getStatus()));
                return;
            }
            result.complete(null);
        });
        return result;
    }

    private boolean isRetryableStatus(final BrokerApi.ReplicationAck ack) {
        final BrokerApi.ReplicationAck.Status st = ack.getStatus();
        return st == BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY ||
                st == BrokerApi.ReplicationAck.Status.ERROR_INVALID_REQUEST;
    }

    public CompletableFuture<BrokerApi.ReplicationAck> handleSealAndRollAsync(final BrokerApi.SealRequest s) {
        final CompletableFuture<BrokerApi.ReplicationAck> f = new CompletableFuture<>();
        replicationExecutor.submit(() -> {
            try {
                final BrokerApi.ReplicationAck ack = handleSeal(s);
                f.complete(ack);
            } catch (final Throwable t) {
                f.complete(BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.ERROR_UNKNOWN)
                        .setErrorMessage(String.valueOf(t.getMessage()))
                        .build());
            }
        });
        return f;
    }

    public CompletableFuture<BrokerApi.BackfillReply> handleBackfillAsync(final BrokerApi.BackfillRequest req) {
        return CompletableFuture.supplyAsync(() -> handleBackfill(req), replicationExecutor);
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
            } catch (final Throwable t) {
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
            } catch (final Throwable t) {
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

    public CompletableFuture<BrokerApi.ReplicationAck> handleOpenEpochAsync(final BrokerApi.OpenEpochRequest req) {
        return CompletableFuture.completedFuture(handleOpenEpoch(req));
    }

    public BrokerApi.ReplicationAck handleMetadataUpdate(final BrokerApi.MetadataUpdate upd) {
        final LogConfiguration cfg = BroadcastingLogMetadataStore.fromProto(upd);
        metadataStore.applyRemote(cfg);
        refreshEpochFromMetadata(upd.getPartitionId());
        return BrokerApi.ReplicationAck.newBuilder()
                .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                .build();
    }

    private BrokerApi.ReplicationAck handleEpochStatus(final BrokerApi.EpochStatusRequest s) {
        final int pid = s.getPartitionId();
        final long epoch = s.getEpoch();

        final Ingress ing = ingressMap.get(pid);
        final long persisted = (ing != null && ing.getVirtualLog().hasEpoch(epoch))
                ? ing.getVirtualLog().forEpoch(epoch).getHighWaterMark()
                : -1L;

        return BrokerApi.ReplicationAck.newBuilder()
                .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                .setOffset(Lsn.encode(epoch, Math.max(-1L, persisted)))
                .build();
    }

    private BrokerApi.ReplicationAck handleSeal(final BrokerApi.SealRequest s) {
        final int pid = s.getPartitionId();
        final long epoch = s.getEpoch();
        final boolean sealOnly = s.getSealOnly();
        final long tieBreaker = sealOnly ? 0L : computeTieBreaker(pid);

        final PartitionEpochs pe = partitionEpochs(pid);
        if (epoch < pe.highestSeenEpoch.get()) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_INVALID_REQUEST)
                    .setErrorMessage("stale epoch " + epoch)
                    .build();
        }

        final EpochState st = pe.active;
        if (st == null || st.epochId != epoch) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY)
                    .setErrorMessage("epoch mismatch")
                    .build();
        }

        final Ingress ing = getOrCreateIngress(pid, epoch);
        final long persisted = ing.getVirtualLog().forEpoch(epoch).getHighWaterMark();

        st.sealed = true;
        st.sealedEndSeq = persisted;
        pe.highestSeenEpoch.accumulateAndGet(epoch, Math::max);
        final PartitionEpochState fence = pe.epochFences.computeIfAbsent(epoch, __ -> new PartitionEpochState());
        fence.sealed.set(true);
        fence.sealedEndSeq = persisted;
        FenceStore.storeEpochFence(baseDataDir.resolve("partition-" + pid), epoch, persisted, fence.lastSeq.get(), true);

        // If instructed, immediately prepare next epoch locally (useful for replicas after seal).
        if (!sealOnly) {
            final long nextEpoch = epoch + 1;
            pe.highestSeenEpoch.accumulateAndGet(nextEpoch, Math::max);
            pe.lastTieBreaker.set(tieBreaker);
            final Ingress ingNext = getOrCreateIngress(pid, nextEpoch);
            ingNext.setActiveEpoch(nextEpoch);
            final long nextLast = ingNext.highWaterMark(nextEpoch);
            pe.active = new EpochState(nextEpoch, nextLast);

            final List<Integer> placement = replicaResolver.replicas(pid);
            final EpochPlacement ep = new EpochPlacement(nextEpoch, placement, replicator.getAckQuorum());
            pe.activePlacement = ep;
            metadataStore.sealAndCreateEpoch(pid, epoch, persisted, ep, nextEpoch, tieBreaker);
            FenceStore.storeHighest(baseDataDir.resolve("partition-" + pid), nextEpoch);
        }

        return BrokerApi.ReplicationAck.newBuilder()
                .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                .setOffset(Lsn.encode(epoch, persisted))
                .build();
    }

    private BrokerApi.ReplicationAck handleOpenEpoch(final BrokerApi.OpenEpochRequest req) {
        final int pid = req.getPartitionId();
        final long epoch = req.getEpoch();
        final long tieBreaker = req.getTieBreaker();

        final PartitionEpochs pe = partitionEpochs(pid);
        final long currentHighest = pe.highestSeenEpoch.get();
        final long currentTie = pe.lastTieBreaker.get();
        if (epoch < currentHighest) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY)
                    .setErrorMessage("higher epoch already opened: " + currentHighest)
                    .build();
        }

        // Tie-break if same epoch opened twice
        if (epoch == currentHighest) {
            // deterministic: larger tieBreaker wins
            if (tieBreaker <= currentTie) {
                return BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY)
                        .setErrorMessage("epoch already opened with equal/greater tieBreaker")
                        .build();
            }
        }

        // Open epoch on storage
        final Ingress ing = getOrCreateIngress(pid, epoch);
        ing.setActiveEpoch(epoch);
        final long last = ing.highWaterMark(epoch);
        pe.active = new EpochState(epoch, last);
        pe.highestSeenEpoch.set(epoch);
        pe.lastTieBreaker.set(tieBreaker);
        FenceStore.storeHighest(baseDataDir.resolve("partition-" + pid), epoch);

        final List<Integer> placement = replicaResolver.replicas(pid);
        final EpochPlacement ep = new EpochPlacement(epoch, placement, replicator.getAckQuorum());
        metadataStore.bootstrapIfAbsent(pid, ep, Math.max(0L, last + 1));

        return BrokerApi.ReplicationAck.newBuilder()
                .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                .setOffset(Lsn.encode(epoch, last))
                .build();
    }

    private BrokerApi.BackfillReply handleBackfill(final BrokerApi.BackfillRequest req) {
        final int pid = req.getPartitionId();
        final long epoch = req.getEpoch();
        final long offset = req.getOffset();
        final int maxBytes = Math.max(1, req.getMaxBytes());

        final BrokerApi.BackfillReply.Builder reply = BrokerApi.BackfillReply.newBuilder();

        final Ingress ing = ingressMap.get(pid);
        final Optional<List<Integer>> placement = placementForEpoch(pid, epoch);
        if (ing == null || !ing.getVirtualLog().hasEpoch(epoch)) {
            reply.addAllRedirectNodes(placement.orElseGet(Collections::emptyList));
            return reply.build();
        }

        final int[] written = new int[]{0};
        final byte[][] scratch = new byte[backfillBatchSize][];
        final int[] count = new int[]{0};

        ing.fetchEpoch(epoch, offset, backfillBatchSize, (off, segBuf, payloadPos, payloadLen) -> {
            if (written[0] + payloadLen > maxBytes) return;
            final byte[] buf = new byte[payloadLen + Integer.BYTES];
            // length prefix (little endian)
            buf[0] = (byte) (payloadLen);
            buf[1] = (byte) (payloadLen >>> 8);
            buf[2] = (byte) (payloadLen >>> 16);
            buf[3] = (byte) (payloadLen >>> 24);
            segBuf.position(payloadPos).get(buf, Integer.BYTES, payloadLen);
            scratch[count[0]++] = buf;
            written[0] += buf.length;
        });

        if (count[0] == 0) {
            reply.addAllRedirectNodes(placement.orElseGet(Collections::emptyList));
            return reply.build();
        }

        // Concatenate payloads to a single ByteString to avoid many small copies on the wire.
        int total = 0;
        for (int i = 0; i < count[0]; i++) total += scratch[i].length;
        final byte[] out = new byte[total];
        int pos = 0;
        for (int i = 0; i < count[0]; i++) {
            final byte[] src = scratch[i];
            System.arraycopy(src, 0, out, pos, src.length);
            pos += src.length;
        }

        final long hwm = ing.getVirtualLog().forEpoch(epoch).getHighWaterMark();
        reply.setPayload(com.google.protobuf.ByteString.copyFrom(out));
        reply.setEndOfEpoch(offset + count[0] > hwm);

        return reply.build();
    }

    private BrokerApi.ReplicationAck appendAndWaitDurable(final BrokerApi.AppendRequest a) {
        final int pid = a.getPartitionId();
        final long epoch = a.getEpoch();
        final long seq = a.getSeq();

        final String topic = a.getTopic();
        if (!registry.contains(topic)) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_INVALID_REQUEST)
                    .setErrorMessage("Unknown topic: " + topic)
                    .build();
        }

        final PartitionEpochs pe = partitionEpochs(pid);
        final long highest = pe.highestSeenEpoch.get();
        if (epoch < highest) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_INVALID_REQUEST)
                    .setErrorMessage("stale epoch " + epoch + " highest=" + highest)
                    .build();
        }
        final EpochState st = ensureEpochState(pid, epoch);
        final PartitionEpochState fence = pe.epochFences.computeIfAbsent(epoch, __ -> new PartitionEpochState());

        if (st.sealed && seq > st.sealedEndSeq) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY)
                    .setErrorMessage("Epoch sealed at " + st.sealedEndSeq)
                    .build();
        }
        if (fence.sealed.get() && seq > fence.sealedEndSeq) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY)
                    .setErrorMessage("Epoch sealed at " + fence.sealedEndSeq)
                    .build();
        }

        final Ingress ing = getOrCreateIngress(pid, epoch);

        while (true) {
            final long cur = st.lastSeqReserved.get();

            if (seq <= cur) {
                if (!awaitPersisted(ing, epoch, seq, APPEND_WAIT_TIMEOUT_MS)) {
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
            fence.lastSeq.set(seq);

            try {
                ing.publish(topic, a.getRetries(), a.getPayload().toByteArray());
            } catch (final Throwable t) {
                st.lastSeqReserved.compareAndSet(seq, cur);
                return BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.ERROR_PERSISTENCE_FAILED)
                        .setErrorMessage(String.valueOf(t.getMessage()))
                        .build();
            }

            if (!awaitPersisted(ing, epoch, seq, APPEND_WAIT_TIMEOUT_MS)) {
                return BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.ERROR_PERSISTENCE_FAILED)
                        .setErrorMessage("timeout waiting for durable seq=" + seq)
                        .build();
            }
            fence.lastSeq.set(seq);

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

        final PartitionEpochs pe = partitionEpochs(pid);
        if (epoch < pe.highestSeenEpoch.get()) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_INVALID_REQUEST)
                    .setErrorMessage("stale epoch " + epoch)
                    .build();
        }
        final EpochState st = ensureEpochState(pid, epoch);
        final Ingress ing = getOrCreateIngress(pid, epoch);
        final PartitionEpochState fence = pe.epochFences.computeIfAbsent(epoch, __ -> new PartitionEpochState());
        final long lastSeqInBatch = baseSeq + n - 1L;
        if ((st.sealed && lastSeqInBatch > st.sealedEndSeq) ||
                (fence.sealed.get() && lastSeqInBatch > fence.sealedEndSeq)) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY)
                    .setErrorMessage("Epoch sealed at " + Math.max(st.sealedEndSeq, fence.sealedEndSeq))
                    .build();
        }

        while (true) {
            final long cur = st.lastSeqReserved.get();

            if (baseSeq <= cur) {
                final long last = Math.max(cur, baseSeq + n - 1L);
                if (!awaitPersisted(ing, epoch, last, APPEND_WAIT_TIMEOUT_MS)) {
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
            fence.lastSeq.set(last);

            try {
                for (int i = 0; i < n; i++) {
                    ing.publish(topic, b.getRetries(), payloads.get(i).toByteArray());
                }
            } catch (final Throwable t) {
                st.lastSeqReserved.compareAndSet(last, cur);
                return BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.ERROR_PERSISTENCE_FAILED)
                        .setErrorMessage(String.valueOf(t.getMessage()))
                        .build();
            }

            if (!awaitPersisted(ing, epoch, last, APPEND_WAIT_TIMEOUT_MS)) {
                return BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(BrokerApi.ReplicationAck.Status.ERROR_PERSISTENCE_FAILED)
                        .setErrorMessage("timeout waiting for durable seq=" + last)
                        .build();
            }
            fence.lastSeq.set(last);

            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                    .setOffset(Lsn.encode(epoch, last))
                    .build();
        }
    }

    private boolean awaitPersisted(final Ingress ing, final long epoch, final long targetSeq, final long timeoutMs) {
        if (targetSeq < 0) return true;

        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        long spins = 0;

        while (System.nanoTime() < deadline) {
            if (ing.highWaterMark(epoch) >= targetSeq) {
                return true;
            }
            if (spins < 1_000) {
                spins++;
                Thread.onSpinWait();
            } else {
                LockSupport.parkNanos(PARK_NANOS);
            }
        }
        return ing.highWaterMark(epoch) >= targetSeq;
    }

    private Ingress getOrCreateIngress(final int partitionId, final long epoch) {
        final Ingress existing = ingressMap.get(partitionId);
        if (existing != null && existing.getActiveEpoch() == epoch) {
            return existing;
        }

        final Ingress created = ingressMap.compute(partitionId, (pid, current) -> {
            if (current != null) {
                current.setActiveEpoch(epoch);
                return current;
            }
            try {
                final Path partDir = baseDataDir.resolve("partition-" + pid);
                Files.createDirectories(partDir);

                final RingBuffer<byte[]> ring = new RingBuffer<>(ringSize, waitStrategy);
                final boolean forceDurable = (myRole == BrokerRole.PERSISTENCE);

                final io.ringbroker.ledger.orchestrator.VirtualLog vLog =
                        new io.ringbroker.ledger.orchestrator.VirtualLog(partDir, (int) segmentCapacity);

                final Ingress ingress = Ingress.create(
                        registry, ring, vLog, epoch, batchSize, forceDurable
                );

                deliveryMap.putIfAbsent(pid, new Delivery(ring));
                if (idempotentMode) {
                    seenMessageIds.computeIfAbsent(pid, __ -> ConcurrentHashMap.newKeySet());
                }

                final long last = ingress.getVirtualLog().forEpoch(epoch).getHighWaterMark();
                PartitionEpochs pe = epochsByPartition.get(pid);
                if (pe == null) {
                    pe = new PartitionEpochs();
                    epochsByPartition.put(pid, pe);
                }
                pe.active = new EpochState(epoch, last);
                pe.highestSeenEpoch.accumulateAndGet(epoch, Math::max);

                final List<Integer> placementNodes = replicaResolver.replicas(pid);
                final EpochPlacement placement = new EpochPlacement(epoch, placementNodes, replicator.getAckQuorum());
                metadataStore.bootstrapIfAbsent(pid, placement, Math.max(0, last + 1));

                return ingress;
            } catch (final Exception e) {
                throw new RuntimeException("Failed to create ingress for partition " + pid + " epoch " + epoch, e);
            }
        });

        return created;
    }

    private PartitionEpochs partitionEpochs(final int partitionId) {
        return epochsByPartition.computeIfAbsent(partitionId, __ -> new PartitionEpochs());
    }

    private long nextSeq(final EpochState st) {
        return st.lastSeqReserved.incrementAndGet();
    }

    private LogConfiguration ensureConfig(final int partitionId) {
        final Optional<LogConfiguration> existing = metadataStore.current(partitionId);
        if (existing.isPresent()) return existing.get();

        final Ingress ing = getOrCreateIngress(partitionId, 0L);
        final long startSeq = Math.max(0L, ing.highWaterMark(0L) + 1);
        final List<Integer> placement = replicaResolver.replicas(partitionId);
        final EpochPlacement ep = new EpochPlacement(0L, placement, replicator.getAckQuorum());
        return metadataStore.bootstrapIfAbsent(partitionId, ep, startSeq);
    }

    public Optional<List<Integer>> placementForEpoch(final int partitionId, final long epoch) {
        final Optional<LogConfiguration> cfg = metadataStore.current(partitionId);
        if (cfg.isEmpty()) return Optional.empty();
        final io.ringbroker.cluster.metadata.EpochMetadata meta = cfg.get().epoch(epoch);
        if (meta == null) return Optional.empty();
        return Optional.of(meta.placement().getStorageNodes());
    }

    private EpochState ensureEpochState(final int partitionId, final long epoch) {
        final PartitionEpochs pe = partitionEpochs(partitionId);
        EpochState st = pe.active;

        if (st == null || st.epochId < epoch) {
            final Ingress ing = getOrCreateIngress(partitionId, epoch);
            ing.setActiveEpoch(epoch);
            final long last = ing.highWaterMark(epoch);
            st = new EpochState(epoch, last);
            pe.active = st;
            pe.highestSeenEpoch.accumulateAndGet(epoch, Math::max);

            final List<Integer> placement = replicaResolver.replicas(partitionId);
            final EpochPlacement ep = new EpochPlacement(epoch, placement, replicator.getAckQuorum());
            metadataStore.bootstrapIfAbsent(partitionId, ep, Math.max(0L, last + 1));
        }
        return st;
    }

    /**
     * Auto-rolls to a new epoch when the current seq approaches the packing limit.
     */
    private void maybeTriggerRollover(final int partitionId, final PartitionEpochs pe, final EpochState st) {
        // Fast-path: check without lock
        if (st.sealed) return;
        final long cur = st.lastSeqReserved.get();
        if (cur < SEQ_ROLLOVER_THRESHOLD) return;

        if (!pe.rolling.compareAndSet(false, true)) return;
        try {
            final Ingress ing = getOrCreateIngress(partitionId, st.epochId);
            final long sealedEnd = Math.max(cur, ing.getVirtualLog().forEpoch(st.epochId).getHighWaterMark());

            st.sealed = true;
            st.sealedEndSeq = sealedEnd;
            pe.highestSeenEpoch.accumulateAndGet(st.epochId, Math::max);
            FenceStore.storeHighest(baseDataDir.resolve("partition-" + partitionId), st.epochId);

            final LogConfiguration cfg = ensureConfig(partitionId);
            final int[] placementArr = cfg.activeEpoch().placement().getStorageNodesArray();
            final List<Integer> replicas = new ArrayList<>(placementArr.length);
            for (final int id : placementArr) {
                if (id != myNodeId) replicas.add(id);
            }
            final int quorum = cfg.activeEpoch().placement().getAckQuorum();

            final BrokerApi.Envelope sealEnv = BrokerApi.Envelope.newBuilder()
                    .setCorrelationId(System.nanoTime())
                    .setSeal(BrokerApi.SealRequest.newBuilder()
                            .setPartitionId(partitionId)
                            .setEpoch(st.epochId)
                            .setSealOnly(false) // instruct replicas to roll forward too
                            .build())
                    .build();

            if (!replicas.isEmpty()) {
                replicator.replicate(sealEnv, replicas, quorum);
            }

            final long newEpochId = st.epochId + 1;
            final long nextTieBreaker = computeTieBreaker(partitionId);
            // Open next epoch on replicas before appending
            final BrokerApi.Envelope openEnv = BrokerApi.Envelope.newBuilder()
                    .setCorrelationId(System.nanoTime())
                    .setOpenEpoch(BrokerApi.OpenEpochRequest.newBuilder()
                            .setPartitionId(partitionId)
                            .setEpoch(newEpochId)
                            .setTieBreaker(nextTieBreaker)
                            .build())
                    .build();
            if (!replicas.isEmpty()) {
                replicator.replicate(openEnv, replicas, quorum);
            }

            final List<Integer> newPlacement = replicaResolver.replicas(partitionId);
            final EpochPlacement ep = new EpochPlacement(newEpochId, newPlacement, replicator.getAckQuorum());
            metadataStore.sealAndCreateEpoch(partitionId, st.epochId, sealedEnd, ep, newEpochId, nextTieBreaker);

            final EpochState next = new EpochState(newEpochId, sealedEnd);
            pe.active = next;
            pe.highestSeenEpoch.accumulateAndGet(newEpochId, Math::max);
            pe.lastTieBreaker.set(nextTieBreaker);
            pe.activePlacement = ep;
            FenceStore.storeHighest(baseDataDir.resolve("partition-" + partitionId), newEpochId);
            ing.setActiveEpoch(newEpochId);
        } catch (final Exception e) {
            log.warn("Rollover failed for partition {} epoch {}: {}", partitionId, st.epochId, e.toString());
        } finally {
            pe.rolling.set(false);
        }
    }

    /**
     * Per-epoch state tracking highest reserved seq and seal info.
     */
    private static final class EpochState {
        final long epochId;
        final AtomicLong lastSeqReserved;
        volatile boolean sealed;
        volatile long sealedEndSeq;

        EpochState(final long epochId, final long lastSeqInit) {
            this.epochId = epochId;
            this.lastSeqReserved = new AtomicLong(lastSeqInit);
            this.sealed = false;
            this.sealedEndSeq = -1L;
        }
    }

    private static final class PartitionEpochs {
        // highest fenced epoch to reject stale writers
        final AtomicLong highestSeenEpoch = new AtomicLong(0L);
        final AtomicLong lastTieBreaker = new AtomicLong(0L);
        final AtomicBoolean rolling = new AtomicBoolean(false);
        final ConcurrentMap<Long, PartitionEpochState> epochFences = new ConcurrentHashMap<>();
        // active epoch state
        volatile EpochState active;
        volatile EpochPlacement activePlacement;
    }

    public void shutdown() throws IOException {
        if (!closed.compareAndSet(false, true)) return;

        // Stop periodic backfill first
        backfillExecutor.shutdownNow();

        // Stop background replication pool
        try {
            replicator.shutdown();
        } catch (final Exception ignored) {
        }

        // Close all netty clients so event loop threads don't keep JVM alive
        for (final RemoteBrokerClient c : clusterNodes.values()) {
            try {
                c.close();
            } catch (final Exception ignored) {
            }
        }
        clusterNodes.clear();

        // Close ingresses (stops writer tasks + closes logs)
        for (final Ingress ingress : ingressMap.values()) {
            try {
                ingress.close();
            } catch (final Exception ignored) {
            }
        }

        // Stop replication executor last
        replicationExecutor.shutdownNow();
        try {
            replicationExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (final InterruptedException ie) {
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
