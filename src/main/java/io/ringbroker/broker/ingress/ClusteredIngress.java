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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

@Slf4j
@Getter
public final class ClusteredIngress {

    private static final CompletableFuture<Void> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);

    private static final long APPEND_WAIT_TIMEOUT_MS = 10_000L;
    private static final long PARK_NANOS = 1_000L;

    private static final long SEQ_ROLLOVER_THRESHOLD = (1L << 40) - 1_000_000L;
    private static final long BACKFILL_INTERVAL_MS = 5_000L;

    // batching in leader pipeline
    private static final int PIPELINE_MAX_DRAIN = 8_192;
    private static final int PIPELINE_QUEUE_FACTOR = 8;

    private final BackfillPlanner backfillPlanner;
    private final int backfillBatchSize = 64;

    private final ScheduledExecutorService backfillExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                final Thread t = new Thread(r, "backfill-worker");
                t.setDaemon(true);
                return t;
            });

    private final ExecutorService adminExecutor =
            Executors.newSingleThreadExecutor(r -> {
                final Thread t = new Thread(r, "cluster-admin");
                t.setDaemon(true);
                return t;
            });

    private final ConcurrentMap<Integer, PartitionEpochs> epochsByPartition = new ConcurrentHashMap<>();
    private final LogMetadataStore metadataStore;

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

    private final AtomicBoolean closed = new AtomicBoolean(false);

    // per-partition serialized pipeline (major hot-path win)
    private final ConcurrentMap<Integer, PartitionPipeline> pipelines = new ConcurrentHashMap<>();

    private ClusteredIngress(final Partitioner partitioner,
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

        this.partitioner = partitioner;
        this.totalPartitions = totalPartitions;
        this.myNodeId = myNodeId;
        this.clusterSize = clusterSize;
        this.ingressMap = ingressMap;

        // Keep a live view of the cluster map when a concurrent map is provided (benchmark wires clients after ctor).
        if (clusterNodes instanceof ConcurrentMap) {
            @SuppressWarnings("unchecked")
            final ConcurrentMap<Integer, RemoteBrokerClient> live = (ConcurrentMap<Integer, RemoteBrokerClient>) clusterNodes;
            this.clusterNodes = live;
        } else {
            this.clusterNodes = new ConcurrentHashMap<>(clusterNodes);
        }
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

        // init partition fencing + bootstrap metadata for local partitions
        for (final var e : ingressMap.entrySet()) {
            final int pid = e.getKey();
            final Ingress ing = e.getValue();
            final Path partDir = baseDataDir.resolve("partition-" + pid);

            // load fences first (disk truth)
            final PartitionEpochs pe = new PartitionEpochs();
            pe.highestSeenEpoch.set(FenceStore.loadHighest(partDir));
            loadFenceState(partDir, pe);

            // use existing metadata (if present) instead of forcing epoch=0 view
            final LogConfiguration cfg;
            final Optional<LogConfiguration> cur = metadataStore.current(pid);
            if (cur.isPresent()) {
                cfg = cur.get();
            } else {
                // bootstrap epoch 0 based on actual persisted HWM
                final long last0 = ing.getVirtualLog().forEpoch(0L).getHighWaterMark();
                final List<Integer> placementNodes = replicaResolver.replicas(pid);
                final EpochPlacement placement0 = new EpochPlacement(0L, placementNodes, replicator.getAckQuorum());
                cfg = metadataStore.bootstrapIfAbsent(pid, placement0, Math.max(0L, last0 + 1));
            }

            final EpochMetadata active = cfg.activeEpoch();
            final long activeEpoch = active.epoch();

            ing.setActiveEpoch(activeEpoch);
            final long last = ing.highWaterMark(activeEpoch);

            pe.active = new EpochState(activeEpoch, last);
            pe.activePlacement = active.placement();
            pe.lastTieBreaker.set(active.tieBreaker());

            // ensure highestSeenEpoch never goes backwards
            pe.highestSeenEpoch.accumulateAndGet(activeEpoch, Math::max);

            epochsByPartition.put(pid, pe);

            // start pipeline for local partition owner
            pipeline(pid);
        }

        backfillExecutor.scheduleAtFixedRate(this::backfillTick, 5_000L, BACKFILL_INTERVAL_MS, TimeUnit.MILLISECONDS);
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

                final Ingress ingress = Ingress.create(registry, ring, vLog, 0L, batchSize, forceDurable);
                ingressMap.put(pid, ingress);
                deliveryMap.put(pid, new Delivery(ring));

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
        final LogMetadataStore metadataStore = new JournaledLogMetadataStore(baseDataDir.resolve("metadata"));
        return create(registry, partitioner, totalPartitions, myNodeId, clusterSize, clusterNodes, baseDataDir,
                ringSize, waitStrategy, segmentCapacity, batchSize, idempotentMode, offsetStore,
                brokerRole, replicaResolver, replicator, metadataStore);
    }

    // ---------- Public API ----------

    public CompletableFuture<Void> publish(final long correlationId,
                                           final String topic,
                                           final byte[] key,
                                           final int retries,
                                           final byte[] payload) {

        final int partitionId = partitioner.selectPartition(key, totalPartitions);
        final int ownerNode = Math.floorMod(partitionId, clusterSize);

        if (ownerNode == myNodeId) {
            if (idempotentMode && shouldDropDuplicate(partitionId, key, payload)) return COMPLETED_FUTURE;
            return pipeline(partitionId).submitPublish(correlationId, topic, retries, payload);
        }

        // forward
        final RemoteBrokerClient ownerClient = clusterNodes.get(ownerNode);
        if (ownerClient == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("No client for owner " + ownerNode));
        }

        final BrokerApi.Envelope env = buildPublishEnvelope(correlationId, topic, key, payload, partitionId, retries);
        return forwardWithRetry(ownerClient, env, partitionId, 0);
    }

    public CompletableFuture<Void> publish(final String topic, final byte[] key, final byte[] payload) {
        final long defaultCorrelationId = (myRole == BrokerRole.INGESTION) ? System.nanoTime() : 0L;
        return publish(defaultCorrelationId, topic, key, 0, payload);
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

    // ---------- Replica handlers (serialized through pipeline) ----------

    public CompletableFuture<BrokerApi.ReplicationAck> handleAppendAsync(final BrokerApi.AppendRequest a) {
        return pipeline(a.getPartitionId()).submitReplicaAppend(a);
    }

    public CompletableFuture<BrokerApi.ReplicationAck> handleAppendBatchAsync(final BrokerApi.AppendBatchRequest b) {
        return pipeline(b.getPartitionId()).submitReplicaAppendBatch(b);
    }

    public CompletableFuture<BrokerApi.ReplicationAck> handleSealAsync(final BrokerApi.SealRequest s) {
        return pipeline(s.getPartitionId()).submitSeal(s);
    }

    public CompletableFuture<BrokerApi.ReplicationAck> handleOpenEpochAsync(final BrokerApi.OpenEpochRequest req) {
        return pipeline(req.getPartitionId()).submitOpenEpoch(req);
    }

    public CompletableFuture<BrokerApi.ReplicationAck> handleEpochStatusAsync(final BrokerApi.EpochStatusRequest s) {
        return CompletableFuture.completedFuture(handleEpochStatus(s));
    }

    public CompletableFuture<BrokerApi.BackfillReply> handleBackfillAsync(final BrokerApi.BackfillRequest req) {
        return CompletableFuture.supplyAsync(() -> handleBackfill(req), adminExecutor);
    }

    public CompletableFuture<BrokerApi.ReplicationAck> handleSealAndRollAsync(final BrokerApi.SealRequest s) {
        // IMPORTANT: keep this serialized as well (can mutate epoch state)
        return pipeline(s.getPartitionId()).submitSeal(s);
    }

    /**
     * Serialize metadata updates through the per-partition pipeline to avoid races with publish/replica appends.
     */
    public BrokerApi.ReplicationAck handleMetadataUpdate(final BrokerApi.MetadataUpdate upd) {
        try {
            return pipeline(upd.getPartitionId()).submitMetadataUpdate(upd).get(5, TimeUnit.SECONDS);
        } catch (final Exception e) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_INVALID_REQUEST)
                    .setErrorMessage("metadata update failed: " + e)
                    .build();
        }
    }

    // ---------- Pipeline core ----------

    private PartitionPipeline pipeline(final int pid) {
        return pipelines.computeIfAbsent(pid, __ -> {
            final int cap = nextPow2(Math.max(1 << 16, batchSize * PIPELINE_QUEUE_FACTOR));
            final PartitionPipeline p = new PartitionPipeline(pid, cap);
            p.start();
            return p;
        });
    }

    private static int nextPow2(final int v) {
        final int x = Math.max(2, v);
        final int hi = Integer.highestOneBit(x);
        return (x == hi) ? x : hi << 1;
    }

    private final class PartitionPipeline implements Runnable {
        private static final int OFFER_SPIN_LIMIT = 256;
        private static final long OFFER_PARK_NANOS = 1_000L; // 1µs backoff when full

        private final int pid;
        private final MpscQueue queue;
        private final Thread thread;

        // one-item defer slot for the (single) consumer thread (used by batching)
        private Object deferred;

        // batch scratch (reused) — never allow 0-length
        private final int maxDrain = Math.max(1, Math.min(PIPELINE_MAX_DRAIN, Math.max(1, batchSize)));
        private final byte[][] payloads = new byte[maxDrain][];
        @SuppressWarnings("unchecked")
        private final CompletableFuture<Void>[] publishFuts =
                (CompletableFuture<Void>[]) new CompletableFuture<?>[maxDrain];

        // replication targets scratch (avoid per-publish allocation)
        private int[] replicaScratch = new int[Math.max(1, clusterSize)];

        PartitionPipeline(final int pid, final int capacityPow2) {
            this.pid = pid;
            this.queue = new MpscQueue(capacityPow2);
            this.thread = Thread.ofVirtual().name("partition-pipeline-" + pid).unstarted(this);
        }

        void start() { thread.start(); }

        void stop() { thread.interrupt(); }

        private Object pollOne() {
            final Object d = deferred;
            if (d != null) {
                deferred = null;
                return d;
            }
            return queue.poll();
        }

        private void deferOne(final Object o) {
            if (o == null) return;
            if (deferred == null) {
                deferred = o;
                return;
            }
            // extremely rare: if we already deferred one, put into main queue
            int spins = 0;
            while (!queue.offer(o)) {
                if (closed.get() || Thread.currentThread().isInterrupted()) return;
                if (spins++ < OFFER_SPIN_LIMIT) Thread.onSpinWait();
                else { spins = 0; LockSupport.parkNanos(OFFER_PARK_NANOS); }
            }
        }

        private boolean enqueueOrFail(final Object task, final CompletableFuture<?> f) {
            int spins = 0;
            while (!queue.offer(task)) {
                if (closed.get() || Thread.currentThread().isInterrupted()) {
                    f.completeExceptionally(new IllegalStateException("Broker is closed"));
                    return false;
                }
                if (spins++ < OFFER_SPIN_LIMIT) {
                    Thread.onSpinWait();
                } else {
                    spins = 0;
                    LockSupport.parkNanos(OFFER_PARK_NANOS);
                }
            }
            return true;
        }

        CompletableFuture<Void> submitPublish(final long correlationId,
                                              final String topic,
                                              final int retries,
                                              final byte[] payload) {
            final CompletableFuture<Void> f = new CompletableFuture<>();
            final PublishTask t = new PublishTask(correlationId, topic, retries, payload, f);
            enqueueOrFail(t, f);
            return f;
        }

        CompletableFuture<BrokerApi.ReplicationAck> submitReplicaAppend(final BrokerApi.AppendRequest a) {
            final CompletableFuture<BrokerApi.ReplicationAck> f = new CompletableFuture<>();
            final ReplicaAppendTask t = new ReplicaAppendTask(a, f);
            enqueueOrFail(t, f);
            return f;
        }

        CompletableFuture<BrokerApi.ReplicationAck> submitReplicaAppendBatch(final BrokerApi.AppendBatchRequest b) {
            final CompletableFuture<BrokerApi.ReplicationAck> f = new CompletableFuture<>();
            final ReplicaAppendBatchTask t = new ReplicaAppendBatchTask(b, f);
            enqueueOrFail(t, f);
            return f;
        }

        CompletableFuture<BrokerApi.ReplicationAck> submitSeal(final BrokerApi.SealRequest s) {
            final CompletableFuture<BrokerApi.ReplicationAck> f = new CompletableFuture<>();
            final SealTask t = new SealTask(s, f);
            enqueueOrFail(t, f);
            return f;
        }

        CompletableFuture<BrokerApi.ReplicationAck> submitOpenEpoch(final BrokerApi.OpenEpochRequest r) {
            final CompletableFuture<BrokerApi.ReplicationAck> f = new CompletableFuture<>();
            final OpenEpochTask t = new OpenEpochTask(r, f);
            enqueueOrFail(t, f);
            return f;
        }

        CompletableFuture<BrokerApi.ReplicationAck> submitMetadataUpdate(final BrokerApi.MetadataUpdate u) {
            final CompletableFuture<BrokerApi.ReplicationAck> f = new CompletableFuture<>();
            final MetadataUpdateTask t = new MetadataUpdateTask(u, f);
            enqueueOrFail(t, f);
            return f;
        }

        @Override
        public void run() {
            try {
                while (!closed.get() && !Thread.currentThread().isInterrupted()) {
                    final Object obj = pollOne();
                    if (obj == null) {
                        LockSupport.parkNanos(PARK_NANOS);
                        continue;
                    }

                    // IMPORTANT: never let one bad message kill the whole pipeline
                    try {
                        if (obj instanceof PublishTask first) {
                            drainAndProcessPublish(first);
                        } else if (obj instanceof ReplicaAppendTask ra) {
                            ra.future.complete(appendReplicaFast(ra.req));
                        } else if (obj instanceof ReplicaAppendBatchTask rb) {
                            rb.future.complete(appendReplicaBatchFast(rb.req));
                        } else if (obj instanceof SealTask st) {
                            st.future.complete(handleSeal(st.req));
                        } else if (obj instanceof OpenEpochTask oe) {
                            oe.future.complete(handleOpenEpoch(oe.req));
                        } else if (obj instanceof MetadataUpdateTask mu) {
                            mu.future.complete(applyMetadataUpdate(mu.req));
                        } else {
                            // ignore unknown
                        }
                    } catch (final Throwable taskErr) {
                        completeTaskExceptionally(obj, taskErr);
                        log.error("Partition pipeline {} task failed (continuing)", pid, taskErr);
                    }
                }
            } finally {
                // On shutdown/crash: fail everything still queued so callers don’t hang forever
                final Throwable stop = new IllegalStateException("Partition pipeline stopped: " + pid);

                final Object d = deferred;
                if (d != null) completeTaskExceptionally(d, stop);
                deferred = null;

                Object obj;
                while ((obj = queue.poll()) != null) {
                    completeTaskExceptionally(obj, stop);
                }
            }
        }

        private void completeTaskExceptionally(final Object obj, final Throwable t) {
            if (obj instanceof PublishTask pt) {
                pt.future.completeExceptionally(t);
            } else if (obj instanceof ReplicaAppendTask ra) {
                ra.future.completeExceptionally(t);
            } else if (obj instanceof ReplicaAppendBatchTask rb) {
                rb.future.completeExceptionally(t);
            } else if (obj instanceof SealTask st) {
                st.future.completeExceptionally(t);
            } else if (obj instanceof OpenEpochTask oe) {
                oe.future.completeExceptionally(t);
            } else if (obj instanceof MetadataUpdateTask mu) {
                mu.future.completeExceptionally(t);
            }
        }

        private BrokerApi.ReplicationAck applyMetadataUpdate(final BrokerApi.MetadataUpdate upd) {
            final LogConfiguration cfg = BroadcastingLogMetadataStore.fromProto(upd);
            metadataStore.applyRemote(cfg);
            refreshEpochFromMetadata(upd.getPartitionId());
            return BrokerApi.ReplicationAck.newBuilder().setStatus(BrokerApi.ReplicationAck.Status.SUCCESS).build();
        }

        private void drainAndProcessPublish(final PublishTask first) {
            // Validate once
            if (!registry.contains(first.topic)) {
                first.future.completeExceptionally(new IllegalArgumentException("Unknown topic: " + first.topic));
                return;
            }

            // group by (topic,retries) contiguous
            int count = 0;
            final String topic = first.topic;
            final int retries = first.retries;

            payloads[count] = Objects.requireNonNull(first.payload, "payload");
            publishFuts[count] = first.future;
            count++;

            while (count < payloads.length) {
                final Object o = queue.poll(); // IMPORTANT: do not consume deferred here
                if (o == null) break;

                if (!(o instanceof PublishTask p)) {
                    deferOne(o);
                    break;
                }
                if (!Objects.equals(topic, p.topic) || retries != p.retries) {
                    deferOne(p);
                    break;
                }

                payloads[count] = Objects.requireNonNull(p.payload, "payload");
                publishFuts[count] = p.future;
                count++;
            }

            try {
                refreshEpochFromMetadata(pid);
                final PartitionEpochs pe = partitionEpochs(pid);
                EpochState st = pe.active;
                if (st == null) throw new IllegalStateException("No active epoch state");

                // rollover check is based on projected reservation
                final long cur = st.lastSeqReserved.get();
                if (!st.sealed && (cur + count) >= SEQ_ROLLOVER_THRESHOLD) {
                    maybeTriggerRollover(pid, pe, st, cur + count);

                    // After rollover, we MUST re-load active epoch/state before reserving.
                    refreshEpochFromMetadata(pid);
                    st = pe.active;
                    if (st == null) throw new IllegalStateException("No active epoch state after rollover");
                }

                if (st.sealed) {
                    throw new IllegalStateException("Cannot publish: epoch sealed (pid=" + pid + ", epoch=" + st.epochId + ")");
                }

                final long epoch = st.epochId;

                // reserve range atomically (pipeline is serialized, but this avoids torn logic and is safer)
                final long prev = st.lastSeqReserved.getAndAdd(count);
                final long baseSeq = prev + 1;
                final long lastSeq = prev + count;

                final PartitionEpochState fence = pe.epochFences.computeIfAbsent(epoch, __ -> new PartitionEpochState());
                if (fence.sealed.get() && lastSeq > fence.sealedEndSeq) {
                    throw new IllegalStateException("Epoch is sealed at " + fence.sealedEndSeq + " but publish wants " + lastSeq);
                }
                fence.lastSeq.set(lastSeq);

                final Ingress ing = getOrCreateIngress(pid, epoch);

                // enqueue into ingress queue (fast)
                for (int i = 0; i < count; i++) {
                    ing.publishForEpoch(epoch, payloads[i]);
                }

                if (!awaitPersisted(ing, epoch, lastSeq, APPEND_WAIT_TIMEOUT_MS)) {
                    throw new RuntimeException("timeout waiting for durable seq=" + lastSeq);
                }

                // replicate as a batch
                final EpochPlacement placementCache = pe.activePlacement;
                final int[] placementArr = placementCache != null
                        ? placementCache.getStorageNodesArray()
                        : ensureConfig(pid).activeEpoch().placement().getStorageNodesArray();

                final int quorum = placementCache != null
                        ? placementCache.getAckQuorum()
                        : ensureConfig(pid).activeEpoch().placement().getAckQuorum();

                if (replicaScratch.length < placementArr.length) {
                    replicaScratch = new int[placementArr.length];
                }

                int rc = 0;
                for (final int id : placementArr) {
                    if (id != myNodeId) replicaScratch[rc++] = id;
                }

                if (rc > 0) {
                    final BrokerApi.AppendBatchRequest.Builder bb = BrokerApi.AppendBatchRequest.newBuilder()
                            .setPartitionId(pid)
                            .setEpoch(epoch)
                            .setBaseSeq(baseSeq)
                            .setTopic(topic)
                            .setRetries(retries);

                    for (int i = 0; i < count; i++) {
                        bb.addPayloads(com.google.protobuf.UnsafeByteOperations.unsafeWrap(payloads[i]));
                    }

                    final BrokerApi.Envelope env = BrokerApi.Envelope.newBuilder()
                            .setCorrelationId(System.nanoTime())
                            .setAppendBatch(bb.build())
                            .build();

                    // HOT PATH OPT: no List boxing/allocation
                    replicator.replicate(env, replicaScratch, rc, quorum);
                }

                for (int i = 0; i < count; i++) publishFuts[i].complete(null);

            } catch (final Throwable t) {
                for (int i = 0; i < count; i++) {
                    publishFuts[i].completeExceptionally(t);
                }
            } finally {
                Arrays.fill(payloads, 0, count, null);
                Arrays.fill(publishFuts, 0, count, null);
            }
        }
    }

    // small tasks
    private record PublishTask(long correlationId, String topic, int retries, byte[] payload, CompletableFuture<Void> future) {}
    private record ReplicaAppendTask(BrokerApi.AppendRequest req, CompletableFuture<BrokerApi.ReplicationAck> future) {}
    private record ReplicaAppendBatchTask(BrokerApi.AppendBatchRequest req, CompletableFuture<BrokerApi.ReplicationAck> future) {}
    private record SealTask(BrokerApi.SealRequest req, CompletableFuture<BrokerApi.ReplicationAck> future) {}
    private record OpenEpochTask(BrokerApi.OpenEpochRequest req, CompletableFuture<BrokerApi.ReplicationAck> future) {}
    private record MetadataUpdateTask(BrokerApi.MetadataUpdate req, CompletableFuture<BrokerApi.ReplicationAck> future) {}

    /**
     * Low-allocation MPSC ring queue.
     */
    private static final class MpscQueue {
        private static final VarHandle SEQ, BUF;
        static {
            SEQ = MethodHandles.arrayElementVarHandle(long[].class);
            BUF = MethodHandles.arrayElementVarHandle(Object[].class);
        }

        private final int mask;
        private final int capacity;
        private final long[] sequence;
        private final Object[] buffer;

        private final AtomicLong tail = new AtomicLong(0);
        private final AtomicLong head = new AtomicLong(0);

        MpscQueue(final int capacityPow2) {
            if (Integer.bitCount(capacityPow2) != 1) throw new IllegalArgumentException("capacity must be pow2");
            this.capacity = capacityPow2;
            this.mask = capacityPow2 - 1;
            this.sequence = new long[capacityPow2];
            this.buffer = new Object[capacityPow2];
            for (int i = 0; i < capacityPow2; i++) sequence[i] = i;
        }

        boolean offer(final Object item) {
            Objects.requireNonNull(item, "item");
            long t;
            while (true) {
                t = tail.get();
                final int idx = (int) (t & mask);
                final long sv = (long) SEQ.getVolatile(sequence, idx);
                final long dif = sv - t;
                if (dif == 0) {
                    if (tail.compareAndSet(t, t + 1)) break;
                } else if (dif < 0) {
                    return false; // full
                } else {
                    Thread.onSpinWait();
                }
            }
            final int idx = (int) (t & mask);
            BUF.setRelease(buffer, idx, item);
            SEQ.setRelease(sequence, idx, t + 1);
            return true;
        }

        Object poll() {
            long h;
            while (true) {
                h = head.get();
                final int idx = (int) (h & mask);
                final long sv = (long) SEQ.getVolatile(sequence, idx);
                final long dif = sv - (h + 1);
                if (dif == 0) {
                    if (head.compareAndSet(h, h + 1)) break;
                } else if (dif < 0) {
                    return null; // empty
                } else {
                    Thread.onSpinWait();
                }
            }

            final int idx = (int) (h & mask);
            final Object item = BUF.getAcquire(buffer, idx);

            // IMPORTANT: clear BEFORE making slot available
            BUF.setRelease(buffer, idx, null);
            SEQ.setRelease(sequence, idx, h + capacity);

            return item;
        }
    }

    // ---------- Fast replica append paths (serialized => no CAS loops) ----------

    private BrokerApi.ReplicationAck appendReplicaFast(final BrokerApi.AppendRequest a) {
        final int pid = a.getPartitionId();
        final long epoch = a.getEpoch();
        final long seq = a.getSeq();

        if (!registry.contains(a.getTopic())) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_INVALID_REQUEST)
                    .setErrorMessage("Unknown topic: " + a.getTopic())
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
        final PartitionEpochState fence = pe.epochFences.computeIfAbsent(epoch, __ -> new PartitionEpochState());

        if ((st.sealed && seq > st.sealedEndSeq) || (fence.sealed.get() && seq > fence.sealedEndSeq)) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY)
                    .setErrorMessage("Epoch sealed at " + Math.max(st.sealedEndSeq, fence.sealedEndSeq))
                    .build();
        }

        final Ingress ing = getOrCreateIngress(pid, epoch);

        final long cur = st.lastSeqReserved.get();

        // duplicate/late: ack immediately (no durable wait on replica hot path)
        if (seq <= cur) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                    .setOffset(Lsn.encode(epoch, cur))
                    .build();
        }

        // enforce contiguous acceptance
        if (seq != cur + 1) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY)
                    .setErrorMessage("Gap. expected=" + (cur + 1) + " got=" + seq)
                    .build();
        }

        try {
            ing.publishForEpoch(epoch, a.getPayload().toByteArray());
        } catch (final Throwable t) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_PERSISTENCE_FAILED)
                    .setErrorMessage("append failed: " + t)
                    .build();
        }

        // commit state ONLY after successful write
        st.lastSeqReserved.set(seq);
        fence.lastSeq.set(seq);

        return BrokerApi.ReplicationAck.newBuilder()
                .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                .setOffset(Lsn.encode(epoch, seq))
                .build();
    }

    private BrokerApi.ReplicationAck appendReplicaBatchFast(final BrokerApi.AppendBatchRequest b) {
        final int pid = b.getPartitionId();
        final long epoch = b.getEpoch();
        final long baseSeq = b.getBaseSeq();

        if (!registry.contains(b.getTopic())) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_INVALID_REQUEST)
                    .setErrorMessage("Unknown topic: " + b.getTopic())
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
        final PartitionEpochState fence = pe.epochFences.computeIfAbsent(epoch, __ -> new PartitionEpochState());
        final long lastSeq = baseSeq + n - 1L;

        if ((st.sealed && lastSeq > st.sealedEndSeq) || (fence.sealed.get() && lastSeq > fence.sealedEndSeq)) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY)
                    .setErrorMessage("Epoch sealed at " + Math.max(st.sealedEndSeq, fence.sealedEndSeq))
                    .build();
        }

        final Ingress ing = getOrCreateIngress(pid, epoch);

        final long cur = st.lastSeqReserved.get();

        // fully duplicate: ack immediately
        if (lastSeq <= cur) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                    .setOffset(Lsn.encode(epoch, cur))
                    .build();
        }

        final long expected = cur + 1;

        // gap: leader started beyond what we have
        if (baseSeq > expected) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY)
                    .setErrorMessage("Gap. expected=" + expected + " got=" + baseSeq)
                    .build();
        }

        // overlap is allowed: skip already-present prefix
        final int startIdx = (int) (expected - baseSeq); // >= 0 here
        if (startIdx >= n) {
            // should be impossible because lastSeq > cur, but keep it safe
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                    .setOffset(Lsn.encode(epoch, cur))
                    .build();
        }

        try {
            for (int i = startIdx; i < n; i++) {
                ing.publishForEpoch(epoch, payloads.get(i).toByteArray());
            }
        } catch (final Throwable t) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_PERSISTENCE_FAILED)
                    .setErrorMessage("append batch failed: " + t)
                    .build();
        }

        // commit after successful writes
        st.lastSeqReserved.set(lastSeq);
        fence.lastSeq.set(lastSeq);

        return BrokerApi.ReplicationAck.newBuilder()
                .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                .setOffset(Lsn.encode(epoch, lastSeq))
                .build();
    }

    // ---------- Existing logic kept (metadata / fencing / rollover / backfill etc) ----------

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

    private CompletableFuture<Void> forwardWithRetry(final RemoteBrokerClient client,
                                                     final BrokerApi.Envelope env,
                                                     final int partitionId,
                                                     final int attempt) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        client.sendEnvelopeWithAck(env).whenComplete((ack, err) -> {
            if (err != null) {
                if (attempt < 1) {
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
                result.completeExceptionally(new RuntimeException("Forwarding failed: " + ack.getStatus()));
                return;
            }
            result.complete(null);
        });
        return result;
    }

    private void backfillTick() {
        for (final var entry : ingressMap.entrySet()) {
            final int pid = entry.getKey();
            final Ingress ing = entry.getValue();

            final LogConfiguration cfg = metadataStore.current(pid).orElse(null);
            if (cfg == null) continue;

            for (final EpochMetadata em : cfg.epochs()) {
                final long epoch = em.epoch();
                if (!em.isSealed()) continue;
                if (!em.placement().getStorageNodes().contains(myNodeId)) continue;
                if (ing.getVirtualLog().hasEpoch(epoch)) continue;

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
                        if (reply.getEndOfEpoch()) break;
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
        return (configVersion + 1L) << 16 | (myNodeId & 0xFFFFL);
    }

    private boolean shouldDropDuplicate(final int partitionId, final byte[] key, final byte[] payload) {
        final Set<Long> seen = seenMessageIds.get(partitionId);
        if (seen == null) throw new IllegalStateException("Seen set missing for partition " + partitionId);
        final long msgId = computeMessageId(partitionId, key, payload);
        return !seen.add(msgId);
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

        if (epoch == currentHighest && tieBreaker <= currentTie) {
            return BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY)
                    .setErrorMessage("epoch already opened with equal/greater tieBreaker")
                    .build();
        }

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
            // FIX: include 4-byte header in maxBytes accounting
            if (written[0] + payloadLen + Integer.BYTES > maxBytes) return;
            final byte[] buf = new byte[payloadLen + Integer.BYTES];
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

    private boolean awaitPersisted(final Ingress ing, final long epoch, final long targetSeq, final long timeoutMs) {
        if (targetSeq < 0) return true;

        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        long spins = 0;

        while (System.nanoTime() < deadline) {
            if (ing.highWaterMark(epoch) >= targetSeq) return true;
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
        if (existing != null && existing.getActiveEpoch() == epoch) return existing;

        return ingressMap.compute(partitionId, (pid, current) -> {
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
                vLog.discoverOnDisk(); // IMPORTANT: required for lazily created replicas

                final Ingress ingress = Ingress.create(registry, ring, vLog, epoch, batchSize, forceDurable);

                deliveryMap.putIfAbsent(pid, new Delivery(ring));
                if (idempotentMode) {
                    seenMessageIds.computeIfAbsent(pid, __ -> ConcurrentHashMap.newKeySet());
                }

                final long last = ingress.getVirtualLog().forEpoch(epoch).getHighWaterMark();
                final PartitionEpochs pe = epochsByPartition.computeIfAbsent(pid, __ -> new PartitionEpochs());
                pe.active = new EpochState(epoch, last);
                pe.highestSeenEpoch.accumulateAndGet(epoch, Math::max);

                final List<Integer> placementNodes = replicaResolver.replicas(pid);
                final EpochPlacement placement = new EpochPlacement(epoch, placementNodes, replicator.getAckQuorum());
                metadataStore.bootstrapIfAbsent(pid, placement, Math.max(0, last + 1));

                // ensure pipeline exists once ingress exists
                pipeline(pid);

                return ingress;
            } catch (final Exception e) {
                throw new RuntimeException("Failed to create ingress for partition " + pid + " epoch " + epoch, e);
            }
        });
    }

    private PartitionEpochs partitionEpochs(final int partitionId) {
        return epochsByPartition.computeIfAbsent(partitionId, __ -> new PartitionEpochs());
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
        final EpochMetadata meta = cfg.get().epoch(epoch);
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
     * Trigger rollover when the projected last sequence would exceed threshold.
     *
     * IMPORTANT: This method performs:
     *  - local seal + fence persistence,
     *  - replication of a sealOnly=true request,
     *  - replication of an explicit OpenEpoch request (with a leader-chosen tieBreaker),
     *  - metadataStore update and local state advance.
     */
    private void maybeTriggerRollover(final int partitionId, final PartitionEpochs pe, final EpochState st, final long projectedLastSeq) {
        if (st.sealed) return;
        if (projectedLastSeq < SEQ_ROLLOVER_THRESHOLD) return;
        if (!pe.rolling.compareAndSet(false, true)) return;

        try {
            final Ingress ing = getOrCreateIngress(partitionId, st.epochId);

            final long cur = st.lastSeqReserved.get();
            final long sealedEnd = Math.max(cur, ing.getVirtualLog().forEpoch(st.epochId).getHighWaterMark());

            // local seal
            st.sealed = true;
            st.sealedEndSeq = sealedEnd;

            final PartitionEpochState fence = pe.epochFences.computeIfAbsent(st.epochId, __ -> new PartitionEpochState());
            fence.lastSeq.set(Math.max(fence.lastSeq.get(), cur));
            fence.sealed.set(true);
            fence.sealedEndSeq = sealedEnd;

            final Path partDir = baseDataDir.resolve("partition-" + partitionId);
            FenceStore.storeEpochFence(partDir, st.epochId, sealedEnd, fence.lastSeq.get(), true);

            final LogConfiguration cfg = ensureConfig(partitionId);
            final int[] placementArr = cfg.activeEpoch().placement().getStorageNodesArray();
            final ArrayList<Integer> replicas = new ArrayList<>(placementArr.length);
            for (final int id : placementArr) if (id != myNodeId) replicas.add(id);
            final int quorum = cfg.activeEpoch().placement().getAckQuorum();

            // replicate a SEAL-ONLY to replicas (do NOT ask replicas to locally invent tieBreakers)
            if (!replicas.isEmpty()) {
                final BrokerApi.Envelope sealEnv = BrokerApi.Envelope.newBuilder()
                        .setCorrelationId(System.nanoTime())
                        .setSeal(BrokerApi.SealRequest.newBuilder()
                                .setPartitionId(partitionId)
                                .setEpoch(st.epochId)
                                .setSealOnly(true)
                                .build())
                        .build();
                replicator.replicate(sealEnv, replicas, quorum);
            }

            // leader chooses tieBreaker + opens next epoch explicitly
            final long newEpochId = st.epochId + 1;
            final long nextTieBreaker = computeTieBreaker(partitionId);

            if (!replicas.isEmpty()) {
                final BrokerApi.Envelope openEnv = BrokerApi.Envelope.newBuilder()
                        .setCorrelationId(System.nanoTime())
                        .setOpenEpoch(BrokerApi.OpenEpochRequest.newBuilder()
                                .setPartitionId(partitionId)
                                .setEpoch(newEpochId)
                                .setTieBreaker(nextTieBreaker)
                                .build())
                        .build();
                replicator.replicate(openEnv, replicas, quorum);
            }

            final List<Integer> newPlacement = replicaResolver.replicas(partitionId);
            final EpochPlacement ep = new EpochPlacement(newEpochId, newPlacement, replicator.getAckQuorum());
            metadataStore.sealAndCreateEpoch(partitionId, st.epochId, sealedEnd, ep, newEpochId, nextTieBreaker);

            final Ingress ingNext = getOrCreateIngress(partitionId, newEpochId);
            ingNext.setActiveEpoch(newEpochId);
            final long nextLast = ingNext.highWaterMark(newEpochId);

            pe.active = new EpochState(newEpochId, nextLast);
            pe.highestSeenEpoch.accumulateAndGet(newEpochId, Math::max);
            pe.lastTieBreaker.set(nextTieBreaker);
            pe.activePlacement = ep;

            FenceStore.storeHighest(partDir, newEpochId);

        } catch (final Exception e) {
            log.warn("Rollover failed for partition {} epoch {}: {}", partitionId, st.epochId, e.toString());
        } finally {
            pe.rolling.set(false);
        }
    }

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
        final AtomicLong highestSeenEpoch = new AtomicLong(0L);
        final AtomicLong lastTieBreaker = new AtomicLong(0L);
        final AtomicBoolean rolling = new AtomicBoolean(false);
        final ConcurrentMap<Long, PartitionEpochState> epochFences = new ConcurrentHashMap<>();
        volatile EpochState active;
        volatile EpochPlacement activePlacement;
    }

    public void shutdown() throws IOException {
        if (!closed.compareAndSet(false, true)) return;

        backfillExecutor.shutdownNow();
        adminExecutor.shutdownNow();

        try { replicator.shutdown(); } catch (final Exception ignored) {}

        for (final RemoteBrokerClient c : clusterNodes.values()) {
            try { c.close(); } catch (final Exception ignored) {}
        }
        clusterNodes.clear();

        for (final PartitionPipeline p : pipelines.values()) {
            try { p.stop(); } catch (final Exception ignored) {}
        }
        pipelines.clear();

        for (final Ingress ingress : ingressMap.values()) {
            try { ingress.close(); } catch (final Exception ignored) {}
        }
    }

    private long computeMessageId(final int partitionId, final byte[] key, final byte[] payload) {
        final int keyHash = (key != null ? Arrays.hashCode(key) : 0);
        final int payloadHash = Arrays.hashCode(payload);
        final int combined = 31 * keyHash + payloadHash;
        return (((long) partitionId) << 32) ^ (combined & 0xFFFF_FFFFL);
    }
}
