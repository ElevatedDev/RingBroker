package io.ringbroker.benchmark;

import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.membership.member.Member;
import io.ringbroker.cluster.membership.replicator.AdaptiveReplicator;
import io.ringbroker.cluster.membership.resolver.ReplicaSetResolver;
import io.ringbroker.cluster.metadata.BroadcastingLogMetadataStore;
import io.ringbroker.cluster.metadata.JournaledLogMetadataStore;
import io.ringbroker.cluster.metadata.LogMetadataStore;
import io.ringbroker.cluster.partitioner.impl.RoundRobinPartitioner;
import io.ringbroker.core.wait.AdaptiveSpin;
import io.ringbroker.core.wait.Blocking;
import io.ringbroker.core.wait.BusySpin;
import io.ringbroker.core.wait.WaitStrategy;
import io.ringbroker.offset.InMemoryOffsetStore;
import io.ringbroker.proto.test.EventsProto;
import io.ringbroker.registry.TopicRegistry;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

@State(Scope.Benchmark)
public class RingBrokerBenchmark {

    private static final int TOTAL_PARTITIONS = 16;
    private static final int RING_SIZE = 1 << 20;
    private static final long SEG_BYTES = 256L << 20;
    private static final int BATCH_SIZE = 12_000;

    private static final String TOPIC = "orders/created";
    private static final Path DATA = Paths.get("data-jmh-cluster");

    // Window size for the "committed" benchmark: controls in-flight pressure.
    private static final int INFLIGHT_WINDOW = 4096;

    // Precomputed keys length: power-of-two for cheap masking.
    private static final int KEY_POOL_SIZE = 1 << 17; // 131072

    // Replication timeout tuned for heavy in-proc load to avoid spurious quorum drops.
    private static final long REPL_TIMEOUT_MS = 60_000L;

    @Param({"100000"})
    private int totalMessages;

    @Param({"adaptive-spin", "blocking", "busy-spin"})
    private String waitStrategy;

    /**
     * Profile:
     *  - fast        : single-node, ingestion role (no fsync/quorum)
     *  - quorum-lite : 3-node, quorum=2, ingestion role (replication without fsync)
     *  - quorum      : 3-node, quorum=2, persistence role (durable path)
     *  - frontdoor   : 1 ingestion node forwarding to 2 persistence nodes (quorum=2, durable)
     */
    @Param({"fast"})
    private String profile;

    private byte[] payload;
    private byte[][] keys;

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(RingBrokerBenchmark.class);

    // Cluster state
    private Map<Integer, ClusteredIngress> clusterIngresses;
    private Map<Integer, InMemoryOffsetStore> clusterOffsetStores;
    private ReplicaSetResolver clusterResolver;

    // Fast handle to the leader to avoid map lookups in the hot loops.
    private ClusteredIngress leader;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        try {
            wipeDir(DATA);
            Files.createDirectories(DATA);

            final TopicRegistry registry = new TopicRegistry.Builder()
                    .topic(TOPIC, EventsProto.OrderCreated.getDescriptor())
                    .build();

            final WaitStrategy ws = switch (this.waitStrategy) {
                case "adaptive-spin" -> new AdaptiveSpin();
                case "blocking" -> new Blocking();
                case "busy-spin" -> new BusySpin();
                default -> throw new IllegalArgumentException("Unknown wait strategy: " + this.waitStrategy);
            };

            payload = EventsProto.OrderCreated.newBuilder()
                    .setOrderId("ord-1")
                    .setCustomer("bob")
                    .build()
                    .toByteArray();

            // Precompute keys once to remove allocation + UTF8 encoding from the benchmark.
            keys = new byte[KEY_POOL_SIZE][];
            for (int i = 0; i < KEY_POOL_SIZE; i++) {
                keys[i] = ("qkey-" + i).getBytes(StandardCharsets.UTF_8);
            }

            final Profile prof = profileConfig();
            buildCluster(registry, ws, prof);
            leader = clusterIngresses.get(prof.leaderId());

        } catch (Exception e) {
            try { tearDown(); } catch (Exception ignore) {}
            throw e;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (clusterIngresses != null) {
            for (ClusteredIngress ci : clusterIngresses.values()) {
                if (ci == null) continue;
                try { ci.shutdown(); } catch (Exception ignore) {}
            }
        }
        if (clusterOffsetStores != null) {
            for (InMemoryOffsetStore os : clusterOffsetStores.values()) {
                if (os == null) continue;
                try { os.close(); } catch (Exception ignore) {}
            }
        }
        log.info("=== Cluster benchmark complete ===");
    }

    /**
     * Fire-and-forget throughput: measures "accepted/enqueued" pressure + backpressure behaviour,
     * not necessarily durability/quorum commit.
     *
     * With totalMessages=100000 and OperationsPerInvocation=100000, JMH reports msgs/sec directly.
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Measurement(iterations = 3, time = 10)
    @OperationsPerInvocation(100000)
    public void quorumPublish(final Blackhole blackhole) {
        final ClusteredIngress l = leader;
        final byte[] p = payload;
        final byte[][] ks = keys;
        final int mask = ks.length - 1;

        for (int written = 0; written < totalMessages; written++) {
            final byte[] key = ks[written & mask];
            l.publish(TOPIC, key, p);
        }

        blackhole.consume(totalMessages);
    }

    /**
     * Committed throughput: windowed in-flight publishing that waits for completion.
     * This is the benchmark you want when you care about quorum and "real" completion semantics.
     *
     * Still reports msgs/sec directly.
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Measurement(iterations = 3, time = 10)
    @OperationsPerInvocation(100000)
    public void quorumPublishCommittedWindowed(final Blackhole blackhole) {
        final ClusteredIngress l = leader;
        final byte[] p = payload;
        final byte[][] ks = keys;
        final int mask = ks.length - 1;

        @SuppressWarnings("unchecked")
        final CompletableFuture<Void>[] inflight = (CompletableFuture<Void>[]) new CompletableFuture[INFLIGHT_WINDOW];

        int written = 0;
        while (written < totalMessages) {
            int w = 0;

            // Fill a window.
            for (; w < INFLIGHT_WINDOW && written < totalMessages; w++, written++) {
                final byte[] key = ks[written & mask];
                inflight[w] = l.publish(TOPIC, key, p);
            }

            // Wait the window (sequential join avoids creating allOf() objects each window).
            for (int i = 0; i < w; i++) {
                inflight[i].join();
                inflight[i] = null; // help GC in long runs / different params
            }
        }

        blackhole.consume(written);
    }

    /**
     * Message-level latency probe: one publish+wait per operation. Use this to inspect p50/p99.
     * (Do NOT multiply by 100k; this one is already "1 message op".)
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Measurement(iterations = 3, time = 10)
    public void quorumPublishSingleMessageLatency(final Blackhole blackhole) {
        // Use a changing key to avoid pathological caching / same-partition artifacts.
        final int idx = (int) (System.nanoTime() & (keys.length - 1));
        leader.publish(TOPIC, keys[idx], payload).join();
        blackhole.consume(idx);
    }

    private record Profile(int persistenceNodes,
                          int ingestionNodes,
                          int ackQuorum,
                          BrokerRole persistenceRole,
                          boolean frontdoor,
                          int leaderId) {}

    private Profile profileConfig() {
        if ("fast".equalsIgnoreCase(profile)) {
            return new Profile(1, 0, 1, BrokerRole.INGESTION, false, 0);
        }
        if ("quorum-lite".equalsIgnoreCase(profile)) {
            return new Profile(3, 0, 2, BrokerRole.INGESTION, false, 0);
        }
        if ("quorum".equalsIgnoreCase(profile)) {
            return new Profile(3, 0, 2, BrokerRole.PERSISTENCE, false, 0);
        }
        if ("frontdoor".equalsIgnoreCase(profile)) {
            // ingestion node id = 2, persistence nodes = 0,1
            return new Profile(2, 1, 2, BrokerRole.PERSISTENCE, true, 2);
        }
        throw new IllegalArgumentException("Unknown profile: " + profile + " (expected fast|quorum-lite|quorum|frontdoor)");
    }

    private void buildCluster(final TopicRegistry registry,
                              final WaitStrategy ws,
                              final Profile cfg) throws IOException {
        final int persistenceNodes = Math.max(1, cfg.persistenceNodes());
        final int ingestionNodes = Math.max(0, cfg.ingestionNodes());
        final int totalNodes = persistenceNodes + ingestionNodes;
        final int ackQuorum = Math.min(persistenceNodes, Math.max(1, cfg.ackQuorum()));

        final List<Member> persistenceMembers = new ArrayList<>(persistenceNodes);
        for (int i = 0; i < persistenceNodes; i++) {
            persistenceMembers.add(new Member(
                    i,
                    cfg.persistenceRole(),
                    new InetSocketAddress("localhost", 9000 + i),
                    System.currentTimeMillis(),
                    1
            ));
        }

        clusterResolver = new ReplicaSetResolver(persistenceNodes, () -> persistenceMembers);
        clusterIngresses = new HashMap<>(totalNodes);
        clusterOffsetStores = new HashMap<>(totalNodes);

        final Map<Integer, AtomicReference<ClusteredIngress>> targets = new HashMap<>();
        for (int i = 0; i < totalNodes; i++) targets.put(i, new AtomicReference<>());

        for (int nodeId = 0; nodeId < totalNodes; nodeId++) {
            final Path nodeDir = DATA.resolve("node-" + nodeId);
            Files.createDirectories(nodeDir);

            for (int p = 0; p < TOTAL_PARTITIONS; p++) {
                Files.createDirectories(nodeDir.resolve("partition-" + p));
            }

            final Path offsetsDir = nodeDir.resolve("offsets");
            Files.createDirectories(offsetsDir);

            final InMemoryOffsetStore offsetStore = new InMemoryOffsetStore(offsetsDir);
            clusterOffsetStores.put(nodeId, offsetStore);

            final Map<Integer, RemoteBrokerClient> clientsForNode = new ConcurrentHashMap<>();

            final LogMetadataStore metadataStore = new BroadcastingLogMetadataStore(
                    new JournaledLogMetadataStore(nodeDir.resolve("metadata")),
                    clientsForNode,
                    nodeId,
                    targets::keySet
            );

            final AdaptiveReplicator rep = new AdaptiveReplicator(ackQuorum, clientsForNode, REPL_TIMEOUT_MS);

            final ClusteredIngress ci = ClusteredIngress.create(
                    registry,
                    new RoundRobinPartitioner(),
                    TOTAL_PARTITIONS,
                    nodeId,
                    persistenceNodes,
                    clientsForNode,
                    nodeDir,
                    RING_SIZE,
                    ws,
                    SEG_BYTES,
                    BATCH_SIZE,
                    false,
                    offsetStore,
                    (cfg.frontdoor() && nodeId >= persistenceNodes)
                            ? BrokerRole.INGESTION
                            : cfg.persistenceRole(),
                    clusterResolver,
                    rep,
                    metadataStore
            );

            clusterIngresses.put(nodeId, ci);
            targets.get(nodeId).set(ci);

            clientsForNode.putAll(buildInProcClients(targets, nodeId));
        }
    }

    private static Map<Integer, RemoteBrokerClient> buildInProcClients(
            final Map<Integer, AtomicReference<ClusteredIngress>> targets,
            final int selfId
    ) {
        final Map<Integer, RemoteBrokerClient> m = new HashMap<>();
        for (Map.Entry<Integer, AtomicReference<ClusteredIngress>> e : targets.entrySet()) {
            final int id = e.getKey();
            if (id == selfId) continue;
            m.put(id, new InProcClient(e.getValue()));
        }
        return m;
    }

    private static final class InProcClient implements RemoteBrokerClient {
        private final AtomicReference<ClusteredIngress> target;

        InProcClient(final AtomicReference<ClusteredIngress> target) {
            this.target = target;
        }

        @Override
        public void sendMessage(final String topic, final byte[] key, final byte[] payload) {
            // not used here
        }

        @Override
        public CompletableFuture<io.ringbroker.api.BrokerApi.ReplicationAck> sendEnvelopeWithAck(
                final io.ringbroker.api.BrokerApi.Envelope envelope
        ) {
            final ClusteredIngress ing = target.get();
            if (ing == null) {
                return CompletableFuture.failedFuture(new IllegalStateException("target not set"));
            }
            return switch (envelope.getKindCase()) {
                case PUBLISH -> {
                    final io.ringbroker.api.BrokerApi.Message m = envelope.getPublish();
                    final byte[] key = m.getKey().isEmpty() ? null : m.getKey().toByteArray();
                    yield ing.publish(
                                    envelope.getCorrelationId(),
                                    m.getTopic(),
                                    key,
                                    m.getRetries(),
                                    m.getPayload().toByteArray()
                            )
                            .thenApply(v -> io.ringbroker.api.BrokerApi.ReplicationAck.newBuilder()
                                    .setStatus(io.ringbroker.api.BrokerApi.ReplicationAck.Status.SUCCESS)
                                    .build());
                }
                case APPEND -> ing.handleAppendAsync(envelope.getAppend());
                case APPEND_BATCH -> ing.handleAppendBatchAsync(envelope.getAppendBatch());
                case SEAL -> ing.handleSealAsync(envelope.getSeal());
                case OPEN_EPOCH -> ing.handleOpenEpochAsync(envelope.getOpenEpoch());
                case METADATA_UPDATE -> CompletableFuture.completedFuture(
                        ing.handleMetadataUpdate(envelope.getMetadataUpdate())
                );
                default -> CompletableFuture.failedFuture(
                        new UnsupportedOperationException("Unsupported kind " + envelope.getKindCase())
                );
            };
        }

        @Override
        public CompletableFuture<io.ringbroker.api.BrokerApi.BackfillReply> sendBackfill(
                final io.ringbroker.api.BrokerApi.Envelope envelope
        ) {
            // not used by this benchmark
            return CompletableFuture.completedFuture(
                    io.ringbroker.api.BrokerApi.BackfillReply.newBuilder().build()
            );
        }
    }

    private static void wipeDir(final Path root) throws IOException {
        if (!Files.exists(root)) return;
        try (Stream<Path> stream = Files.walk(root)) {
            stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); }
                catch (Exception e) { /* ignore */ }
            });
        }
    }
}
