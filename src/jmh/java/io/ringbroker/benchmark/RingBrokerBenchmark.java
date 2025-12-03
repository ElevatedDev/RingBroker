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
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
@State(Scope.Benchmark)
public class RingBrokerBenchmark {

    private static final int TOTAL_PARTITIONS = 16;
    private static final int RING_SIZE = 1 << 20;
    private static final long SEG_BYTES = 256L << 20;
    private static final int BATCH_SIZE = 12_000;

    private static final String TOPIC = "orders/created";
    private static final Path DATA = Paths.get("data-jmh-cluster");

    @Param({"100000"})
    private long totalMessages;

    @Param({"adaptive-spin", "blocking", "busy-spin"})
    private String waitStrategy;

    private byte[] payload;

    // Cluster state
    private Map<Integer, ClusteredIngress> clusterIngresses;
    private Map<Integer, InMemoryOffsetStore> clusterOffsetStores;
    private ReplicaSetResolver clusterResolver;

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

            buildCluster(registry, ws);

        } catch (Exception e) {
            // Ensure we don’t leak threads if setup fails mid-way
            try { tearDown(); } catch (Exception ignore) {}
            throw e;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        // 1) Shutdown ingresses first (they may still use offset store / executors)
        if (clusterIngresses != null) {
            for (ClusteredIngress ci : clusterIngresses.values()) {
                if (ci == null) continue;
                try { ci.shutdown(); } catch (Exception ignore) {}
            }
        }

        // 2) Close offset stores to stop flusher threads
        if (clusterOffsetStores != null) {
            for (InMemoryOffsetStore os : clusterOffsetStores.values()) {
                if (os == null) continue;
                try { os.close(); } catch (Exception ignore) {}
            }
        }

        log.info("=== Cluster benchmark complete ===");
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Measurement(iterations = 3, time = 10)
    public void quorumPublish(final Blackhole blackhole) {
        long written = 0;
        final ClusteredIngress leader = clusterIngresses.get(0);

        while (written < totalMessages) {
            for (int i = 0; i < BATCH_SIZE && written < totalMessages; i++, written++) {
                final byte[] key = ("qkey-" + written).getBytes(StandardCharsets.UTF_8);
                leader.publish(TOPIC, key, payload);
            }
        }

        blackhole.consume(written);
    }

    private void buildCluster(final TopicRegistry registry, final WaitStrategy ws) throws IOException {
        final int clusterSize = 3;
        final int ackQuorum = 2;

        final List<Member> members = new ArrayList<>(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            members.add(new Member(
                    i,
                    BrokerRole.PERSISTENCE,
                    new InetSocketAddress("localhost", 9000 + i),
                    System.currentTimeMillis(),
                    1
            ));
        }

        clusterResolver = new ReplicaSetResolver(clusterSize, () -> members);
        clusterIngresses = new HashMap<>(clusterSize);
        clusterOffsetStores = new HashMap<>(clusterSize);

        // Deferred ingress targets for in-proc client wiring
        final Map<Integer, AtomicReference<ClusteredIngress>> targets = new HashMap<>();
        for (int i = 0; i < clusterSize; i++) targets.put(i, new AtomicReference<>());

        for (int nodeId = 0; nodeId < clusterSize; nodeId++) {
            final Path nodeDir = DATA.resolve("node-" + nodeId);
            Files.createDirectories(nodeDir);

            // optional but helps keep layout consistent / avoids edge cases
            for (int p = 0; p < TOTAL_PARTITIONS; p++) {
                Files.createDirectories(nodeDir.resolve("partition-" + p));
            }

            final Path offsetsDir = nodeDir.resolve("offsets");
            Files.createDirectories(offsetsDir);

            final InMemoryOffsetStore offsetStore = new InMemoryOffsetStore(offsetsDir);
            clusterOffsetStores.put(nodeId, offsetStore);

            // clients map is used by metadata broadcaster + replicator
            final Map<Integer, RemoteBrokerClient> clientsForNode = new ConcurrentHashMap<>();

            final LogMetadataStore metadataStore = new BroadcastingLogMetadataStore(
                    new JournaledLogMetadataStore(nodeDir.resolve("metadata")),
                    clientsForNode,
                    nodeId,
                    () -> targets.keySet()
            );

            final AdaptiveReplicator rep = new AdaptiveReplicator(ackQuorum, clientsForNode, 1_000);

            final ClusteredIngress ci = ClusteredIngress.create(
                    registry,
                    new RoundRobinPartitioner(),
                    TOTAL_PARTITIONS,
                    nodeId,
                    clusterSize,
                    clientsForNode,
                    nodeDir,
                    RING_SIZE,
                    ws,
                    SEG_BYTES,
                    BATCH_SIZE,
                    false,
                    offsetStore,
                    BrokerRole.PERSISTENCE,
                    clusterResolver,
                    rep,
                    metadataStore
            );

            clusterIngresses.put(nodeId, ci);
            targets.get(nodeId).set(ci);

            // Now wire in-proc clients to other nodes
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
