package io.ringbroker.benchmark;

import com.google.protobuf.ByteString;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.membership.replicator.AdaptiveReplicator;
import io.ringbroker.cluster.membership.resolver.ReplicaSetResolver;
import io.ringbroker.cluster.partitioner.impl.RoundRobinPartitioner;
import io.ringbroker.core.wait.AdaptiveSpin;
import io.ringbroker.core.wait.Blocking;
import io.ringbroker.core.wait.BusySpin;
import io.ringbroker.core.wait.WaitStrategy;
import io.ringbroker.offset.InMemoryOffsetStore;
import io.ringbroker.proto.test.EventsProto;
import io.ringbroker.registry.TopicRegistry;
import io.ringbroker.transport.type.NettyTransport;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * JMH benchmark for RingBroker performance testing.
 * This class replicates the TestMain functionality as a proper JMH benchmark.
 */
@Slf4j
@State(Scope.Benchmark)
public class RingBrokerBenchmark {

    private static final int TOTAL_PARTITIONS = 16;
    private static final int RING_SIZE = 1 << 20;

    private static final long SEG_BYTES = 256L << 20;
    private static final int BATCH_SIZE = 12000;

    private static final String TOPIC = "orders/created";

    private static final Path DATA = Paths.get("data-jmh");
    private static final Path OFFSETS = DATA.resolve("offsets"); // Dedicated offsets dir

    private static final String SUB_GROUP = "sub-benchmark";
    private static final String FETCH_GROUP = "fetch-benchmark";

    // Benchmark parameters
    @Param({"100000"})
    private long totalMessages;

    @Param({"adaptive-spin", "blocking", "busy-spin"})
    private String waitStrategy;

    // Test components
    private ClusteredIngress ingress;
    private NettyTransport tcpTransport;
    private RawTcpClient client;
    private byte[] payload;
    private InMemoryOffsetStore offsetStore; // Use field to close cleanly

    @Setup(Level.Trial)
    public void setup() throws Exception {
        // Clean data directory
        if (Files.exists(DATA)) {
            try (final Stream<Path> stream = Files.walk(DATA)) {
                stream.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(file -> {
                            if (!file.delete()) {
                                log.warn("Failed to delete file: {}", file.getAbsolutePath());
                            }
                        });
            }
        }

        Files.createDirectories(DATA);
        Files.createDirectories(OFFSETS);

        // Build registry
        final TopicRegistry registry = new TopicRegistry.Builder()
                .topic(TOPIC, EventsProto.OrderCreated.getDescriptor())
                .build();

        // FIX: Instantiate Durable Offset Store
        offsetStore = new InMemoryOffsetStore(OFFSETS);

        // Create directory for each partition
        for (int p = 0; p < TOTAL_PARTITIONS; p++) {
            Files.createDirectories(DATA.resolve("partition-" + p));
        }

        final WaitStrategy waitStrategy = switch (this.waitStrategy) {
            case "adaptive-spin" -> new AdaptiveSpin();
            case "blocking" -> new Blocking();
            case "busy-spin" -> new BusySpin();
            default -> throw new IllegalArgumentException("Unknown wait strategy: " + this.waitStrategy);
        };

        final ReplicaSetResolver resolver = new ReplicaSetResolver(1, List::of);
        final AdaptiveReplicator replicator = new AdaptiveReplicator(1, Map.of(), -1);

        ingress = ClusteredIngress.create(
                registry,
                new RoundRobinPartitioner(),
                TOTAL_PARTITIONS,
                0,                     // myNodeId
                1,                     // clusterSize (single PB)
                new HashMap<>(),       // clusterNodes
                DATA,
                RING_SIZE,
                waitStrategy,
                SEG_BYTES,
                BATCH_SIZE,
                false,                 // idempotentMode
                offsetStore,
                BrokerRole.INGESTION, // local durable path
                resolver,
                replicator
        );

        // Start TCP transport
        tcpTransport = new NettyTransport(9090, ingress, offsetStore);
        tcpTransport.start();

        // Prepare client
        client = new RawTcpClient("localhost", 9090);

        // Prepare payload
        payload = EventsProto.OrderCreated.newBuilder()
                .setOrderId("ord-1")
                .setCustomer("bob")
                .build()
                .toByteArray();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }

        if (tcpTransport != null) {
            tcpTransport.stop();
        }

        // FIX: Close offset store to stop flusher thread
        if (offsetStore != null) {
            offsetStore.close();
        }

        log.info("=== Benchmark complete ===");
    }

    // [Benchmarks: directIngressPublish, tcpBatchPublish, tcpFetch remain unchanged]

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Measurement(iterations = 3, time = 10)
    public void directIngressPublish(final Blackhole blackhole) {
        long written = 0;

        while (written < totalMessages) {
            for (int i = 0; i < BATCH_SIZE && written < totalMessages; i++, written++) {
                final byte[] key = ("key-" + written).getBytes(StandardCharsets.UTF_8);

                ingress.publish(TOPIC, key, payload);
            }
        }

        blackhole.consume(written);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Measurement(iterations = 3, time = 10)
    public void tcpBatchPublish(final Blackhole blackhole) throws Exception {
        long written = 0;

        final List<CompletableFuture<Void>> publishFutures = new ArrayList<>();

        while (written < totalMessages) {
            final List<BrokerApi.Message> batch = new ArrayList<>(BATCH_SIZE);

            for (int i = 0; i < BATCH_SIZE && written < totalMessages; i++, written++) {
                final byte[] key = ("key-" + written).getBytes(StandardCharsets.UTF_8);

                final BrokerApi.Message m = BrokerApi.Message.newBuilder()
                        .setTopic(TOPIC)
                        .setRetries(0)
                        .setKey(ByteString.copyFrom(key))
                        .setPayload(ByteString.copyFrom(payload))
                        .build();

                batch.add(m);
            }

            publishFutures.add(client.publishBatchAsync(batch));
        }

        client.finishAndFlush();

        CompletableFuture.allOf(publishFutures.toArray(new CompletableFuture[0])).join();

        blackhole.consume(written);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Measurement(iterations = 3, time = 10)
    public void tcpFetch(final Blackhole blackhole) {
        long written = 0;

        final List<CompletableFuture<Void>> publishFuts = new ArrayList<>();

        while (written < totalMessages) {
            final List<BrokerApi.Message> batch = new ArrayList<>(BATCH_SIZE);

            for (int i = 0; i < BATCH_SIZE && written < totalMessages; i++, written++) {
                final byte[] key = ("key-" + written).getBytes(StandardCharsets.UTF_8);

                final BrokerApi.Message m = BrokerApi.Message.newBuilder()
                        .setTopic(TOPIC)
                        .setRetries(0)
                        .setKey(ByteString.copyFrom(key))
                        .setPayload(ByteString.copyFrom(payload))
                        .build();

                batch.add(m);
            }

            publishFuts.add(client.publishBatchAsync(batch));
        }

        client.finishAndFlush();
        CompletableFuture.allOf(publishFuts.toArray(new CompletableFuture[0])).join();

        // Commit offsets to zero before fetch
        final List<CompletableFuture<Void>> commitFuts = new ArrayList<>();

        for (int p = 0; p < TOTAL_PARTITIONS; p++) {
            commitFuts.add(client.commitAsync(TOPIC, FETCH_GROUP, p, 0L));
        }

        CompletableFuture.allOf(commitFuts.toArray(new CompletableFuture[0])).join();

        // Fetch loop
        final AtomicLong[] offs = new AtomicLong[TOTAL_PARTITIONS];

        for (int p = 0; p < TOTAL_PARTITIONS; p++) {
            final long o = client.fetchCommittedAsync(TOPIC, FETCH_GROUP, p).join();

            offs[p] = new AtomicLong(o);
        }

        long totalFetched = 0;

        while (totalFetched < totalMessages) {
            for (int p = 0; p < TOTAL_PARTITIONS && totalFetched < totalMessages; p++) {
                final long off = offs[p].get();

                final List<BrokerApi.MessageEvent> msgs = client.fetchAsync(TOPIC, p, off, 1000).join();

                if (msgs.isEmpty()) continue;

                for (final BrokerApi.MessageEvent ev : msgs) {
                    offs[p].set(ev.getOffset() + 1);
                    totalFetched++;
                }

                client.commitAsync(TOPIC, FETCH_GROUP, p, offs[p].get()).join();
            }
        }

        blackhole.consume(totalFetched);
    }
}

    /**
     * Subscribe benchmark - currently commented out in the TestMain
     * but included here so it ain't missing.
     */
//    @Benchmark
//    @BenchmarkMode(Mode.Throughput)
//    @OutputTimeUnit(TimeUnit.SECONDS)
//    @Fork(value = 1)
//    @Warmup(iterations = 1, time = 5)
//    // This test takes 5 billion years so a timeout is set
//    // This is an arbitrary/placeholder value for now
//    @Timeout(time = 30, timeUnit = TimeUnit.SECONDS)
//    @Measurement(iterations = 3, time = 10)
//    public void tcpSubscribe(final Blackhole blackhole) throws Exception {
//        tcpBatchPublish(blackhole);
//
//        final AtomicLong received = new AtomicLong(0);
//        final Object lock = new Object();
//
//        client.subscribe(TOPIC, SUB_GROUP, (seq, body) -> {
//            final long count = received.incrementAndGet();
//
//            if (count >= totalMessages) {
//                synchronized (lock) {
//                    lock.notify();
//                }
//            }
//        });
//
//        blackhole.consume(received.get());
//    }
//}
