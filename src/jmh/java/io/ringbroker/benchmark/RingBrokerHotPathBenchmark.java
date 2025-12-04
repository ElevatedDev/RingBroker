package io.ringbroker.benchmark;

import io.ringbroker.broker.delivery.Delivery;
import io.ringbroker.broker.ingress.Ingress;
import io.ringbroker.core.ring.RingBuffer;
import io.ringbroker.core.wait.AdaptiveSpin;
import io.ringbroker.core.wait.Blocking;
import io.ringbroker.core.wait.BusySpin;
import io.ringbroker.core.wait.WaitStrategy;
import io.ringbroker.ledger.orchestrator.VirtualLog;
import io.ringbroker.registry.TopicRegistry;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@State(Scope.Benchmark)
public class RingBrokerHotPathBenchmark {

    private static final int RING_SIZE = 1 << 20;
    private static final long SEG_BYTES = 256L << 20;
    private static final int BATCH_SIZE = 12_000;

    private static final String TOPIC = "hot-topic";
    private static final Path DATA = Paths.get("data-jmh-hot");

    @Param({"100000"})
    private long totalMessages;

    @Param({"adaptive-spin", "blocking", "busy-spin"})
    private String waitStrategy;

    private Ingress ingress;
    private VirtualLog vLog;
    private Delivery delivery;

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(RingBrokerHotPathBenchmark.class);

    private byte[] payload;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        try {
            wipeDir(DATA);
            Files.createDirectories(DATA);

            final WaitStrategy ws = switch (this.waitStrategy) {
                case "adaptive-spin" -> new AdaptiveSpin();
                case "blocking" -> new Blocking();
                case "busy-spin" -> new BusySpin();
                default -> throw new IllegalArgumentException("Unknown wait strategy: " + this.waitStrategy);
            };

            // Ingress only checks registry.contains(topic), so descriptor can be null if your registry supports it.
            final TopicRegistry registry = new TopicRegistry.Builder()
                    .topic(TOPIC, null)
                    .build();

            final Path partitionDir = DATA.resolve("partition-0");
            Files.createDirectories(partitionDir);

            final RingBuffer<byte[]> ring = new RingBuffer<>(RING_SIZE, ws);

            vLog = new VirtualLog(partitionDir, (int) SEG_BYTES);
            vLog.discoverOnDisk();

            // durable=false for pure hot path
            ingress = Ingress.create(registry, ring, vLog, 0L, BATCH_SIZE, false);

            // IMPORTANT: keep ring drained, otherwise you benchmark ring backpressure.
            delivery = new Delivery(ring);
            delivery.subscribe(0L, (seq, msg) -> {
                // no-op consumer
            });

            payload = "hot".getBytes(StandardCharsets.UTF_8);
        } catch (final Exception e) {
            // best-effort cleanup if setup fails mid-way
            try { tearDown(); } catch (final Exception ignore) {}
            throw e;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (ingress != null) {
            try { ingress.close(); } catch (final Exception ignore) {}
        }
        if (vLog != null) {
            try { vLog.close(); } catch (final Exception ignore) {}
        }
        log.info("=== Hot-path benchmark complete ===");
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Measurement(iterations = 3, time = 10)
    public void hotPathIngressPublish(final Blackhole blackhole) {
        long written = 0;
        while (written < totalMessages) {
            for (int i = 0; i < BATCH_SIZE && written < totalMessages; i++, written++) {
                ingress.publish(TOPIC, payload);
            }
        }
        blackhole.consume(written);
    }

    private static void wipeDir(final Path root) throws IOException {
        if (!Files.exists(root)) return;
        try (Stream<Path> stream = Files.walk(root)) {
            stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); }
                catch (Exception ignore) {}
            });
        }
    }
}
