package test;

import com.google.protobuf.ByteString;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.cluster.impl.RoundRobinPartitioner;
import io.ringbroker.core.wait.AdaptiveSpin;
import io.ringbroker.core.wait.WaitStrategy;
import io.ringbroker.grpc.server.GrpcAdminServer;
import io.ringbroker.offset.InMemoryOffsetStore;
import io.ringbroker.proto.test.EventsProto;
import io.ringbroker.registry.TopicRegistry;
import io.ringbroker.transport.type.NettyTransport;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public final class TestMain {
    private static final int TOTAL_PARTITIONS = 16;
    private static final int RING_SIZE = 1 << 20;
    private static final WaitStrategy WAIT_STRATEGY = new AdaptiveSpin();
    private static final long SEG_BYTES = 128L << 20;
    private static final int WRITER_THREADS = 8;
    private static final int BATCH_SIZE = 12000;
    private static final long TOTAL_MESSAGES = 50_000_000L;
    private static final String TOPIC = "orders/created";
    private static final Path DATA = Paths.get("data");
    private static final String SUB_GROUP = "sub-benchmark";
    private static final String FETCH_GROUP = "fetch-benchmark";

    public static void main(final String[] args) throws Exception {
        // 1) Clean data directory
        if (Files.exists(DATA)) {
            Files.walk(DATA)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        }
        Files.createDirectories(DATA);

        // 2) Build registry, offset store, ingress
        final TopicRegistry registry = new TopicRegistry.Builder()
                .topic(TOPIC, EventsProto.OrderCreated.getDescriptor())
                .build();
        final InMemoryOffsetStore offsetStore = new InMemoryOffsetStore();
        final ClusteredIngress ingress = ClusteredIngress.create(
                registry,
                new RoundRobinPartitioner(),
                TOTAL_PARTITIONS,
                0, 1,
                new HashMap<>(),
                DATA,
                RING_SIZE,
                WAIT_STRATEGY,
                SEG_BYTES,
                WRITER_THREADS,
                BATCH_SIZE,
                false,
                offsetStore
        );

        // 3) Start our raw-TCP transport
        final NettyTransport tcpTransport = new NettyTransport(9090, ingress, offsetStore);

        tcpTransport.start();

        // 4) Prepare client
        final RawTcpClient client = new RawTcpClient("localhost", 9090);

        // 5) Prepare payload
        final byte[] payload = EventsProto.OrderCreated.newBuilder()
                .setOrderId("ord-1")
                .setCustomer("bob")
                .build()
                .toByteArray();

        // === A) DIRECT ingress.publish() ===
        {
            long written = 0;
            final Instant t0 = Instant.now();
            while (written < TOTAL_MESSAGES) {
                for (int i = 0; i < BATCH_SIZE && written < TOTAL_MESSAGES; i++, written++) {
                    final byte[] key = ("key-" + written).getBytes(StandardCharsets.UTF_8);
                    // calls the 4-arg overload: topic, key, retries, payload
                    ingress.publish(TOPIC, key, 0, payload);
                }

            }
            final Instant t1 = Instant.now();
            final double secs = Duration.between(t0, t1).toMillis() / 1000.0;
            log.info(String.format(
                    "[DIRECT-INGRESS] Wrote %,d msgs in %.2fs = %.1f msgs/s",
                    written, secs, written / secs
            ));
        }

        // === B) TCP batch publish ===
        {
            long written = 0;
            final Instant t0 = Instant.now();

            // collect all batch futures to join later
            final List<CompletableFuture<Void>> publishFuts = new ArrayList<>();

            while (written < TOTAL_MESSAGES) {
                final List<BrokerApi.Message> batch = new ArrayList<>(BATCH_SIZE);
                for (int i = 0; i < BATCH_SIZE && written < TOTAL_MESSAGES; i++, written++) {
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

            // flush all writes
            client.finishAndFlush();
            // wait for all publish acks
            CompletableFuture.allOf(publishFuts.toArray(new CompletableFuture[0])).join();

            final Instant t1 = Instant.now();
            final double secs = Duration.between(t0, t1).toMillis() / 1000.0;
            log.info(String.format(
                    "[TCP-BATCH] Wrote %,d msgs (%d batches) in %.2fs = %.1f msgs/s",
                    TOTAL_MESSAGES,
                    (TOTAL_MESSAGES + BATCH_SIZE - 1) / BATCH_SIZE,
                    secs,
                    TOTAL_MESSAGES / secs
            ));
        }

        // === C) Commit offsets to zero before fetch ===
        {
            final List<CompletableFuture<Void>> commitFuts = new ArrayList<>();
            for (int p = 0; p < TOTAL_PARTITIONS; p++) {
                commitFuts.add(client.commitAsync(TOPIC, FETCH_GROUP, p, 0L));
            }
            CompletableFuture.allOf(commitFuts.toArray(new CompletableFuture[0])).join();
        }

        // === D) Fetch loop ===
        {
            final AtomicLong[] offs = new AtomicLong[TOTAL_PARTITIONS];
            for (int p = 0; p < TOTAL_PARTITIONS; p++) {
                final long o = client.fetchCommittedAsync(TOPIC, FETCH_GROUP, p).join();
                offs[p] = new AtomicLong(o);
            }

            long totalFetched = 0;
            final Instant f0 = Instant.now();
            while (totalFetched < TOTAL_MESSAGES) {
                for (int p = 0; p < TOTAL_PARTITIONS && totalFetched < TOTAL_MESSAGES; p++) {
                    final long off = offs[p].get();
                    final List<BrokerApi.MessageEvent> msgs =
                            client.fetchAsync(TOPIC, p, off, 1000).join();
                    if (msgs.isEmpty()) continue;
                    for (final BrokerApi.MessageEvent ev : msgs) {
                        offs[p].set(ev.getOffset() + 1);
                        totalFetched++;
                    }
                    client.commitAsync(TOPIC, FETCH_GROUP, p, offs[p].get()).join();
                }
            }
            final Instant f1 = Instant.now();
            final double secs = Duration.between(f0, f1).toMillis() / 1000.0;
            log.info(String.format(
                    "[TCP-FETCH] Delivered %,d msgs in %.2fs = %.1f msgs/s",
                    totalFetched, secs, totalFetched / secs
            ));
        }

        // === E) Subscribe test ===
        {
            final AtomicLong received = new AtomicLong(0);
            final Instant s0 = Instant.now();
            client.subscribe(TOPIC, SUB_GROUP, (seq, body) -> {
                if (received.incrementAndGet() >= TOTAL_MESSAGES) {
                    // done
                }
            });
            // spin until done
            while (received.get() < TOTAL_MESSAGES) {
                Thread.sleep(1);
            }
            final Instant s1 = Instant.now();
            final double secs = Duration.between(s0, s1).toMillis() / 1000.0;
            log.info(String.format(
                    "[TCP-SUBSCRIBE] Received %,d msgs in %.2fs = %.1f msgs/s",
                    received.get(), secs, received.get() / secs
            ));
        }

        // Cleanup
        client.close();
        tcpTransport.stop();
        log.info("=== Benchmark complete ===");
    }
}
