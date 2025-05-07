package test;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.api.BrokerGrpc;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.cluster.impl.RoundRobinPartitioner;
import io.ringbroker.cluster.type.Partitioner;
import io.ringbroker.cluster.type.RemoteBrokerClient;
import io.ringbroker.core.wait.AdaptiveSpin;
import io.ringbroker.core.wait.WaitStrategy;
import io.ringbroker.offset.InMemoryOffsetStore;
import io.ringbroker.proto.test.EventsProto;
import io.ringbroker.registry.TopicRegistry;
import io.ringbroker.transport.GrpcTransport;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public final class TestMain {
    private static final int TOTAL_PARTITIONS = 16;
    private static final int RING_SIZE = 1 << 20;
    private static final WaitStrategy WAIT_STRATEGY = new AdaptiveSpin();
    private static final long SEG_BYTES = 128L << 20;
    private static final int WRITER_THREADS = 8;
    private static final int BATCH_SIZE = 4096;
    private static final long TOTAL_MESSAGES = 50_000_000L;
    private static final String TOPIC = "orders/created";
    private static final Path DATA = Paths.get("data");

    public static void main(final String[] args) throws Exception {
        // Clean data dir
        if (Files.exists(DATA)) {
            Files.walk(DATA).sorted(Comparator.reverseOrder())
                    .map(Path::toFile).forEach(java.io.File::delete);
        }
        Files.createDirectories(DATA);

        // Topic + broker setup
        final TopicRegistry registry = new TopicRegistry.Builder()
                .topic(TOPIC, EventsProto.OrderCreated.getDescriptor())
                .build();

        final Partitioner partitioner = new RoundRobinPartitioner();
        final Map<Integer, RemoteBrokerClient> clusterNodes = new HashMap<>();
        final InMemoryOffsetStore store = new InMemoryOffsetStore();

        final ClusteredIngress ingress = ClusteredIngress.create(
                registry, partitioner, TOTAL_PARTITIONS, 0, 1,
                clusterNodes, DATA, RING_SIZE, WAIT_STRATEGY,
                SEG_BYTES, WRITER_THREADS, BATCH_SIZE,
                false, store
        );

        final GrpcTransport grpc = new GrpcTransport(9090, ingress, store, registry);
        grpc.start();

        // Build client
        final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9090)
                .usePlaintext().build();
        final BrokerGrpc.BrokerBlockingStub stub = BrokerGrpc.newBlockingStub(channel);

        // Produce
        final EventsProto.OrderCreated msg = EventsProto.OrderCreated.newBuilder()
                .setOrderId("ord-1").setCustomer("bob").build();
        final byte[] payload = msg.toByteArray();

        long written = 0;
        final Instant t0 = Instant.now();
        while (written < TOTAL_MESSAGES) {
            for (int i = 0; i < BATCH_SIZE && written < TOTAL_MESSAGES; i++) {
                final byte[] key = ("key-" + written).getBytes(StandardCharsets.UTF_8);
                ingress.publish(TOPIC, key, 0, payload);
                written++;
            }
        }
        final Instant t1 = Instant.now();
        final double pubSecs = Duration.between(t0, t1).toMillis() / 1000.0;
        log.info(String.format("[PRODUCER] Wrote %d messages in %.2fs = %.1f msgs/s", written, pubSecs, written / pubSecs));

        // Manually commit offset = 0 to start fetching
        for (int pid = 0; pid < TOTAL_PARTITIONS; pid++) {
            stub.commitOffset(BrokerApi.CommitRequest.newBuilder()
                    .setTopic(TOPIC)
                    .setGroup("fetch-benchmark")
                    .setPartition(pid)
                    .setOffset(0)
                    .build());
        }

        // Fetch
        final AtomicLong[] partitionOffsets = new AtomicLong[TOTAL_PARTITIONS];
        for (int pid = 0; pid < TOTAL_PARTITIONS; pid++) {
            final long offset = stub.fetchCommitted(BrokerApi.CommittedRequest.newBuilder()
                    .setTopic(TOPIC)
                    .setGroup("fetch-benchmark")
                    .setPartition(pid).build()).getOffset();
            partitionOffsets[pid] = new AtomicLong(Math.max(offset, 0));
        }

        long totalFetched = 0;
        final Instant f0 = Instant.now();
        while (totalFetched < TOTAL_MESSAGES) {
            for (int pid = 0; pid < TOTAL_PARTITIONS && totalFetched < TOTAL_MESSAGES; pid++) {
                final long offset = partitionOffsets[pid].get();
                final BrokerApi.FetchRequest req = BrokerApi.FetchRequest.newBuilder()
                        .setTopic(TOPIC)
                        .setGroup("fetch-benchmark")
                        .setPartition(pid)
                        .setOffset(offset)
                        .setMaxMessages(1000)
                        .build();

                final BrokerApi.FetchReply rep;
                try {
                    rep = stub.fetch(req);
                } catch (final Exception e) {
                    log.error("Fetch failed from partition " + pid, e);
                    continue;
                }

                if (rep.getMessagesCount() == 0) continue;

                for (final BrokerApi.MessageEvent ev : rep.getMessagesList()) {
                    partitionOffsets[pid].set(ev.getOffset() + 1);
                    totalFetched++;
                }

                stub.commitOffset(BrokerApi.CommitRequest.newBuilder()
                        .setTopic(TOPIC)
                        .setGroup("fetch-benchmark")
                        .setPartition(pid)
                        .setOffset(partitionOffsets[pid].get())
                        .build());
            }
        }

        final Instant f1 = Instant.now();
        final double fetchSecs = Duration.between(f0, f1).toMillis() / 1000.0;
        log.info(String.format("[FETCH-CONSUMER] Delivered %d messages in %.2fs = %.1f msgs/s",
                totalFetched, fetchSecs, totalFetched / fetchSecs));

        grpc.stop();
        channel.shutdownNow();
        log.info("Benchmark complete.");
    }
}
