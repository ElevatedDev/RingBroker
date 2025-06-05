package io.ringbroker.test;

import com.google.protobuf.Timestamp;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.membership.replicator.FlashReplicator;
import io.ringbroker.cluster.membership.resolver.ReplicaSetResolver;
import io.ringbroker.cluster.partitioner.impl.RoundRobinPartitioner;
import io.ringbroker.core.wait.AdaptiveSpin;
import io.ringbroker.offset.InMemoryOffsetStore;
import io.ringbroker.proto.test.EventsProto;
import io.ringbroker.registry.TopicRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end smoke test:
 * – publish 1 000 OrderCreated events to a 16-partition single-node broker
 * – verify the in-memory subscription sees every record
 * – replay every segment on disk and confirm the same IDs partition-by-partition
 */
class SanityCheckMain {

    private static final int PARTITIONS = 16;
    private static final int WRITER_THREADS = 1;
    private static final int BATCH_SIZE = 100;
    private static final int TOTAL_MSGS = 1_000;
    private static final long SEGMENT_BYTES = 128L << 20;   // 128 MiB

    private static final String TOPIC = "orders/created";
    private static final String GROUP = "sanity-latch";

    /**
     * read a 32-bit little-endian int from the stream
     */
    private static int readIntLE(final DataInputStream in) throws IOException {
        final int b0 = in.readUnsignedByte();
        final int b1 = in.readUnsignedByte();
        final int b2 = in.readUnsignedByte();
        final int b3 = in.readUnsignedByte();
        return (b3 << 24) | (b2 << 16) | (b1 << 8) | b0;
    }

    /**
     * parse one segment file and add every OrderId to outSet
     */
    private static void parseSegmentLittleEndian(final Path seg, final Set<String> outSet) throws IOException {
        try (final FileChannel channel = FileChannel.open(seg, StandardOpenOption.READ);
             final DataInputStream input = new DataInputStream(Channels.newInputStream(channel))) {
            input.skipBytes(26);   // header (magic+CRC etc.)

            while (true) {
                final int len;

                try {
                    len = readIntLE(input);
                } catch (final EOFException e) {
                    break;
                }

                if (len <= 0) break;    // end-padding

                final int storedCrc = readIntLE(input);    // crc  (LE)
                final byte[] buffer = input.readNBytes(len);

                if (buffer.length < len) break;    // truncated (shouldn’t)

                final CRC32 crc = new CRC32();
                crc.update(buffer, 0, len);

                if ((int) crc.getValue() != storedCrc) {
                    fail("CRC mismatch in " + seg.getFileName());
                    break;
                }

                final var event = EventsProto.OrderCreated.parseFrom(buffer);

                outSet.add(event.getOrderId());
            }
        }
    }

    private static Set<String> diff(final Set<String> a, final Set<String> b) {
        final Set<String> d = new TreeSet<>(a);
        d.removeAll(b);
        return d;
    }

    @Test
    void endToEndSanityTest(@TempDir final Path tempDir) throws Exception {
        final Path dataDir = tempDir.resolve("data");

        Files.createDirectories(dataDir);

        final TopicRegistry registry = new TopicRegistry.Builder()
                .topic(TOPIC, EventsProto.OrderCreated.getDescriptor())
                .build();

        final ReplicaSetResolver resolver = new ReplicaSetResolver(
                1,
                List::of);

        final FlashReplicator replicator = new FlashReplicator(
                1,
                Map.of(),
                -1);

        final ClusteredIngress ingress = ClusteredIngress.create(
                registry,
                new RoundRobinPartitioner(),
                PARTITIONS,
                /* nodeId   */ 0,
                /* cluster  */ 1,   // single-node
                Collections.emptyMap(),
                dataDir,
                1 << 20,    // flushBytes
                new AdaptiveSpin(),
                SEGMENT_BYTES,
                BATCH_SIZE,
                /* flushOnWrite */ false,
                new InMemoryOffsetStore(),
                BrokerRole.PERSISTENCE,
                resolver,
                replicator
        );

        final RoundRobinPartitioner psel = new RoundRobinPartitioner();
        final Map<Integer, Set<String>> expected = new HashMap<>();

        for (int partition = 0; partition < PARTITIONS; partition++) expected.put(partition, new HashSet<>());

        final CountDownLatch latch = new CountDownLatch(TOTAL_MSGS);
        ingress.subscribeTopic(TOPIC, GROUP, (seq, payload) -> latch.countDown());

        for (int i = 0; i < TOTAL_MSGS; i++) {
            final String id = "msg-" + i;
            final byte[] key = id.getBytes(StandardCharsets.UTF_8);

            expected.get(psel.selectPartition(key, PARTITIONS)).add(id);

            final var event = EventsProto.OrderCreated.newBuilder()
                    .setOrderId(id)
                    .setCustomer("sanity")
                    .setCreatedAt(Timestamp.getDefaultInstance())
                    .build();

            ingress.publish(TOPIC, key, event.toByteArray());
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS),
                () -> String.format("saw only %d/%d messages", TOTAL_MSGS - latch.getCount(), TOTAL_MSGS));

        ingress.shutdown();

        final Map<Integer, Set<String>> seen = new HashMap<>();

        for (int partition = 0; partition < PARTITIONS; partition++) {
            seen.put(partition, new HashSet<>());

            final Path dir = dataDir.resolve("partition-" + partition);

            if (!Files.isDirectory(dir)) continue;

            try (final Stream<Path> segs = Files.list(dir)) {
                for (final Path seg : segs.filter(f -> f.toString().endsWith(".seg")).sorted().toList()) {
                    parseSegmentLittleEndian(seg, seen.get(partition));
                }
            }
        }

        for (int p = 0; p < PARTITIONS; p++) {
            final Set<String> exp = expected.get(p);
            final Set<String> got = seen.get(p);

            assertEquals(exp, got, "Partition " + p + " mismatch.\n" +
                    "Missing: " + diff(exp, got) + "\nExtra: " + diff(got, exp));
        }

    }
}
