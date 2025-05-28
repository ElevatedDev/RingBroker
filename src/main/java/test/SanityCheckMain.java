package test;

import com.google.protobuf.Timestamp;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.cluster.partitioner.impl.RoundRobinPartitioner;
import io.ringbroker.core.wait.AdaptiveSpin;
import io.ringbroker.offset.InMemoryOffsetStore;
import io.ringbroker.proto.test.EventsProto;
import io.ringbroker.registry.TopicRegistry;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.CRC32;

/**
 * End-to-end smoke test:
 * – publish 1 000 OrderCreated events to a 16-partition single-node broker
 * – verify the in-memory subscription sees every record
 * – replay every segment on disk and confirm the same IDs partition-by-partition
 */
public class SanityCheckMain {

    /* ---------- test parameters ---------- */

    private static final int PARTITIONS = 16;
    private static final int WRITER_THREADS = 1;
    private static final int BATCH_SIZE = 100;
    private static final int TOTAL_MSGS = 1_000;
    private static final long SEGMENT_BYTES = 128L << 20;   // 128 MiB

    private static final String TOPIC = "orders/created";
    private static final String GROUP = "sanity-latch";
    private static final Path DATA = Paths.get("data");

    /* ---------- main ---------- */

    public static void main(final String[] args) throws Exception {
        /* 0) clean data dir ------------------------------------------------- */
        if (Files.exists(DATA)) {
            try (final Stream<Path> w = Files.walk(DATA)) {
                w.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }

        Files.createDirectories(DATA);

        /* 1) registry + broker --------------------------------------------- */
        final TopicRegistry registry = new TopicRegistry.Builder()
                .topic(TOPIC, EventsProto.OrderCreated.getDescriptor())
                .build();

        final ClusteredIngress ingress = ClusteredIngress.create(
                registry,
                new RoundRobinPartitioner(),
                PARTITIONS,
                /* nodeId   */ 0,
                /* cluster  */ 1,   // single-node
                Collections.emptyMap(),
                DATA,
                1 << 20,    // flushBytes
                new AdaptiveSpin(),
                SEGMENT_BYTES,
                BATCH_SIZE,
                /* flushOnWrite */ false,
                new InMemoryOffsetStore()
        );

        /* expected IDs per partition --------------------------------------- */
        final RoundRobinPartitioner psel = new RoundRobinPartitioner();
        final Map<Integer, Set<String>> expected = new HashMap<>();

        for (int p = 0; p < PARTITIONS; p++) expected.put(p, new HashSet<>());

        /* latch for subscriber --------------------------------------------- */
        final CountDownLatch latch = new CountDownLatch(TOTAL_MSGS);
        ingress.subscribeTopic(TOPIC, GROUP, (seq, payload) -> latch.countDown());

        /* 2) publish ------------------------------------------------------- */
        System.out.println("=== publishing " + TOTAL_MSGS + " messages ===");
        for (int i = 0; i < TOTAL_MSGS; i++) {
            final String id = "msg-" + i;
            final byte[] key = id.getBytes(StandardCharsets.UTF_8);

            expected.get(psel.selectPartition(key, PARTITIONS)).add(id);

            final var evt = EventsProto.OrderCreated.newBuilder()
                    .setOrderId(id)
                    .setCustomer("sanity")
                    .setCreatedAt(Timestamp.getDefaultInstance())
                    .build();

            ingress.publish(TOPIC, key, 0, evt.toByteArray());
        }

        /* writers push tail batches immediately (patch already in place)    */

        /* 3) wait for subscriber ------------------------------------------- */
        System.out.println("waiting for in-memory delivery…");

        if (!latch.await(30, TimeUnit.SECONDS)) {
            System.err.printf("❌ saw only %d/%d messages%n",
                    TOTAL_MSGS - latch.getCount(), TOTAL_MSGS);

            System.exit(1);
        }

        System.out.println("✅ all messages delivered in-memory");

        /* 4) shutdown (forces fsync) --------------------------------------- */
        ingress.shutdown();

        /* 5) replay segments per partition --------------------------------- */
        final Map<Integer, Set<String>> seen = new HashMap<>();

        for (int p = 0; p < PARTITIONS; p++) {
            seen.put(p, new HashSet<>());

            final Path dir = DATA.resolve("partition-" + p);

            if (!Files.isDirectory(dir)) continue;

            try (final Stream<Path> segs = Files.list(dir)) {
                for (final Path seg : segs.filter(f -> f.toString().endsWith(".seg")).sorted().toList()) {
                    parseSegmentLittleEndian(seg, seen.get(p));
                }
            }
        }

        /* 6) compare ------------------------------------------------------- */
        boolean pass = true;

        System.out.println("\n=== partition results ===");

        for (int p = 0; p < PARTITIONS; p++) {
            final Set<String> exp = expected.get(p);
            final Set<String> got = seen.get(p);

            System.out.printf("partition-%2d: exp=%3d, got=%3d%n", p, exp.size(), got.size());

            if (!exp.equals(got)) {
                pass = false;

                System.err.println("  missing: " + diff(exp, got));
                System.err.println("  extra  : " + diff(got, exp));
            }
        }

        System.out.println(pass
                ? "\n✅ SANITY-PASS: all routing + writes succeeded."
                : "\n❌ SANITY-FAIL: see mismatches above.");

        if (!pass) System.exit(1);
    }

    /* ---------- helpers --------------------------------------------------- */

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
        try (final FileChannel ch = FileChannel.open(seg, StandardOpenOption.READ);
             final DataInputStream in = new DataInputStream(Channels.newInputStream(ch))) {

            in.skipBytes(26);   // header (magic+CRC etc.)

            while (true) {
                final int len;

                try {
                    len = readIntLE(in);
                } catch (final EOFException e) {
                    break;
                }

                if (len <= 0) break;    // end-padding

                final int storedCrc = readIntLE(in);    // crc  (LE)
                final byte[] buf = in.readNBytes(len);

                if (buf.length < len) break;    // truncated (shouldn’t)

                final CRC32 crc = new CRC32();

                crc.update(buf, 0, len);

                if ((int) crc.getValue() != storedCrc) {
                    System.err.printf("CRC mismatch in %s%n", seg.getFileName());
                    break;
                }

                final var evt = EventsProto.OrderCreated.parseFrom(buf);
                outSet.add(evt.getOrderId());
            }
        }
    }

    private static Set<String> diff(final Set<String> a, final Set<String> b) {
        final Set<String> d = new TreeSet<>(a);
        d.removeAll(b);
        return d;
    }
}
