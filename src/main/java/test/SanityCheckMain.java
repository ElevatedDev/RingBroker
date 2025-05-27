package test;

import com.google.protobuf.Timestamp;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.cluster.impl.RoundRobinPartitioner;
import io.ringbroker.core.wait.AdaptiveSpin;
import io.ringbroker.offset.InMemoryOffsetStore;
import io.ringbroker.proto.test.EventsProto;
import io.ringbroker.registry.TopicRegistry;
import lombok.extern.slf4j.Slf4j;

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
 *  – publish 1 000 OrderCreated events to a 16-partition single-node broker
 *  – verify the in-memory subscription sees every record
 *  – replay every segment on disk and confirm the same IDs partition-by-partition
 */
@Slf4j
public class SanityCheckMain {

    /* ---------- test parameters ---------- */

    private static final int  PARTITIONS     = 16;
    private static final int  WRITER_THREADS = 1;
    private static final int  BATCH_SIZE     = 100;
    private static final int  TOTAL_MSGS     = 1_000;
    private static final long SEGMENT_BYTES  = 128L << 20;   // 128 MiB

    private static final String TOPIC = "orders/created";
    private static final String GROUP = "sanity-latch";
    private static final Path   DATA  = Paths.get("data");

    public static void main(String[] args) throws Exception {
        if (Files.exists(DATA)) {
            try (Stream<Path> w = Files.walk(DATA)) {
                w.sorted(Comparator.reverseOrder())
                        .map(Path::toFile).forEach(File::delete);
            }
        }
        Files.createDirectories(DATA);

        TopicRegistry registry = new TopicRegistry.Builder()
                .topic(TOPIC, EventsProto.OrderCreated.getDescriptor())
                .build();

        ClusteredIngress ingress = ClusteredIngress.create(
                registry,
                new RoundRobinPartitioner(),
                PARTITIONS,
                /* nodeId   */ 0,
                /* cluster  */ 1,                // single-node
                Collections.emptyMap(),
                DATA,
                1 << 20,                         // flushBytes
                new AdaptiveSpin(),
                SEGMENT_BYTES,
                BATCH_SIZE,
                /* flushOnWrite */ false,
                new InMemoryOffsetStore()
        );

        RoundRobinPartitioner psel = new RoundRobinPartitioner();
        Map<Integer, Set<String>> expected = new HashMap<>();
        for (int p = 0; p < PARTITIONS; p++) expected.put(p, new HashSet<>());

        CountDownLatch latch = new CountDownLatch(TOTAL_MSGS);
        ingress.subscribeTopic(TOPIC, GROUP, (seq, payload) -> latch.countDown());

        log.info("=== publishing {} messages ===", TOTAL_MSGS);
        for (int i = 0; i < TOTAL_MSGS; i++) {
            String id  = "msg-" + i;
            byte[] key = id.getBytes(StandardCharsets.UTF_8);

            expected.get(psel.selectPartition(key, PARTITIONS)).add(id);

            var evt = EventsProto.OrderCreated.newBuilder()
                    .setOrderId(id)
                    .setCustomer("sanity")
                    .setCreatedAt(Timestamp.getDefaultInstance())
                    .build();

            ingress.publish(TOPIC, key, 0, evt.toByteArray());
        }

        log.info("waiting for in-memory delivery…");
        if (!latch.await(30, TimeUnit.SECONDS)) {
            log.error("saw only {}/{} messages", TOTAL_MSGS - latch.getCount(), TOTAL_MSGS);
            System.exit(1);
        }
        log.info("all messages delivered in-memory");

        ingress.shutdown();

        Map<Integer, Set<String>> seen = new HashMap<>();
        for (int p = 0; p < PARTITIONS; p++) {
            seen.put(p, new HashSet<>());
            Path dir = DATA.resolve("partition-" + p);
            if (!Files.isDirectory(dir)) continue;

            try (Stream<Path> segs = Files.list(dir)) {
                for (Path seg : segs.filter(f -> f.toString().endsWith(".seg")).sorted().toList()) {
                    parseSegmentLittleEndian(seg, seen.get(p));
                }
            }
        }

        boolean pass = true;
        log.info("\n=== partition results ===");
        for (int p = 0; p < PARTITIONS; p++) {
            Set<String> exp = expected.get(p), got = seen.get(p);
            log.info("partition-{}: exp={}, got={}", p, exp.size(), got.size());
            if (!exp.equals(got)) {
                log.error("  missing: {}", diff(exp, got));
                log.error("  extra  : {}", diff(got, exp));
                pass = false;
            }
        }

        if (pass) {
            log.info("\nSANITY-PASS: all routing + writes succeeded.");
        } else {
            log.error("\nSANITY-FAIL: see mismatches above.");
            System.exit(1);
        }
    }

    /** read a 32-bit little-endian int from the stream */
    private static int readIntLE(DataInputStream in) throws IOException {
        int b0 = in.readUnsignedByte();
        int b1 = in.readUnsignedByte();
        int b2 = in.readUnsignedByte();
        int b3 = in.readUnsignedByte();
        return  (b3 << 24) | (b2 << 16) | (b1 << 8) | b0;
    }

    /** parse one segment file and add every OrderId to outSet */
    private static void parseSegmentLittleEndian(Path seg, Set<String> outSet) throws IOException {
        try (FileChannel ch = FileChannel.open(seg, StandardOpenOption.READ);
             DataInputStream in = new DataInputStream(Channels.newInputStream(ch))) {

            in.skipBytes(26);                          // header (magic+CRC etc.)

            while (true) {
                int len;
                try { len = readIntLE(in); }           // length (LE)
                catch (EOFException e) { break; }

                if (len <= 0) break;                   // end-padding

                int storedCrc = readIntLE(in);         // crc  (LE)
                byte[] buf    = in.readNBytes(len);
                if (buf.length < len) break;           // truncated (shouldn’t)

                CRC32 crc = new CRC32();
                crc.update(buf, 0, len);
                if ((int) crc.getValue() != storedCrc) {
                    log.error("CRC mismatch in {}", seg.getFileName());
                    break;
                }

                var evt = EventsProto.OrderCreated.parseFrom(buf);
                outSet.add(evt.getOrderId());
            }
        }
    }

    private static Set<String> diff(Set<String> a, Set<String> b) {
        Set<String> d = new TreeSet<>(a); d.removeAll(b); return d;
    }
}
