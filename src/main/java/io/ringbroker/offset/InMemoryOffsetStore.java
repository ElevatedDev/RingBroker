package io.ringbroker.offset;

import io.ringbroker.ledger.constant.LedgerConstant;
import io.ringbroker.ledger.orchestrator.LedgerOrchestrator;
import io.ringbroker.ledger.segment.LedgerSegment;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

/*
 * Hyper-optimized, Durable, Low-Latency OffsetStore backed by the LedgerOrchestrator.
 *
 * Hot-path goals:
 *  - commit(): O(1) with minimal allocations & string work
 *  - fetch(): O(1) with simple nested map + array read
 */
@Slf4j
public final class InMemoryOffsetStore implements OffsetStore, AutoCloseable {

    /* 16MB segments as before. */
    private static final int OFFSET_SEGMENT_CAPACITY = 16 * 1024 * 1024;

    /* Batch size for WAL appends. Tune as needed. */
    private static final int BATCH_SIZE = 1024;

    /* Idle park duration for flusher. 1 microsecond. */
    private static final long PARK_NANOS = 1_000L;

    private final Path storageDir;

    /*
     * In-memory structure:
     *
     * topicMap: topic -> TopicState
     * TopicState.groups: group -> PartitionOffsets
     * PartitionOffsets.offsets: long[] indexed by partition
     *
     * This avoids string concatenation for keys and per-call boxing.
     */
    private static final class TopicState {
        final ConcurrentHashMap<String, PartitionOffsets> groups = new ConcurrentHashMap<>();
    }

    private static final class PartitionOffsets {
        // Volatile to ensure visibility when we resize.
        volatile long[] offsets = new long[16];

        long get(int partition) {
            long[] arr = offsets;
            return (partition >= 0 && partition < arr.length) ? arr[partition] : 0L;
        }

        void set(int partition, long value) {
            long[] arr = offsets;
            if (partition >= arr.length) {
                growToAtLeast(partition + 1);
                arr = offsets;
            }
            // Plain write is fine; visibility is eventually guaranteed and
            // read-your-writes holds for the calling thread.
            arr[partition] = value;
        }

        private synchronized void growToAtLeast(int minSize) {
            long[] current = offsets;
            if (current.length >= minSize) return;
            int newSize = current.length;
            while (newSize < minSize) {
                newSize <<= 1;
            }
            long[] bigger = new long[newSize];
            System.arraycopy(current, 0, bigger, 0, current.length);
            offsets = bigger;
        }
    }

    /* topic -> TopicState */
    private final ConcurrentHashMap<String, TopicState> topicMap = new ConcurrentHashMap<>();

    /*
     * Caches for UTF-8 bytes of topic and group names to avoid repeated String.getBytes().
     */
    private final ConcurrentHashMap<String, byte[]> topicBytesCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, byte[]> groupBytesCache = new ConcurrentHashMap<>();

    private TopicState topicState(final String topic) {
        TopicState ts = topicMap.get(topic);
        if (ts != null) return ts;
        TopicState fresh = new TopicState();
        TopicState existing = topicMap.putIfAbsent(topic, fresh);
        return existing != null ? existing : fresh;
    }

    private PartitionOffsets partitionOffsets(final String topic, final String group) {
        TopicState ts = topicState(topic);
        PartitionOffsets po = ts.groups.get(group);
        if (po != null) return po;
        PartitionOffsets fresh = new PartitionOffsets();
        PartitionOffsets existing = ts.groups.putIfAbsent(group, fresh);
        return existing != null ? existing : fresh;
    }

    private byte[] topicBytes(final String topic) {
        byte[] cached = topicBytesCache.get(topic);
        if (cached != null) return cached;
        byte[] fresh = topic.getBytes(StandardCharsets.UTF_8);
        byte[] existing = topicBytesCache.putIfAbsent(topic, fresh);
        return existing != null ? existing : fresh;
    }

    private byte[] groupBytes(final String group) {
        byte[] cached = groupBytesCache.get(group);
        if (cached != null) return cached;
        byte[] fresh = group.getBytes(StandardCharsets.UTF_8);
        byte[] existing = groupBytesCache.putIfAbsent(group, fresh);
        return existing != null ? existing : fresh;
    }

    private final LedgerOrchestrator wal;

    /*
     * MPSC queue for commits.
     */
    private final ConcurrentLinkedQueue<byte[]> commitQueue = new ConcurrentLinkedQueue<>();

    private final ExecutorService flusherExecutor = Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name("offset-flusher").factory()
    );

    private final AtomicBoolean running = new AtomicBoolean(true);

    public InMemoryOffsetStore(final Path storageDir) throws IOException {
        this.storageDir = Objects.requireNonNull(storageDir, "storageDir");
        Files.createDirectories(storageDir);

        // Phase 1: recovery from existing segments.
        recoverStateFromDisk();

        // Phase 2: WAL bootstrap.
        this.wal = LedgerOrchestrator.bootstrap(storageDir, OFFSET_SEGMENT_CAPACITY);

        // Phase 3: start flusher loop.
        flusherExecutor.submit(this::flusherLoop);
    }

    @Override
    public void commit(final String topic, final String group, final int partition, final long offset) {
        // Fast in-memory update: nested map + array write.
        PartitionOffsets po = partitionOffsets(topic, group);
        po.set(partition, offset);

        // Serialize for async WAL persistence.
        final byte[] payload = serialize(topic, group, partition, offset);
        commitQueue.offer(payload);
    }

    @Override
    public long fetch(final String topic, final String group, final int partition) {
        TopicState ts = topicMap.get(topic);
        if (ts == null) return 0L;
        PartitionOffsets po = ts.groups.get(group);
        if (po == null) return 0L;
        return po.get(partition);
    }

    /*
     * Background flush loop: drain queue, batch, append to WAL.
     */
    private void flusherLoop() {
        final List<byte[]> batchBuffer = new ArrayList<>(BATCH_SIZE);

        while (running.get()) {
            try {
                byte[] element = commitQueue.poll();

                if (element == null) {
                    // Flush any accumulated batch before idling.
                    if (!batchBuffer.isEmpty()) {
                        flushBatch(batchBuffer);
                    }
                    LockSupport.parkNanos(PARK_NANOS);
                    continue;
                }

                batchBuffer.add(element);

                // Greedy drain up to BATCH_SIZE.
                while (batchBuffer.size() < BATCH_SIZE) {
                    element = commitQueue.poll();
                    if (element == null) break;
                    batchBuffer.add(element);
                }

                flushBatch(batchBuffer);
            } catch (final Throwable t) {
                log.error("Offset flusher loop encountered error", t);
            }
        }

        // Final drain when running flag is cleared.
        try {
            if (!commitQueue.isEmpty()) {
                final List<byte[]> remaining = new ArrayList<>();
                byte[] b;
                while ((b = commitQueue.poll()) != null) {
                    remaining.add(b);
                    if (remaining.size() >= BATCH_SIZE) {
                        flushBatch(remaining);
                    }
                }
                if (!remaining.isEmpty()) {
                    flushBatch(remaining);
                }
            }
        } catch (final Throwable t) {
            log.error("Error while flushing remaining offsets on flusher shutdown", t);
        }
    }

    private void flushBatch(final List<byte[]> batch) {
        if (batch.isEmpty()) return;

        int totalBytes = 0;
        for (byte[] b : batch) {
            totalBytes += (8 + b.length);
        }

        try {
            wal.writable(totalBytes).appendBatch(batch, totalBytes);
            batch.clear();
        } catch (final IOException e) {
            log.error("Failed to persist offset batch.", e);
        }
    }

    @Override
    public void close() throws Exception {
        // Stop flusher loop.
        running.set(false);
        flusherExecutor.shutdown();
        try {
            if (!flusherExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Offset flusher executor did not terminate within 30s");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        // WAL close.
        wal.close();
    }

    private void recoverStateFromDisk() throws IOException {
        log.info("Recovering offsets from: {}", storageDir);
        try (final Stream<Path> files = Files.list(storageDir)) {
            final List<Path> segments = files
                    .filter(p -> p.toString().endsWith(LedgerConstant.SEGMENT_EXT))
                    .sorted(Comparator.comparing(Path::getFileName))
                    .toList();

            long count = 0;
            for (final Path segment : segments) {
                count += replaySegment(segment);
            }
            log.info("Offset recovery complete. Replayed {} commits.", count);
        }
    }

    private int replaySegment(final Path segmentPath) {
        int replayed = 0;

        try (final FileChannel ch = FileChannel.open(segmentPath, StandardOpenOption.READ)) {
            final long fileSize = ch.size();
            if (fileSize < LedgerSegment.HEADER_SIZE) return 0;

            // Skip segment header in one shot.
            ch.position(LedgerSegment.HEADER_SIZE);

            final ByteBuffer lenBuf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);

            while (ch.position() < fileSize) {
                lenBuf.clear();
                int n = ch.read(lenBuf);
                if (n < Integer.BYTES) break;
                lenBuf.flip();

                final int payloadLen = lenBuf.getInt();
                if (payloadLen == 0) break; // padding / EOF

                // Skip CRC (4 bytes).
                ch.position(ch.position() + Integer.BYTES);

                if (payloadLen < 0 || payloadLen > (fileSize - ch.position())) {
                    // Bogus length, stop replaying this segment.
                    break;
                }

                final ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLen);
                while (payloadBuf.hasRemaining()) {
                    int r = ch.read(payloadBuf);
                    if (r < 0) {
                        // Torn record; stop.
                        break;
                    }
                }
                if (payloadBuf.hasRemaining()) {
                    // Incomplete / torn record.
                    break;
                }
                payloadBuf.flip();

                deserializeAndUpdate(payloadBuf);
                replayed++;
            }
        } catch (final IOException e) {
            log.warn("Corrupt or partial segment found during recovery: {}", segmentPath, e);
        }
        return replayed;
    }

    /*
     * Payload format (LE):
     * [tLen:int][tBytes][gLen:int][gBytes][partition:int][offset:long]
     */
    private void deserializeAndUpdate(final ByteBuffer buf) {
        buf.order(ByteOrder.LITTLE_ENDIAN);

        final int tLen = buf.getInt();
        final byte[] tBytes = new byte[tLen];
        buf.get(tBytes);

        final int gLen = buf.getInt();
        final byte[] gBytes = new byte[gLen];
        buf.get(gBytes);

        final int partition = buf.getInt();
        final long offset = buf.getLong();

        final String topic = new String(tBytes, StandardCharsets.UTF_8);
        final String group = new String(gBytes, StandardCharsets.UTF_8);

        // Warm caches so we don't recompute bytes for these strings later.
        topicBytes(topic);
        groupBytes(group);

        PartitionOffsets po = partitionOffsets(topic, group);
        po.set(partition, offset);
    }

    private byte[] serialize(final String topic, final String group, final int partition, final long offset) {
        final byte[] tBytes = topicBytes(topic);
        final byte[] gBytes = groupBytes(group);

        final int size =
                4 + tBytes.length + // topic length
                        4 + gBytes.length + // group length
                        4 +                 // partition
                        8;                  // offset

        final byte[] out = new byte[size];
        int p = 0;

        p = putIntLE(out, p, tBytes.length);
        System.arraycopy(tBytes, 0, out, p, tBytes.length);
        p += tBytes.length;

        p = putIntLE(out, p, gBytes.length);
        System.arraycopy(gBytes, 0, out, p, gBytes.length);
        p += gBytes.length;

        p = putIntLE(out, p, partition);
        p = putLongLE(out, p, offset);

        return out;
    }

    private static int putIntLE(byte[] arr, int pos, int value) {
        arr[pos    ] = (byte) (value       & 0xFF);
        arr[pos + 1] = (byte) ((value >> 8)  & 0xFF);
        arr[pos + 2] = (byte) ((value >> 16) & 0xFF);
        arr[pos + 3] = (byte) ((value >> 24) & 0xFF);
        return pos + 4;
    }

    private static int putLongLE(byte[] arr, int pos, long value) {
        arr[pos    ] = (byte) (value       & 0xFFL);
        arr[pos + 1] = (byte) ((value >> 8)  & 0xFFL);
        arr[pos + 2] = (byte) ((value >> 16) & 0xFFL);
        arr[pos + 3] = (byte) ((value >> 24) & 0xFFL);
        arr[pos + 4] = (byte) ((value >> 32) & 0xFFL);
        arr[pos + 5] = (byte) ((value >> 40) & 0xFFL);
        arr[pos + 6] = (byte) ((value >> 48) & 0xFFL);
        arr[pos + 7] = (byte) ((value >> 56) & 0xFFL);
        return pos + 8;
    }
}
