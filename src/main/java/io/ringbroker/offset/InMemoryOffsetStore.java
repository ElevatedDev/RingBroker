package io.ringbroker.offset;

import io.ringbroker.ledger.constant.LedgerConstant;
import io.ringbroker.ledger.orchestrator.LedgerOrchestrator;
import io.ringbroker.ledger.segment.LedgerSegment;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

/*
 * Hyper-optimized, Durable, Low-Latency OffsetStore.
 *
 * Optimizations:
 * 1. Nested Map + Primitive Array structure for O(1) lookups.
 * 2. Manual Serialization (Little Endian) to avoid allocation.
 * 3. Byte[] Caching for Topics/Groups.
 * 4. VarHandle Array Access for memory visibility.
 */
@Slf4j
public final class InMemoryOffsetStore implements OffsetStore, AutoCloseable {

    private static final int OFFSET_SEGMENT_CAPACITY = 16 * 1024 * 1024;
    private static final int BATCH_SIZE = 1024;
    private static final long PARK_NANOS = 1_000L;

    private final Path storageDir;
    private final ConcurrentHashMap<String, TopicState> topicMap = new ConcurrentHashMap<>();

    /* Cache for UTF-8 bytes to avoid string conversion churn */
    private final ConcurrentHashMap<String, byte[]> topicBytesCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, byte[]> groupBytesCache = new ConcurrentHashMap<>();

    private static final class TopicState {
        final ConcurrentHashMap<String, PartitionOffsets> groups = new ConcurrentHashMap<>();
    }

    private static final class PartitionOffsets {
        private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);

        /* Volatile reference to the array */
        volatile long[] offsets = new long[16];

        /* Lock-free read with memory acquire semantics */
        long get(int partition) {
            long[] arr = offsets;
            if (partition >= 0 && partition < arr.length) {
                return (long) ARRAY_HANDLE.getAcquire(arr, partition);
            }
            return 0L;
        }

        /*
         * Synchronized writer to prevent "Lost Updates" during array resizing.
         * Since commit() is just updating memory, this lock contention is negligible.
         */
        synchronized void set(int partition, long value) {
            if (partition >= offsets.length) {
                grow(partition + 1);
            }
            /* Release semantics ensure readers see this write */
            ARRAY_HANDLE.setRelease(offsets, partition, value);
        }

        private void grow(int minSize) {
            int newSize = offsets.length;
            while (newSize < minSize) {
                newSize <<= 1;
            }
            long[] bigger = new long[newSize];
            /* Copy old data to new array */
            System.arraycopy(offsets, 0, bigger, 0, offsets.length);
            /* Volatile write publishes the new array */
            offsets = bigger;
        }
    }

    private final LedgerOrchestrator wal;
    private final ConcurrentLinkedQueue<byte[]> commitQueue = new ConcurrentLinkedQueue<>();
    private final ExecutorService flusherExecutor = Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name("offset-flusher").factory()
    );
    private final AtomicBoolean running = new AtomicBoolean(true);

    public InMemoryOffsetStore(final Path storageDir) throws IOException {
        this.storageDir = Objects.requireNonNull(storageDir, "storageDir");
        Files.createDirectories(storageDir);

        recoverStateFromDisk();
        this.wal = LedgerOrchestrator.bootstrap(storageDir, OFFSET_SEGMENT_CAPACITY);
        flusherExecutor.submit(this::flusherLoop);
    }

    @Override
    public void commit(final String topic, final String group, final int partition, final long offset) {
        /* 1. Fast In-Memory Update */
        getPartitionOffsets(topic, group).set(partition, offset);

        /* 2. Async Persistence */
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

    private PartitionOffsets getPartitionOffsets(String topic, String group) {
        return topicMap
                .computeIfAbsent(topic, k -> new TopicState())
                .groups
                .computeIfAbsent(group, k -> new PartitionOffsets());
    }

    private byte[] getTopicBytes(String topic) {
        return topicBytesCache.computeIfAbsent(topic, k -> k.getBytes(StandardCharsets.UTF_8));
    }

    private byte[] getGroupBytes(String group) {
        return groupBytesCache.computeIfAbsent(group, k -> k.getBytes(StandardCharsets.UTF_8));
    }

    private void flusherLoop() {
        final List<byte[]> batchBuffer = new ArrayList<>(BATCH_SIZE);
        while (running.get()) {
            try {
                byte[] element = commitQueue.poll();
                if (element == null) {
                    if (!batchBuffer.isEmpty()) flushBatch(batchBuffer);
                    LockSupport.parkNanos(PARK_NANOS);
                    continue;
                }
                batchBuffer.add(element);
                while (batchBuffer.size() < BATCH_SIZE) {
                    element = commitQueue.poll();
                    if (element == null) break;
                    batchBuffer.add(element);
                }
                flushBatch(batchBuffer);
            } catch (final Throwable t) {
                log.error("Offset flusher loop error", t);
            }
        }
        // Drain remaining is handled in close() after loop termination
    }

    private void flushBatch(final List<byte[]> batch) {
        if (batch.isEmpty()) return;

        /* FIX: Calculate exact batch size to request correct segment space */
        int totalBytes = 0;
        for (byte[] b : batch) {
            totalBytes += (8 + b.length); // 8 = LedgerSegment overhead
        }

        try {
            wal.writable(totalBytes).appendBatch(batch);
            batch.clear();
        } catch (final IOException e) {
            log.error("Failed to persist offset batch.", e);
        }
    }

    @Override
    public void close() throws Exception {
        running.set(false);
        flusherExecutor.shutdown();

        /* FIX: Wait for flusher to die before manual draining to avoid race conditions on WAL */
        if (!flusherExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            log.warn("Offset flusher did not terminate cleanly.");
            flusherExecutor.shutdownNow();
        }

        /* Safe to drain now */
        if (!commitQueue.isEmpty()) {
            final List<byte[]> remaining = new ArrayList<>();
            byte[] b;
            while ((b = commitQueue.poll()) != null) remaining.add(b);
            flushBatch(remaining);
        }

        if (wal != null) wal.close();
    }

    /* ========= Recovery Path ========= */

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

            ch.position(LedgerSegment.HEADER_SIZE);
            final ByteBuffer lenBuf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);

            while (ch.position() < fileSize) {
                lenBuf.clear();
                if (ch.read(lenBuf) < Integer.BYTES) break;
                lenBuf.flip();

                final int payloadLen = lenBuf.getInt();
                if (payloadLen == 0) break; // Valid EOF

                ch.position(ch.position() + Integer.BYTES); // Skip CRC

                if (payloadLen < 0 || payloadLen > (fileSize - ch.position())) break;

                final ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLen);
                while (payloadBuf.hasRemaining()) {
                    if (ch.read(payloadBuf) < 0) break;
                }
                if (payloadBuf.hasRemaining()) break;

                payloadBuf.flip();
                deserializeAndUpdate(payloadBuf);
                replayed++;
            }
        } catch (final IOException e) {
            log.warn("Corrupt segment: {}", segmentPath, e);
        }
        return replayed;
    }

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

        // Populate caches during recovery
        topicBytesCache.putIfAbsent(topic, tBytes);
        groupBytesCache.putIfAbsent(group, gBytes);

        getPartitionOffsets(topic, group).set(partition, offset);
    }

    /* ========= Manual Serialization ========= */

    private byte[] serialize(final String topic, final String group, final int partition, final long offset) {
        final byte[] tBytes = getTopicBytes(topic);
        final byte[] gBytes = getGroupBytes(group);

        final int size = 4 + tBytes.length + 4 + gBytes.length + 4 + 8;
        final byte[] out = new byte[size];

        int p = 0;
        p = putIntLE(out, p, tBytes.length);
        System.arraycopy(tBytes, 0, out, p, tBytes.length);
        p += tBytes.length;

        p = putIntLE(out, p, gBytes.length);
        System.arraycopy(gBytes, 0, out, p, gBytes.length);
        p += gBytes.length;

        p = putIntLE(out, p, partition);
        putLongLE(out, p, offset);

        return out;
    }

    private static int putIntLE(byte[] arr, int pos, int value) {
        arr[pos]     = (byte) (value & 0xFF);
        arr[pos + 1] = (byte) ((value >> 8) & 0xFF);
        arr[pos + 2] = (byte) ((value >> 16) & 0xFF);
        arr[pos + 3] = (byte) ((value >> 24) & 0xFF);
        return pos + 4;
    }

    private static void putLongLE(byte[] arr, int pos, long value) {
        arr[pos]     = (byte) (value & 0xFFL);
        arr[pos + 1] = (byte) ((value >> 8) & 0xFFL);
        arr[pos + 2] = (byte) ((value >> 16) & 0xFFL);
        arr[pos + 3] = (byte) ((value >> 24) & 0xFFL);
        arr[pos + 4] = (byte) ((value >> 32) & 0xFFL);
        arr[pos + 5] = (byte) ((value >> 40) & 0xFFL);
        arr[pos + 6] = (byte) ((value >> 48) & 0xFFL);
        arr[pos + 7] = (byte) ((value >> 56) & 0xFFL);
    }
}