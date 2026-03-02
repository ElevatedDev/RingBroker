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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

/** Durable in-memory offset store backed by a WAL. */
@Slf4j
public final class InMemoryOffsetStore implements OffsetStore, AutoCloseable {

    /* 16MB segments as before. */
    private static final int OFFSET_SEGMENT_CAPACITY = 16 * 1024 * 1024;

    /* Batch size for WAL appends. Tune as needed. */
    private static final int BATCH_SIZE = 1024;

    /* Idle park duration for flusher. 1 microsecond. */
    private static final long PARK_NANOS = 1_000L;
    private static final int INITIAL_FRAMED_BATCH_CAPACITY = 1 << 20; // 1MB
    private static final int MAX_COMMIT_POOL_SIZE = BATCH_SIZE * 8;

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

        long get(final int partition) {
            final long[] arr = offsets;
            return (partition >= 0 && partition < arr.length) ? arr[partition] : 0L;
        }

        void set(final int partition, final long value) {
            long[] arr = offsets;
            if (partition >= arr.length) {
                growToAtLeast(partition + 1);
                arr = offsets;
            }
            // Plain write is fine; visibility is eventually guaranteed and
            // read-your-writes holds for the calling thread.
            arr[partition] = value;
        }

        private synchronized void growToAtLeast(final int minSize) {
            final long[] current = offsets;
            if (current.length >= minSize) return;
            int newSize = current.length;
            while (newSize < minSize) {
                newSize <<= 1;
            }
            final long[] bigger = new long[newSize];
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
        final TopicState ts = topicMap.get(topic);
        if (ts != null) return ts;
        final TopicState fresh = new TopicState();
        final TopicState existing = topicMap.putIfAbsent(topic, fresh);
        return existing != null ? existing : fresh;
    }

    private PartitionOffsets partitionOffsets(final String topic, final String group) {
        final TopicState ts = topicState(topic);
        final PartitionOffsets po = ts.groups.get(group);
        if (po != null) return po;
        final PartitionOffsets fresh = new PartitionOffsets();
        final PartitionOffsets existing = ts.groups.putIfAbsent(group, fresh);
        return existing != null ? existing : fresh;
    }

    private byte[] topicBytes(final String topic) {
        final byte[] cached = topicBytesCache.get(topic);
        if (cached != null) return cached;
        final byte[] fresh = topic.getBytes(StandardCharsets.UTF_8);
        final byte[] existing = topicBytesCache.putIfAbsent(topic, fresh);
        return existing != null ? existing : fresh;
    }

    private byte[] groupBytes(final String group) {
        final byte[] cached = groupBytesCache.get(group);
        if (cached != null) return cached;
        final byte[] fresh = group.getBytes(StandardCharsets.UTF_8);
        final byte[] existing = groupBytesCache.putIfAbsent(group, fresh);
        return existing != null ? existing : fresh;
    }

    private final LedgerOrchestrator wal;

    private static final class PendingCommit {
        byte[] topicBytes;
        byte[] groupBytes;
        int partition;
        long offset;

        void set(final byte[] topicBytes, final byte[] groupBytes, final int partition, final long offset) {
            this.topicBytes = topicBytes;
            this.groupBytes = groupBytes;
            this.partition = partition;
            this.offset = offset;
        }

        void clear() {
            this.topicBytes = null;
            this.groupBytes = null;
            this.partition = 0;
            this.offset = 0L;
        }
    }

    /*
     * MPSC queue for commits.
     */
    private final ConcurrentLinkedQueue<PendingCommit> commitQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<PendingCommit> commitPool = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pooledCommitCount = new AtomicInteger(0);

    private final ExecutorService flusherExecutor = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().name("offset-flusher").factory()
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
        final PartitionOffsets po = partitionOffsets(topic, group);
        po.set(partition, offset);

        final PendingCommit c = acquireCommit();
        c.set(topicBytes(topic), groupBytes(group), partition, offset);
        commitQueue.offer(c);
    }

    @Override
    public long fetch(final String topic, final String group, final int partition) {
        final TopicState ts = topicMap.get(topic);
        if (ts == null) return 0L;
        final PartitionOffsets po = ts.groups.get(group);
        if (po == null) return 0L;
        return po.get(partition);
    }

    /*
     * Background flush loop: drain queue, batch, append to WAL.
     */
    private void flusherLoop() {
        final PendingCommit[] batch = new PendingCommit[BATCH_SIZE];
        int batchCount = 0;
        ByteBuffer framedBatch = ByteBuffer.allocateDirect(INITIAL_FRAMED_BATCH_CAPACITY)
                .order(ByteOrder.LITTLE_ENDIAN);

        while (running.get()) {
            try {
                PendingCommit element = commitQueue.poll();

                if (element == null) {
                    if (batchCount > 0) {
                        framedBatch = flushBatch(batch, batchCount, framedBatch);
                        recycleBatch(batch, batchCount);
                        batchCount = 0;
                    }
                    LockSupport.parkNanos(PARK_NANOS);
                    continue;
                }

                batch[batchCount++] = element;

                // Greedy drain up to BATCH_SIZE.
                while (batchCount < BATCH_SIZE) {
                    element = commitQueue.poll();
                    if (element == null) break;
                    batch[batchCount++] = element;
                }

                framedBatch = flushBatch(batch, batchCount, framedBatch);
                recycleBatch(batch, batchCount);
                batchCount = 0;
            } catch (final Throwable t) {
                log.error("Offset flusher loop encountered error", t);
                LockSupport.parkNanos(PARK_NANOS);
            }
        }

        // Final drain when running flag is cleared.
        try {
            for (;;) {
                while (batchCount < BATCH_SIZE) {
                    final PendingCommit c = commitQueue.poll();
                    if (c == null) break;
                    batch[batchCount++] = c;
                }
                if (batchCount == 0) break;
                framedBatch = flushBatch(batch, batchCount, framedBatch);
                recycleBatch(batch, batchCount);
                batchCount = 0;
                if (commitQueue.isEmpty()) {
                    break;
                }
            }
        } catch (final Throwable t) {
            log.error("Error while flushing remaining offsets on flusher shutdown", t);
        }
    }

    private ByteBuffer flushBatch(final PendingCommit[] batch,
                                  final int count,
                                  final ByteBuffer currentFramedBuffer) throws IOException {
        if (count <= 0) return currentFramedBuffer;

        int framedBytes = 0;
        for (int i = 0; i < count; i++) {
            framedBytes = Math.addExact(framedBytes, Integer.BYTES + payloadSize(batch[i]));
        }

        final ByteBuffer framed = ensureFramedCapacity(currentFramedBuffer, framedBytes);
        framed.clear();

        for (int i = 0; i < count; i++) {
            final PendingCommit c = batch[i];
            final int payloadLen = payloadSize(c);

            framed.putInt(payloadLen);
            framed.putInt(c.topicBytes.length);
            framed.put(c.topicBytes);
            framed.putInt(c.groupBytes.length);
            framed.put(c.groupBytes);
            framed.putInt(c.partition);
            framed.putLong(c.offset);
        }

        framed.flip();
        int remaining = count;
        while (remaining > 0) {
            final int nextPayloadLen = peekLittleEndianInt(framed);
            final int requiredRecordBytes = Integer.BYTES + Integer.BYTES + nextPayloadLen;
            final LedgerSegment segment = wal.writable(requiredRecordBytes);
            final int written = segment.appendFramedBatchNoOffsets(framed, remaining);
            if (written <= 0) {
                throw new IOException("Failed to append offset WAL batch");
            }
            remaining -= written;
        }
        return framed;
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
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        // WAL close.
        wal.close();
    }

    private PendingCommit acquireCommit() {
        final PendingCommit reused = commitPool.poll();
        if (reused != null) {
            pooledCommitCount.decrementAndGet();
            return reused;
        }
        return new PendingCommit();
    }

    private void recycleBatch(final PendingCommit[] batch, final int count) {
        for (int i = 0; i < count; i++) {
            final PendingCommit c = batch[i];
            batch[i] = null;
            if (c != null) {
                c.clear();
                tryOfferPooledCommit(c);
            }
        }
    }

    private void tryOfferPooledCommit(final PendingCommit commit) {
        if (tryReservePoolSlot()) {
            commitPool.offer(commit);
        }
    }

    private boolean tryReservePoolSlot() {
        for (;;) {
            final int current = pooledCommitCount.get();
            if (current >= MAX_COMMIT_POOL_SIZE) {
                return false;
            }
            if (pooledCommitCount.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    private static int payloadSize(final PendingCommit c) {
        return Integer.BYTES + c.topicBytes.length
                + Integer.BYTES + c.groupBytes.length
                + Integer.BYTES
                + Long.BYTES;
    }

    private static ByteBuffer ensureFramedCapacity(final ByteBuffer current, final int requiredBytes) {
        if (current.capacity() >= requiredBytes) {
            return current;
        }

        int next = current.capacity();
        while (next < requiredBytes) {
            next <<= 1;
        }
        return ByteBuffer.allocateDirect(next).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static int peekLittleEndianInt(final ByteBuffer src) {
        final int pos = src.position();
        final int b0 = src.get(pos) & 0xFF;
        final int b1 = src.get(pos + 1) & 0xFF;
        final int b2 = src.get(pos + 2) & 0xFF;
        final int b3 = src.get(pos + 3) & 0xFF;
        return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
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
            ByteBuffer payloadBuf = ByteBuffer.allocate(4 * 1024).order(ByteOrder.LITTLE_ENDIAN);

            while (ch.position() < fileSize) {
                lenBuf.clear();
                final int n = ch.read(lenBuf);
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

                if (payloadBuf.capacity() < payloadLen) {
                    payloadBuf = ByteBuffer.allocate(nextPowerOfTwo(payloadLen)).order(ByteOrder.LITTLE_ENDIAN);
                }
                payloadBuf.clear();
                payloadBuf.limit(payloadLen);
                while (payloadBuf.hasRemaining()) {
                    final int r = ch.read(payloadBuf);
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

    private static int nextPowerOfTwo(final int value) {
        int v = Math.max(1, value);
        int hi = Integer.highestOneBit(v);
        if (v == hi) {
            return v;
        }
        hi <<= 1;
        return (hi > 0) ? hi : Integer.MAX_VALUE;
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

        final PartitionOffsets po = partitionOffsets(topic, group);
        po.set(partition, offset);
    }

}
