package io.ringbroker.ledger.segment;

import io.ringbroker.ledger.constant.LedgerConstant;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.CRC32C;

@Slf4j
public final class LedgerSegment implements AutoCloseable {
    private static final int MAGIC_POS = 0;
    private static final int VERSION_POS = MAGIC_POS + Integer.BYTES;
    private static final int CRC_POS = VERSION_POS + Short.BYTES;
    private static final int FIRST_OFFSET_POS = CRC_POS + Integer.BYTES;
    private static final int LAST_OFFSET_POS = FIRST_OFFSET_POS + Long.BYTES;
    public static final int HEADER_SIZE = LAST_OFFSET_POS + Long.BYTES;

    private static final int MIN_RECORD_OVERHEAD = Integer.BYTES + Integer.BYTES; // Len + CRC
    private static final int MIN_RECORD_SIZE = MIN_RECORD_OVERHEAD + 1;

    /**
     * Sentinel meaning "no first offset yet" (empty segment).
     * We MUST NOT use 0 because offset 0 is valid.
     */
    private static final long FIRST_OFFSET_UNSET = Long.MIN_VALUE;

    // Hint stride: 1 hint per 256 records.
    private static final int HINT_SHIFT = 8;
    private static final int HINT_STRIDE = 1 << HINT_SHIFT;
    private static final int HINT_MASK = HINT_STRIDE - 1;

    private static final Unsafe UNSAFE = initUnsafe();
    private static final long FIRST_OFF_OFFSET;
    private static final long LAST_OFF_OFFSET;

    private static final VarHandle COUNT_HANDLE;

    static {
        try {
            FIRST_OFF_OFFSET = UNSAFE.objectFieldOffset(LedgerSegment.class.getDeclaredField("firstOffset"));
            LAST_OFF_OFFSET = UNSAFE.objectFieldOffset(LedgerSegment.class.getDeclaredField("lastOffset"));
            COUNT_HANDLE = MethodHandles.lookup().findVarHandle(LedgerSegment.class, "publishedCount", int.class);
        } catch (final Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Getter private final Path file;
    @Getter private final int capacity;
    private final MappedByteBuffer buf;
    private final boolean skipRecordCrc;

    // Sparse heap hints (kept hot in CPU cache)
    private final int[] hintPositions;

    // Optional dense mmap index for sealed segments
    private volatile DenseOffsetIndex denseIndex;

    // Reusing CRC32C instance is fine as LedgerSegment is single-threaded writer
    private final CRC32C recordCrc = new CRC32C();
    private final CRC32C headerCrc = new CRC32C();
    private final ByteBuffer headerScratch = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);

    @SuppressWarnings("FieldMayBeFinal")
    private volatile int publishedCount;

    @Getter private volatile long firstOffset;
    @Getter private volatile long lastOffset;

    private LedgerSegment(final Path file,
                          final int capacity,
                          final MappedByteBuffer buf,
                          final boolean skipRecordCrc,
                          final int[] hintPositions) throws IOException {
        this.file = file;
        this.capacity = capacity;
        this.buf = buf;
        this.skipRecordCrc = skipRecordCrc;
        this.hintPositions = hintPositions;

        readAndVerifyHeader();
        rebuildHintsAndCountFromSegment();
        tryOpenDenseIndexIfValid();
    }

    private static Unsafe initUnsafe() {
        try {
            final Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates an empty segment where lastOffset is initialized to baseLastOffset.
     * For the very first segment in a partition, baseLastOffset should be -1 so the first record offset is 0.
     */
    public static LedgerSegment create(final Path file,
                                       final int capacity,
                                       final boolean skipRecordCrc,
                                       final long baseLastOffset) throws IOException {
        if (capacity < HEADER_SIZE + MIN_RECORD_SIZE) throw new IllegalArgumentException("Capacity too small");

        try (final FileChannel ch = FileChannel.open(file,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ch.truncate(capacity);
            final MappedByteBuffer map = ch.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
            map.order(ByteOrder.LITTLE_ENDIAN);

            // Write header
            map.putInt(MAGIC_POS, LedgerConstant.MAGIC);
            map.putShort(VERSION_POS, LedgerConstant.VERSION);
            map.putInt(CRC_POS, 0);
            map.putLong(FIRST_OFFSET_POS, FIRST_OFFSET_UNSET);
            map.putLong(LAST_OFFSET_POS, baseLastOffset);

            writeHeaderCrcStatic(map, FIRST_OFFSET_UNSET, baseLastOffset);

            map.position(HEADER_SIZE);

            final int maxRecords = Math.max(1, (capacity - HEADER_SIZE) / MIN_RECORD_SIZE);
            final int hintCount = Math.max(1, (maxRecords + HINT_STRIDE - 1) >>> HINT_SHIFT);

            return new LedgerSegment(file, capacity, map, skipRecordCrc, new int[hintCount]);
        }
    }

    public static LedgerSegment openExisting(final Path file, final boolean skipRecordCrc) throws IOException {
        try (final FileChannel ch = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            final long size = ch.size();
            if (size < HEADER_SIZE) throw new IOException("Segment too small");
            final MappedByteBuffer map = ch.map(FileChannel.MapMode.READ_WRITE, 0, size);
            map.order(ByteOrder.LITTLE_ENDIAN);

            final int cap = (int) size;
            final int maxRecords = Math.max(1, (cap - HEADER_SIZE) / MIN_RECORD_SIZE);
            final int hintCount = Math.max(1, (maxRecords + HINT_STRIDE - 1) >>> HINT_SHIFT);

            return new LedgerSegment(file, cap, map, skipRecordCrc, new int[hintCount]);
        }
    }

    public static Path indexPathForSegment(final Path segmentFile) {
        final String name = segmentFile.getFileName().toString();
        final int dot = name.lastIndexOf('.');
        final String base = (dot >= 0) ? name.substring(0, dot) : name;
        return segmentFile.getParent().resolve(base + LedgerConstant.INDEX_EXT);
    }

    private static void writeHeaderCrcStatic(final MappedByteBuffer map, final long fo, final long lo) {
        final ByteBuffer hb = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        hb.putInt(LedgerConstant.MAGIC).putShort(LedgerConstant.VERSION).putInt(0).putLong(fo).putLong(lo).flip();
        final CRC32C crc = new CRC32C();
        crc.update(hb);
        map.putInt(CRC_POS, (int) crc.getValue());
    }

    private void readAndVerifyHeader() throws IOException {
        final int magic = buf.getInt(MAGIC_POS);
        final short ver = buf.getShort(VERSION_POS);
        if (magic != LedgerConstant.MAGIC || ver != LedgerConstant.VERSION)
            throw new IOException("Bad magic/version in " + file);

        final int storedCrc = buf.getInt(CRC_POS);
        final long fo = buf.getLong(FIRST_OFFSET_POS);
        final long lo = buf.getLong(LAST_OFFSET_POS);

        headerScratch.clear();
        headerScratch.putInt(LedgerConstant.MAGIC).putShort(LedgerConstant.VERSION).putInt(0).putLong(fo).putLong(lo).flip();
        headerCrc.reset();
        headerCrc.update(headerScratch);
        if ((int) headerCrc.getValue() != storedCrc)
            throw new IOException("Header CRC mismatch in " + file);

        this.firstOffset = fo;
        this.lastOffset = lo;
    }

    private int countAcquire() {
        return (int) COUNT_HANDLE.getAcquire(this);
    }

    public boolean isLogicallyEmpty() {
        return firstOffset == FIRST_OFFSET_UNSET && countAcquire() == 0;
    }

    private void rebuildHintsAndCountFromSegment() {
        int pos = HEADER_SIZE;
        final int maxPos = capacity - MIN_RECORD_OVERHEAD;

        int count = 0;

        while (pos <= maxPos) {
            final int len = buf.getInt(pos);
            if (len == 0) break;

            if (len < 0) {
                log.warn("Corrupt negative length {} at {} in {}", len, pos, file);
                break;
            }

            final int next = pos + MIN_RECORD_OVERHEAD + len;
            if (next > capacity) {
                log.warn("Record length {} exceeds capacity at {} in {}", len, pos, file);
                break;
            }

            if ((count & HINT_MASK) == 0) {
                final int hi = count >>> HINT_SHIFT;
                if (hi < hintPositions.length) hintPositions[hi] = pos;
            }

            count++;
            pos = next;
        }

        buf.position(pos);
        COUNT_HANDLE.setRelease(this, count);

        // Repair header if needed
        if (count > 0) {
            long fo = this.firstOffset;
            long lo = this.lastOffset;

            if (fo == FIRST_OFFSET_UNSET) {
                if (lo < 0) {
                    fo = 0L;
                    lo = count - 1L;
                } else {
                    fo = lo - (count - 1L);
                }
                UNSAFE.putOrderedLong(this, FIRST_OFF_OFFSET, fo);
                this.firstOffset = fo;
            }

            final long actualLast = fo + (count - 1L);
            if (actualLast != lo) {
                UNSAFE.putOrderedLong(this, LAST_OFF_OFFSET, actualLast);
                this.lastOffset = actualLast;
            }

            updateHeaderOnDisk();
        }
    }

    private void tryOpenDenseIndexIfValid() {
        final int required = countAcquire();
        final DenseOffsetIndex idx = DenseOffsetIndex.openIfValid(indexPathForSegment(file), required);
        if (idx != null) {
            this.denseIndex = idx;
        }
    }

    public boolean hasSpaceFor(final int payloadBytes) {
        return (capacity - buf.position()) >= (payloadBytes + MIN_RECORD_OVERHEAD);
    }

    /**
     * Dense, O(1) offset index:
     * recordIndex = offset - firstOffset
     * idx[recordIndex] = recordStartPos within this segment mmap.
     *
     * Hot fetch path: seek ONCE, then walk sequentially.
     */
    @FunctionalInterface
    public interface PayloadVisitor {
        void accept(long offset, MappedByteBuffer segmentBuffer, int payloadPos, int payloadLen);
    }

    private int positionForRecordIndex(final int recordIndex) {
        final int chunk = recordIndex >>> HINT_SHIFT;
        int pos = hintPositions[chunk];
        final int base = chunk << HINT_SHIFT;

        for (int i = base; i < recordIndex; i++) {
            final int len = buf.getInt(pos);
            pos += (MIN_RECORD_OVERHEAD + len);
        }

        return pos;
    }

    public int visitFromOffset(final long offset,
                               final int maxMessages,
                               final PayloadVisitor visitor) {
        if (maxMessages <= 0) return 0;

        final long fo = this.firstOffset;
        if (fo == FIRST_OFFSET_UNSET) return 0;

        final int available = countAcquire();
        if (available <= 0) return 0;

        long start = offset;
        if (start < fo) start = fo;

        final long lastByCount = fo + (available - 1L);
        if (start > lastByCount) return 0;

        final int startIdx = (int) (start - fo);
        int endIdx = startIdx + maxMessages;
        if (endIdx > available) endIdx = available;

        final DenseOffsetIndex idx = this.denseIndex;
        final int startPos =
                (idx != null && idx.count == available)
                        ? idx.positionAt(startIdx)
                        : positionForRecordIndex(startIdx);

        int p = startPos;

        for (int i = startIdx; i < endIdx; i++) {
            final int len = buf.getInt(p);
            final int payloadPos = p + MIN_RECORD_OVERHEAD;
            visitor.accept(fo + i, buf, payloadPos, len);
            p += (MIN_RECORD_OVERHEAD + len);
        }

        return endIdx - startIdx;
    }

    // ---- Append APIs ----

    public void appendBatchNoOffsets(final List<byte[]> msgs, final int totalBytes) throws IOException {
        if (msgs.isEmpty()) return;

        if (buf.position() + totalBytes > capacity) {
            throw new IOException("Segment full for batch: " + file);
        }

        final int count = msgs.size();

        long curr = lastOffset;
        boolean firstSet = (firstOffset != FIRST_OFFSET_UNSET);
        final boolean doCrc = !skipRecordCrc;

        final int baseIdx = countAcquire();

        for (int i = 0; i < count; i++) {
            final byte[] d = msgs.get(i);
            final int len = d.length;

            final int ridx = baseIdx + i;
            if ((ridx & HINT_MASK) == 0) {
                hintPositions[ridx >>> HINT_SHIFT] = buf.position();
            }

            buf.putInt(len);

            if (doCrc) {
                recordCrc.reset();
                recordCrc.update(d, 0, len);
                buf.putInt((int) recordCrc.getValue());
            } else {
                buf.putInt(0);
            }

            buf.put(d);

            curr++;

            if (!firstSet) {
                UNSAFE.putOrderedLong(this, FIRST_OFF_OFFSET, curr);
                firstOffset = curr;
                firstSet = true;
            }
        }

        UNSAFE.putOrderedLong(this, LAST_OFF_OFFSET, curr);
        lastOffset = curr;

        COUNT_HANDLE.setRelease(this, baseIdx + count);

        updateHeaderOnDisk();
    }

    public void appendBatchAndForceNoOffsets(final List<byte[]> msgs, final int totalBytes) throws IOException {
        appendBatchNoOffsets(msgs, totalBytes);
        if (!msgs.isEmpty()) buf.force();
    }

    // Keep existing signature for compatibility (but avoid using it in hot paths).
    public long[] appendBatch(final List<byte[]> msgs, final int totalBytes) throws IOException {
        if (msgs.isEmpty()) return new long[0];

        final int count = msgs.size();
        final long[] outs = new long[count];

        if (buf.position() + totalBytes > capacity) {
            throw new IOException("Segment full for batch: " + file);
        }

        long curr = lastOffset;
        boolean firstSet = (firstOffset != FIRST_OFFSET_UNSET);
        final boolean doCrc = !skipRecordCrc;

        final int baseIdx = countAcquire();

        for (int i = 0; i < count; i++) {
            final byte[] d = msgs.get(i);
            final int len = d.length;

            final int ridx = baseIdx + i;
            if ((ridx & HINT_MASK) == 0) {
                hintPositions[ridx >>> HINT_SHIFT] = buf.position();
            }

            buf.putInt(len);

            if (doCrc) {
                recordCrc.reset();
                recordCrc.update(d, 0, len);
                buf.putInt((int) recordCrc.getValue());
            } else {
                buf.putInt(0);
            }

            buf.put(d);

            curr++;
            outs[i] = curr;

            if (!firstSet) {
                UNSAFE.putOrderedLong(this, FIRST_OFF_OFFSET, curr);
                firstOffset = curr;
                firstSet = true;
            }
        }

        UNSAFE.putOrderedLong(this, LAST_OFF_OFFSET, curr);
        lastOffset = curr;

        COUNT_HANDLE.setRelease(this, baseIdx + count);

        updateHeaderOnDisk();
        return outs;
    }

    public long[] appendBatchAndForce(final List<byte[]> msgs, final int totalBytes) throws IOException {
        final long[] offs = appendBatch(msgs, totalBytes);
        if (!msgs.isEmpty()) buf.force();
        return offs;
    }

    private void updateHeaderOnDisk() {
        final int p = buf.position();
        try {
            buf.putLong(FIRST_OFFSET_POS, firstOffset);
            buf.putLong(LAST_OFFSET_POS, lastOffset);
            headerScratch.clear();
            headerScratch.putInt(LedgerConstant.MAGIC).putShort(LedgerConstant.VERSION).putInt(0).putLong(firstOffset).putLong(lastOffset).flip();
            headerCrc.reset();
            headerCrc.update(headerScratch);
            buf.putInt(CRC_POS, (int) headerCrc.getValue());
        } finally {
            buf.position(p);
        }
    }

    public boolean isFull() {
        return capacity - buf.position() < MIN_RECORD_SIZE;
    }

    /**
     * Builds dense .idx for this segment if missing/stale.
     * Safe to call multiple times.
     */
    public void buildDenseIndexIfMissingOrStale() {
        final int required = countAcquire();
        final Path idxPath = indexPathForSegment(file);

        final DenseOffsetIndex existing = this.denseIndex;
        if (existing != null && existing.count == required) return;

        final DenseOffsetIndex onDisk = DenseOffsetIndex.openIfValid(idxPath, required);
        if (onDisk != null) {
            this.denseIndex = onDisk;
            return;
        }

        try {
            DenseOffsetIndex.buildFromSegment(idxPath, buf, capacity, required);
            final DenseOffsetIndex opened = DenseOffsetIndex.openIfValid(idxPath, required);
            if (opened != null) this.denseIndex = opened;
        } catch (final Exception e) {
            log.warn("Failed to build dense index for {} (continuing with hints): {}", file, e.toString());
        }
    }

    @Override
    public void close() {
        try {
            // Ensure idx exists for tests + production determinism.
            buildDenseIndexIfMissingOrStale();
        } catch (final Exception ignored) {}

        final DenseOffsetIndex idx = this.denseIndex;
        if (idx != null) {
            try { idx.close(); } catch (final Exception ignored) {}
        }

        if (buf != null) {
            try { buf.force(); } catch (final Exception ignored) {}
            try { UNSAFE.invokeCleaner(buf); } catch (final Exception ignored) {}
        }
    }

    @Override
    public String toString() {
        return "LedgerSegment{" +
                "file=" + file +
                ", capacity=" + capacity +
                ", firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                ", position=" + (buf != null ? buf.position() : "N/A") +
                '}';
    }

    /**
     * Dense offset index stored as a sidecar file:
     * header + int recordStartPos[].
     *
     * Read-only mmap for seek-once usage.
     */
    static final class DenseOffsetIndex implements AutoCloseable {
        private static final int IDX_MAGIC_POS = 0;
        private static final int IDX_VERSION_POS = IDX_MAGIC_POS + Integer.BYTES;
        private static final int IDX_RESERVED_POS = IDX_VERSION_POS + Short.BYTES;
        private static final int IDX_COUNT_POS = IDX_RESERVED_POS + Short.BYTES;
        private static final int IDX_HEADER_SIZE = IDX_COUNT_POS + Integer.BYTES + Integer.BYTES;

        final int count;
        private final MappedByteBuffer idxBuf;

        private DenseOffsetIndex(final int count, final MappedByteBuffer idxBuf) {
            this.count = count;
            this.idxBuf = idxBuf;
        }

        int positionAt(final int recordIndex) {
            final int p = IDX_HEADER_SIZE + (recordIndex * Integer.BYTES);
            return idxBuf.getInt(p);
        }

        static DenseOffsetIndex openIfValid(final Path path, final int requiredCount) {
            if (!Files.exists(path)) return null;

            try (final FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
                final long size = ch.size();
                if (size < IDX_HEADER_SIZE) return null;

                final MappedByteBuffer map = ch.map(FileChannel.MapMode.READ_ONLY, 0, size);
                map.order(ByteOrder.LITTLE_ENDIAN);

                final int magic = map.getInt(IDX_MAGIC_POS);
                if (magic != LedgerConstant.INDEX_MAGIC) return null;

                final short ver = map.getShort(IDX_VERSION_POS);
                if (ver != LedgerConstant.INDEX_VERSION) return null;

                final int count = map.getInt(IDX_COUNT_POS);
                if (count != requiredCount) return null;

                final long need = (long) IDX_HEADER_SIZE + (long) count * Integer.BYTES;
                if (size < need) return null;

                return new DenseOffsetIndex(count, map);
            } catch (final Exception ignored) {
                return null;
            }
        }

        static void buildFromSegment(final Path finalIdxPath,
                                     final MappedByteBuffer segBuf,
                                     final int capacity,
                                     final int expectedCount) throws IOException {
            final Path tmp = finalIdxPath.resolveSibling(finalIdxPath.getFileName().toString() + ".tmp");

            try (final FileChannel ch = FileChannel.open(tmp,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {

                final ByteBuffer header = ByteBuffer.allocate(IDX_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
                header.putInt(LedgerConstant.INDEX_MAGIC);
                header.putShort(LedgerConstant.INDEX_VERSION);
                header.putShort((short) 0);
                header.putInt(expectedCount);
                header.putInt(0);
                header.flip();
                ch.write(header);

                if (expectedCount > 0) {
                    final ByteBuffer out = ByteBuffer.allocateDirect(1 << 20).order(ByteOrder.LITTLE_ENDIAN);

                    int pos = HEADER_SIZE;
                    final int maxPos = capacity - MIN_RECORD_OVERHEAD;
                    int count = 0;

                    while (pos <= maxPos && count < expectedCount) {
                        final int len = segBuf.getInt(pos);
                        if (len == 0) break;
                        if (len < 0) break;

                        final int next = pos + MIN_RECORD_OVERHEAD + len;
                        if (next > capacity) break;

                        if (out.remaining() < Integer.BYTES) {
                            out.flip();
                            while (out.hasRemaining()) ch.write(out);
                            out.clear();
                        }

                        out.putInt(pos);
                        count++;
                        pos = next;
                    }

                    out.flip();
                    while (out.hasRemaining()) ch.write(out);
                }
            }

            try {
                Files.move(tmp, finalIdxPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (final Exception e) {
                Files.move(tmp, finalIdxPath, StandardCopyOption.REPLACE_EXISTING);
            }
        }

        @Override
        public void close() {
            try { UNSAFE.invokeCleaner(idxBuf); } catch (final Exception ignored) {}
        }
    }
}
