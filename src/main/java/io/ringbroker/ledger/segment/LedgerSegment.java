package io.ringbroker.ledger.segment;

import io.ringbroker.ledger.constant.LedgerConstant;
import io.ringbroker.ledger.orchestrator.LedgerOrchestrator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;

/**
 * An append-only, memory-mapped segment file optimized for single-threaded writers.
 * Each segment contains a header followed by records: [length (int), crc32 (int), payload (byte[])].
 * Header is [magic, version, crc32c, firstOffset, lastOffset].
 * Uses Unsafe for ordered writes and in-place header updates.
 */
@Slf4j
public final class LedgerSegment implements AutoCloseable {
    private static final int MAGIC_POS           = 0;
    private static final int VERSION_POS         = MAGIC_POS + Integer.BYTES;
    private static final int CRC_POS             = VERSION_POS + Short.BYTES;
    private static final int FIRST_OFFSET_POS    = CRC_POS + Integer.BYTES;
    private static final int LAST_OFFSET_POS     = FIRST_OFFSET_POS + Long.BYTES;
    public  static final int HEADER_SIZE        = LAST_OFFSET_POS + Long.BYTES;

    private static final int MIN_RECORD_OVERHEAD = Integer.BYTES + Integer.BYTES;
    private static final int MIN_RECORD_SIZE     = MIN_RECORD_OVERHEAD + 1;

    private static final Unsafe UNSAFE = initUnsafe();
    private static final long FIRST_OFF_OFFSET_IN_OBJECT;
    private static final long LAST_OFF_OFFSET_IN_OBJECT;

    static {
        try {
            FIRST_OFF_OFFSET_IN_OBJECT = UNSAFE.objectFieldOffset(
                    LedgerSegment.class.getDeclaredField("firstOffset"));
            LAST_OFF_OFFSET_IN_OBJECT  = UNSAFE.objectFieldOffset(
                    LedgerSegment.class.getDeclaredField("lastOffset"));
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Getter private final Path             file;
    @Getter private final int              capacity;
    private final MappedByteBuffer         buf;
    private final boolean                  skipRecordCrc;

    // single-thread writer CRC instances & scratch
    private final CRC32   recordCrc      = new CRC32();
    private final CRC32C  headerCrc      = new CRC32C();
    private final ByteBuffer headerScratch =
            ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);

    @Getter private volatile long firstOffset;
    @Getter private volatile long lastOffset;

    private LedgerSegment(final Path file,
                          final int capacity,
                          final MappedByteBuffer buf,
                          final boolean skipRecordCrc) throws IOException {
        this.file          = file;
        this.capacity      = capacity;
        this.buf           = buf;
        this.skipRecordCrc = skipRecordCrc;
        readAndVerifyHeader();
        this.buf.position(scanToEndOfWrittenData());
    }

    private static Unsafe initUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe)f.get(null);
        } catch (Exception e) {
            log.error("Failed to initialize Unsafe", e);
            throw new RuntimeException("Unable to access Unsafe", e);
        }
    }

    /**
     * Create a new, empty segment file.
     * Pre-allocates, writes header+CRC, forces to disk.
     */
    public static LedgerSegment create(final Path file,
                                       final int capacity,
                                       final boolean skipRecordCrc) throws IOException {
        if (capacity < HEADER_SIZE + MIN_RECORD_SIZE) {
            throw new IllegalArgumentException(
                    "Capacity too small for header + one record: " + capacity);
        }
        try (FileChannel ch = FileChannel.open(
                file,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {

            ch.truncate(capacity);
            MappedByteBuffer map = ch.map(
                    FileChannel.MapMode.READ_WRITE, 0, capacity);
            map.order(ByteOrder.LITTLE_ENDIAN);

            // init header: magic, version, zero CRC, zero offsets
            map.putInt(MAGIC_POS,       LedgerConstant.MAGIC);
            map.putShort(VERSION_POS,   LedgerConstant.VERSION);
            map.putInt(CRC_POS,         0);
            map.putLong(FIRST_OFFSET_POS, 0L);
            map.putLong(LAST_OFFSET_POS,  0L);

            writeHeaderCrcStatic(map, 0L, 0L);
            map.force();
            map.position(HEADER_SIZE);

            return new LedgerSegment(file, capacity, map, skipRecordCrc);
        }
    }

    /**
     * Open existing segment, validate header, and position for append.
     */
    public static LedgerSegment openExisting(final Path file,
                                             final boolean skipRecordCrc) throws IOException {
        try (FileChannel ch = FileChannel.open(
                file,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {

            long size = ch.size();
            if (size < HEADER_SIZE) {
                throw new IOException("Segment too small for header: " + file);
            }
            MappedByteBuffer map = ch.map(
                    FileChannel.MapMode.READ_WRITE, 0, size);
            map.order(ByteOrder.LITTLE_ENDIAN);
            return new LedgerSegment(file, (int)size, map, skipRecordCrc);
        }
    }

    private void readAndVerifyHeader() throws IOException {
        int magic = buf.getInt(MAGIC_POS);
        short ver = buf.getShort(VERSION_POS);
        if (magic != LedgerConstant.MAGIC) {
            throw new IOException("Bad magic in " + file);
        }
        if (ver != LedgerConstant.VERSION) {
            throw new IOException("Bad version in " + file);
        }
        int storedCrc = buf.getInt(CRC_POS);
        long fo = buf.getLong(FIRST_OFFSET_POS);
        long lo = buf.getLong(LAST_OFFSET_POS);

        // recalc header CRC in-heap
        headerScratch.clear();
        headerScratch
                .putInt(LedgerConstant.MAGIC)
                .putShort(LedgerConstant.VERSION)
                .putInt(0)
                .putLong(fo)
                .putLong(lo)
                .flip();
        headerCrc.reset();
        headerCrc.update(headerScratch);
        if ((int)headerCrc.getValue() != storedCrc) {
            throw new IOException("Header CRC mismatch in " + file);
        }

        this.firstOffset = fo;
        this.lastOffset  = lo;
    }

    /**
     * Scan raw bytes to find end-of-data.
     * Logs warnings on invalid lengths before stopping.
     */
    private int scanToEndOfWrittenData() {
        int pos    = HEADER_SIZE;
        int maxPos = capacity - MIN_RECORD_OVERHEAD - 1;
        while (pos <= maxPos) {
            int len = buf.getInt(pos);
            if (len <= 0) {
                log.warn("Invalid record length {} at pos {} in {} – stopping scan", len, pos, file);
                break;
            }
            if (pos + MIN_RECORD_OVERHEAD + len > capacity) {
                log.warn(
                        "Record length {} at pos {} exceeds remaining {} bytes in {} – stopping scan",
                        len, pos, capacity - pos, file);
                break;
            }
            pos += MIN_RECORD_OVERHEAD + len;
        }
        return pos;
    }

    /**
     * Append single record. Not durable until force().
     */
    public long append(final byte[] data) throws IOException {
        int needed = MIN_RECORD_OVERHEAD + data.length;
        if (buf.position() + needed > capacity) {
            throw new IOException("Segment full: " + file);
        }

        buf.putInt(data.length);
        if (!skipRecordCrc) {
            recordCrc.reset();
            recordCrc.update(data, 0, data.length);
            buf.putInt((int)recordCrc.getValue());
        } else {
            buf.putInt(0);
        }
        buf.put(data);

        long next = (firstOffset == 0 && lastOffset == 0) ? 1L : lastOffset + 1L;
        if (firstOffset == 0L) {
            UNSAFE.putOrderedLong(this, FIRST_OFF_OFFSET_IN_OBJECT, next);
            firstOffset = next;
        }
        UNSAFE.putOrderedLong(this, LAST_OFF_OFFSET_IN_OBJECT, next);
        lastOffset = next;

        updateHeaderOnDisk();
        return next;
    }

    /**
     * Append batch of records. Not durable until force().
     */
    public long[] appendBatch(final List<byte[]> msgs) throws IOException {
        if (msgs.isEmpty()) return new long[0];

        int total = 0;
        for (byte[] m : msgs) total += MIN_RECORD_OVERHEAD + m.length;
        if (buf.position() + total > capacity) {
            throw new IOException("Segment full for batch: " + file);
        }

        long[] outs = new long[msgs.size()];
        long curr = (firstOffset == 0 && lastOffset == 0) ? 0L : lastOffset;
        boolean firstSet = firstOffset != 0L;
        boolean doCrc    = !skipRecordCrc;

        for (int i = 0; i < msgs.size(); i++) {
            byte[] d = msgs.get(i);
            buf.putInt(d.length);
            if (doCrc) {
                recordCrc.reset();
                recordCrc.update(d, 0, d.length);
                buf.putInt((int)recordCrc.getValue());
            } else {
                buf.putInt(0);
            }
            buf.put(d);

            curr++;
            outs[i] = curr;
            if (!firstSet) {
                UNSAFE.putOrderedLong(this, FIRST_OFF_OFFSET_IN_OBJECT, curr);
                firstOffset = curr;
                firstSet = true;
            }
        }

        UNSAFE.putOrderedLong(this, LAST_OFF_OFFSET_IN_OBJECT, curr);
        lastOffset = curr;

        updateHeaderOnDisk();
        return outs;
    }

    public long appendAndForce(final byte[] data) throws IOException {
        long off = append(data);
        buf.force();
        return off;
    }

    public long[] appendBatchAndForce(final List<byte[]> msgs) throws IOException {
        long[] offs = appendBatch(msgs);
        if (!msgs.isEmpty()) buf.force();
        return offs;
    }

    /** Update header offsets + CRC in-place, preserving buffer position. */
    private void updateHeaderOnDisk() {
        int p = buf.position();
        try {
            buf.putLong(FIRST_OFFSET_POS, firstOffset);
            buf.putLong(LAST_OFFSET_POS,  lastOffset);
            // recalc header CRC
            headerScratch.clear();
            headerScratch
                    .putInt(LedgerConstant.MAGIC)
                    .putShort(LedgerConstant.VERSION)
                    .putInt(0)
                    .putLong(firstOffset)
                    .putLong(lastOffset)
                    .flip();
            headerCrc.reset();
            headerCrc.update(headerScratch);
            buf.putInt(CRC_POS, (int)headerCrc.getValue());
        } finally {
            buf.position(p);
        }
    }

    /** Static helper for initial header CRC in create(). */
    private static void writeHeaderCrcStatic(MappedByteBuffer map,
                                             long fo, long lo) {
        ByteBuffer hb = ByteBuffer
                .allocate(HEADER_SIZE)
                .order(ByteOrder.LITTLE_ENDIAN);
        hb.putInt(LedgerConstant.MAGIC)
                .putShort(LedgerConstant.VERSION)
                .putInt(0)
                .putLong(fo)
                .putLong(lo)
                .flip();
        CRC32C crc = new CRC32C();
        crc.update(hb);
        map.putInt(CRC_POS, (int)crc.getValue());
    }

    public boolean isFull() {
        return capacity - buf.position() < MIN_RECORD_SIZE;
    }

    @Override
    public void close() {
        if (buf != null) {
            try { buf.force(); } catch (Exception e) {
                log.warn("Force on close failed: {}", file, e);
            }
            try { UNSAFE.invokeCleaner(buf); } catch (Exception e) {
                log.warn("Cleaner on close failed: {}", file, e);
            }
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
}
