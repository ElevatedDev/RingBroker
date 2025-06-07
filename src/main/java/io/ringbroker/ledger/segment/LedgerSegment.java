package io.ringbroker.ledger.segment;

import io.ringbroker.ledger.constant.LedgerConstant;
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

/**
 * An append-only, memory-mapped segment file optimized for single-threaded writers.
 * Each segment contains a header followed by a sequence of records. Records are
 * structured as [length (int), crc32 (int), payload (byte[])].
 * The segment header includes magic number, version, offsets, and a CRC32 checksum for integrity.
 * This class uses {@link sun.misc.Unsafe} for ordered writes to offset fields.
 */
@Slf4j
public final class LedgerSegment implements AutoCloseable {
    private static final int MAGIC_POS = 0;                     // int   (4 bytes)
    private static final int VERSION_POS = MAGIC_POS + Integer.BYTES; // short (2 bytes)
    private static final int CRC_POS = VERSION_POS + Short.BYTES;   // int   (4 bytes) - CRC32 of header (excluding this field)
    private static final int FIRST_OFFSET_POS = CRC_POS + Integer.BYTES;   // long  (8 bytes) - Global offset of the first record
    private static final int LAST_OFFSET_POS = FIRST_OFFSET_POS + Long.BYTES; // long  (8 bytes) - Global offset of the last record

    public static final int HEADER_SIZE = LAST_OFFSET_POS + Long.BYTES;  // 26 bytes total

    private static final int MIN_RECORD_OVERHEAD = Integer.BYTES + Integer.BYTES;
    private static final int MIN_RECORD_SIZE = MIN_RECORD_OVERHEAD + 1;

    private static final int HEADER_CRC_INTERVAL = 512;

    private static final Unsafe UNSAFE = initUnsafe();

    private static final long FIRST_OFF_OFFSET_IN_OBJECT;
    private static final long LAST_OFF_OFFSET_IN_OBJECT;

    private static final ThreadLocal<CRC32> RECORD_CRC32_TL = ThreadLocal.withInitial(CRC32::new);

    static {
        try {
            FIRST_OFF_OFFSET_IN_OBJECT = UNSAFE.objectFieldOffset(LedgerSegment.class.getDeclaredField("firstOffset"));
            LAST_OFF_OFFSET_IN_OBJECT = UNSAFE.objectFieldOffset(LedgerSegment.class.getDeclaredField("lastOffset"));
        } catch (final NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Getter
    private final Path file;
    @Getter
    private final int capacity;
    private final MappedByteBuffer buf;
    private final boolean skipRecordCrc;

    @Getter
    private volatile long firstOffset;
    @Getter
    private volatile long lastOffset;

    private int writesSinceHeaderCrc;

    private LedgerSegment(final Path file, final int capacity, final MappedByteBuffer buf, final boolean skipRecordCrc) throws IOException {
        this.file = file;
        this.capacity = capacity;
        this.buf = buf;
        this.skipRecordCrc = skipRecordCrc;

        readAndVerifyHeader();
        this.buf.position(scanToEndOfWrittenData());
    }

    private static Unsafe initUnsafe() {
        try {
            final Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (final Exception e) {
            throw new RuntimeException("Unable to access Unsafe", e);
        }
    }

    public static LedgerSegment create(final Path file, final int capacity, final boolean skipRecordCrc) throws IOException {
        if (capacity < HEADER_SIZE + MIN_RECORD_SIZE) {
            throw new IllegalArgumentException("Capacity is too small for header and one minimal record.");
        }
        try (final FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ch.truncate(capacity);

            final MappedByteBuffer map = ch.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
            map.order(ByteOrder.LITTLE_ENDIAN);

            map.putInt(MAGIC_POS, LedgerConstant.MAGIC);
            map.putShort(VERSION_POS, LedgerConstant.VERSION);
            map.putLong(FIRST_OFFSET_POS, 0L);
            map.putLong(LAST_OFFSET_POS, 0L);

            map.putInt(CRC_POS, 0);
            calculateAndWriteHeaderCrc(map);

            map.force();
            map.position(HEADER_SIZE);

            return new LedgerSegment(file, capacity, map, skipRecordCrc);
        }
    }

    public static LedgerSegment openExisting(final Path file, final boolean skipRecordCrc) throws IOException {
        try (final FileChannel ch = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            final long fileSize = ch.size();
            if (fileSize < HEADER_SIZE) {
                throw new IOException("Segment file is too small to contain a header: " + file + " size: " + fileSize);
            }
            final MappedByteBuffer map = ch.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            map.order(ByteOrder.LITTLE_ENDIAN);

            return new LedgerSegment(file, (int) fileSize, map, skipRecordCrc);
        }
    }

    private static void calculateAndWriteHeaderCrc(final MappedByteBuffer map) {
        final int crc = calculateHeaderCrc(map);
        map.putInt(CRC_POS, crc);
    }

    private static int calculateHeaderCrc(final MappedByteBuffer map) {
        final ByteBuffer view = map.asReadOnlyBuffer();
        view.order(map.order());
        view.position(MAGIC_POS);

        final byte[] headerBytes = new byte[HEADER_SIZE];
        view.get(headerBytes);

        for (int i = CRC_POS; i < CRC_POS + Integer.BYTES; i++) {
            headerBytes[i] = 0;
        }

        final CRC32 crcCalculator = new CRC32();
        crcCalculator.update(headerBytes, 0, HEADER_SIZE);
        return (int) crcCalculator.getValue();
    }

    private void readAndVerifyHeader() throws IOException {
        buf.position(MAGIC_POS);

        final int magic = buf.getInt();
        final short version = buf.getShort();

        if (magic != LedgerConstant.MAGIC) {
            throw new IOException("Invalid magic number in segment header: " + file);
        }
        if (version != LedgerConstant.VERSION) {
            throw new IOException("Unsupported version in segment header: " + file);
        }

        final int storedCrc = buf.getInt(CRC_POS);
        final int calculatedCrc = calculateHeaderCrc(buf);

        if (storedCrc != calculatedCrc) {
            throw new IOException("Header CRC mismatch for segment: " + file);
        }

        this.firstOffset = buf.getLong(FIRST_OFFSET_POS);
        this.lastOffset = buf.getLong(LAST_OFFSET_POS);

        buf.position(HEADER_SIZE);
    }

    private int scanToEndOfWrittenData() {
        buf.position(HEADER_SIZE);
        int currentBytePosition = HEADER_SIZE;

        while (currentBytePosition <= capacity - MIN_RECORD_SIZE) {
            if (buf.remaining() < MIN_RECORD_OVERHEAD) {
                break;
            }

            buf.mark();

            final int recordLength = buf.getInt();

            if (recordLength <= 0) {
                buf.reset();
                break;
            }

            final int recordCrcPosition = buf.position();

            if (buf.remaining() < Integer.BYTES + recordLength) {
                buf.reset();
                break;
            }

            buf.position(recordCrcPosition + Integer.BYTES + recordLength);
            currentBytePosition = buf.position();
        }

        return buf.position();
    }

    public long append(final byte[] data) throws IOException {
        final int recordDataLength = data.length;
        final int spaceNeeded = MIN_RECORD_OVERHEAD + recordDataLength;

        if (buf.position() + spaceNeeded > capacity) {
            throw new IOException("Segment full. File: " + file);
        }

        buf.putInt(recordDataLength);

        if (!skipRecordCrc) {
            final CRC32 crc = RECORD_CRC32_TL.get();
            crc.reset();
            crc.update(data, 0, recordDataLength);
            buf.putInt((int) crc.getValue());
        } else {
            buf.putInt(0);
        }

        buf.put(data);

        final long newGlobalOffset = (this.lastOffset == 0 && this.firstOffset == 0) ? 1 : this.lastOffset + 1;

        if (this.firstOffset == 0) {
            UNSAFE.putOrderedLong(this, FIRST_OFF_OFFSET_IN_OBJECT, newGlobalOffset);
        }

        UNSAFE.putOrderedLong(this, LAST_OFF_OFFSET_IN_OBJECT, newGlobalOffset);

        boolean needFullHeaderCrc = (++writesSinceHeaderCrc >= HEADER_CRC_INTERVAL) || (this.firstOffset == newGlobalOffset);
        if (needFullHeaderCrc) writesSinceHeaderCrc = 0;

        updateHeaderOnDisk(needFullHeaderCrc);

        return newGlobalOffset;
    }

    public long[] appendBatch(final List<byte[]> messages) throws IOException {
        if (messages.isEmpty()) {
            return new long[0];
        }

        int totalSpaceNeeded = 0;
        for (final byte[] msg : messages) {
            totalSpaceNeeded += (MIN_RECORD_OVERHEAD + msg.length);
        }

        if (buf.position() + totalSpaceNeeded > capacity) {
            throw new IOException("Segment full for batch. File: " + file);
        }

        final long[] offsets = new long[messages.size()];
        long currentGlobalOffset = (this.lastOffset == 0 && this.firstOffset == 0) ? 0 : this.lastOffset;

        boolean firstInSegmentUpdated = (this.firstOffset != 0);

        for (int i = 0; i < messages.size(); i++) {
            final byte[] data = messages.get(i);
            final int recordDataLength = data.length;

            buf.putInt(recordDataLength);

            if (!skipRecordCrc) {
                final CRC32 crc = RECORD_CRC32_TL.get();
                crc.reset();
                crc.update(data, 0, recordDataLength);

                buf.putInt((int) crc.getValue());
            } else {
                buf.putInt(0);
            }

            buf.put(data);

            currentGlobalOffset++;
            offsets[i] = currentGlobalOffset;

            if (!firstInSegmentUpdated) {
                UNSAFE.putOrderedLong(this, FIRST_OFF_OFFSET_IN_OBJECT, offsets[0]);
                firstInSegmentUpdated = true;
            }
        }

        UNSAFE.putOrderedLong(this, LAST_OFF_OFFSET_IN_OBJECT, currentGlobalOffset);

        boolean needFullHeaderCrc = (writesSinceHeaderCrc += messages.size()) >= HEADER_CRC_INTERVAL;
        if (needFullHeaderCrc) writesSinceHeaderCrc = 0;

        updateHeaderOnDisk(needFullHeaderCrc);
        return offsets;
    }

    public long appendAndForce(final byte[] data) throws IOException {
        final long offset = append(data);

        buf.force();
        return offset;
    }

    public long[] appendBatchAndForce(final List<byte[]> messages) throws IOException {
        final long[] offsets = appendBatch(messages);

        if (messages.isEmpty()) return offsets;
        buf.force();

        return offsets;
    }

    public void force() {
        try {
            buf.force();
        } catch (final Exception e) {
            log.warn("Failed to force buffer for segment: {}", file, e);
        }
    }

    private void updateHeaderOnDisk(final boolean recomputeCrc) {
        final int savedPosition = buf.position();
        try {
            if (buf.getLong(FIRST_OFFSET_POS) == 0L && this.firstOffset != 0L) {
                buf.putLong(FIRST_OFFSET_POS, this.firstOffset);
            }

            buf.putLong(LAST_OFFSET_POS, this.lastOffset);

            if (recomputeCrc) {
                calculateAndWriteHeaderCrc(this.buf);
            }
        } finally {
            buf.position(savedPosition);
        }
    }

    public boolean isFull() {
        return capacity - buf.position() < MIN_RECORD_SIZE;
    }

    @Override
    public void close() {
        if (buf != null) {
            try {
                buf.force();
            } catch (final Exception e) {
                log.warn("Failed to force buffer during close for segment: {}", file, e);
            }
            try {
                UNSAFE.invokeCleaner(buf);
            } catch (final Exception e) {
                log.warn("Failed to invoke cleaner on MappedByteBuffer for segment: {}", file, e);
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
                ", currentPosition=" + (buf != null ? buf.position() : "N/A") +
                '}';
    }
}
