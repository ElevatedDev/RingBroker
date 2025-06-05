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
import java.util.EnumSet;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;

/**
 * An append-only, memory-mapped segment file optimized for single-threaded writers.
 * Each segment contains a header followed by a sequence of records. Records are
 * structured as [length (int), crc32 (int), payload (byte[])].
 * The segment header includes magic number, version, offsets, and a CRC32C checksum for integrity.
 * This class uses {@link sun.misc.Unsafe} for ordered writes to offset fields.
 */
@Slf4j
public final class LedgerSegment implements AutoCloseable {

    /* ─── Layout Constants ────────────────────────────────────────────────── */
    private static final int MAGIC_POS        = 0;                     // int   (4 bytes)
    private static final int VERSION_POS      = MAGIC_POS + Integer.BYTES; // short (2 bytes)
    private static final int CRC_POS          = VERSION_POS + Short.BYTES;   // int   (4 bytes) - CRC32C of header (excluding this field)
    private static final int FIRST_OFFSET_POS = CRC_POS + Integer.BYTES;   // long  (8 bytes) - Global offset of the first record
    private static final int LAST_OFFSET_POS  = FIRST_OFFSET_POS + Long.BYTES; // long  (8 bytes) - Global offset of the last record

    /** Total size of the segment header. */
    public static final int HEADER_SIZE = LAST_OFFSET_POS + Long.BYTES;  // 26 bytes total

    /** Minimum size of a record: length (4) + crc (4) + at least 1 byte payload. */
    private static final int MIN_RECORD_OVERHEAD = Integer.BYTES + Integer.BYTES;
    private static final int MIN_RECORD_SIZE = MIN_RECORD_OVERHEAD + 1;


    /* ─── Unsafe & Offsets ────────────────────────────────────────────────── */
    private static final Unsafe UNSAFE = initUnsafe();
    private static final long FIRST_OFF_OFFSET_IN_OBJECT; // Unsafe offset for 'firstOffset' field
    private static final long LAST_OFF_OFFSET_IN_OBJECT;  // Unsafe offset for 'lastOffset' field

    static {
        try {
            FIRST_OFF_OFFSET_IN_OBJECT = UNSAFE.objectFieldOffset(LedgerSegment.class.getDeclaredField("firstOffset"));
            LAST_OFF_OFFSET_IN_OBJECT = UNSAFE.objectFieldOffset(LedgerSegment.class.getDeclaredField("lastOffset"));
        } catch (final NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /* ─── Thread-Local CRC instances ──────────────────────────────────────── */
    private static final ThreadLocal<CRC32> RECORD_CRC32_TL = ThreadLocal.withInitial(CRC32::new);
    private static final ThreadLocal<CRC32C> HEADER_CRC32C_TL = ThreadLocal.withInitial(CRC32C::new);


    /* ─── Instance Fields ─────────────────────────────────────────────────── */
    @Getter private final Path file;
    @Getter private final int capacity;
    private final MappedByteBuffer buf;
    private final boolean skipRecordCrc;

    /** Global offset of the first record in this segment. 0 if segment is empty. */
    @Getter private volatile long firstOffset;
    /** Global offset of the last record in this segment. 0 if segment is empty. */
    @Getter private volatile long lastOffset;


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
            log.error("Failed to initialize Unsafe", e);
            throw new RuntimeException("Unable to access Unsafe", e);
        }
    }

    /**
     * Creates a new, empty ledger segment file.
     * The file is pre-allocated to the given capacity and the header is written and forced to disk.
     *
     * @param file Path to the segment file to be created.
     * @param capacity Total capacity of the segment file in bytes.
     * @param skipRecordCrc If true, CRC for individual records will not be calculated or written (CRC field will be 0).
     * @return A new {@link LedgerSegment} instance.
     * @throws IOException If an I/O error occurs during file creation or mapping.
     */
    public static LedgerSegment create(final Path file, final int capacity, final boolean skipRecordCrc) throws IOException {
        if (capacity < HEADER_SIZE + MIN_RECORD_SIZE) {
            throw new IllegalArgumentException("Capacity is too small for header and one minimal record.");
        }
        try (final FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ch.truncate(capacity);

            final MappedByteBuffer map = ch.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
            map.order(ByteOrder.LITTLE_ENDIAN);

            // Initialize header fields
            map.putInt(MAGIC_POS, LedgerConstant.MAGIC);
            map.putShort(VERSION_POS, LedgerConstant.VERSION);
            map.putLong(FIRST_OFFSET_POS, 0L);
            map.putLong(LAST_OFFSET_POS, 0L);

            // CRC is calculated last, over the entire header with CRC field zeroed
            map.putInt(CRC_POS, 0);
            calculateAndWriteHeaderCrc(map);

            map.force();
            map.position(HEADER_SIZE);

            return new LedgerSegment(file, capacity, map, skipRecordCrc);
        }
    }

    /**
     * Opens an existing ledger segment file.
     * Validates the header and positions the buffer for subsequent appends.
     * This method assumes that {@link LedgerOrchestrator} has already been
     * called on the file to ensure its integrity and truncate any corrupt/partial data.
     *
     * @param file Path to the existing segment file.
     * @param skipRecordCrc If true, CRC for newly appended records will not be calculated.
     * @return A {@link LedgerSegment} instance for the existing file.
     * @throws IOException If an I/O error occurs or header validation fails.
     */
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

    private void readAndVerifyHeader() throws IOException {
        buf.position(MAGIC_POS); // Ensure we start reading from the beginning of the buffer

        final int magic = buf.getInt();
        final short version = buf.getShort();

        if (magic != LedgerConstant.MAGIC) {
            throw new IOException("Invalid magic number in segment header: " + file + ". Expected " +
                    Integer.toHexString(LedgerConstant.MAGIC) + ", got " + Integer.toHexString(magic));
        }
        if (version != LedgerConstant.VERSION) {
            throw new IOException("Unsupported version in segment header: " + file + ". Expected " +
                    LedgerConstant.VERSION + ", got " + version);
        }

        final int storedCrc = buf.getInt(CRC_POS); // Read CRC without advancing position yet
        final int calculatedCrc = calculateHeaderCrc(buf);

        if (storedCrc != calculatedCrc) {
            throw new IOException("Header CRC mismatch for segment: " + file + ". Stored: " + storedCrc + ", Calculated: " + calculatedCrc);
        }

        // Read offsets now that CRC is validated
        this.firstOffset = buf.getLong(FIRST_OFFSET_POS);
        this.lastOffset = buf.getLong(LAST_OFFSET_POS);

        buf.position(HEADER_SIZE);
    }

    private int scanToEndOfWrittenData() {
        // Start scanning after the header
        buf.position(HEADER_SIZE);
        int currentBytePosition = HEADER_SIZE;

        // Not enough space for even a minimal record
        while (currentBytePosition <= capacity - MIN_RECORD_SIZE) {
            if (buf.remaining() < MIN_RECORD_OVERHEAD) {
                break;
            }

            // Mark position before reading record length
            buf.mark();

            final int recordLength = buf.getInt();

            // Invalid length (end of data or corruption)
            if (recordLength <= 0) {
                buf.reset();
                break;
            }

            final int recordCrcPosition = buf.position();

            // Not enough for CRC + payload
            if (buf.remaining() < Integer.BYTES + recordLength) {
                buf.reset();
                break;
            }

            // Skip CRC and payload
            buf.position(recordCrcPosition + Integer.BYTES + recordLength);
            currentBytePosition = buf.position();
        }

        // buf.position() is now at the end of the last valid record found, or HEADER_SIZE if no records
        return buf.position();
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
        view.get(headerBytes); // Read the current header state

        // Zero out the CRC field in the copied bytes for calculation
        for (int i = CRC_POS; i < CRC_POS + Integer.BYTES; i++) {
            headerBytes[i] = 0;
        }

        final CRC32C crcCalculator = HEADER_CRC32C_TL.get();
        crcCalculator.reset();
        crcCalculator.update(headerBytes, 0, HEADER_SIZE);
        return (int) crcCalculator.getValue();
    }

    /**
     * Appends a single record to the segment. This operation is not necessarily durable
     * until {@link MappedByteBuffer#force()} is called.
     * This method assumes it's called by a single writer thread.
     *
     * @param data The byte array containing the record payload.
     * @return The global offset assigned to this record.
     * @throws IOException If the segment is full or an I/O error occurs.
     */
    public long append(final byte[] data) throws IOException {
        final int recordDataLength = data.length;
        final int spaceNeeded = MIN_RECORD_OVERHEAD + recordDataLength;

        if (buf.position() + spaceNeeded > capacity) {
            throw new IOException("Segment full. File: " + file +
                    ", Current Position: " + buf.position() +
                    ", Space Needed: " + spaceNeeded +
                    ", Capacity: " + capacity);
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


        /*
         * Atomically update offset fields and then the header
         * If lastOffset is 0, this is the first record globally (or for this segment after reset)
         */
        final long newGlobalOffset = (this.lastOffset == 0 && this.firstOffset == 0) ? 1 : this.lastOffset + 1;

        if (this.firstOffset == 0) {
            UNSAFE.putOrderedLong(this, FIRST_OFF_OFFSET_IN_OBJECT, newGlobalOffset);
        }
        UNSAFE.putOrderedLong(this, LAST_OFF_OFFSET_IN_OBJECT, newGlobalOffset);

        updateHeaderOnDisk();
        return newGlobalOffset;
    }

    /**
     * Appends a batch of records to the segment. This operation is not necessarily durable
     * until {@link MappedByteBuffer#force()} is called.
     * This method assumes it's called by a single writer thread.
     *
     * @param messages A list of byte arrays, each representing a record payload.
     * @return An array of global offsets assigned to each record in the batch.
     * @throws IOException If the segment is full or an I/O error occurs.
     */
    public long[] appendBatch(final List<byte[]> messages) throws IOException {
        if (messages.isEmpty()) {
            return new long[0];
        }

        int totalSpaceNeeded = 0;
        for (byte[] msg : messages) {
            totalSpaceNeeded += (MIN_RECORD_OVERHEAD + msg.length);
        }

        if (buf.position() + totalSpaceNeeded > capacity) {
            throw new IOException("Segment full for batch. File: " + file);
        }

        final long[] offsets = new long[messages.size()];
        long currentGlobalOffset = (this.lastOffset == 0 && this.firstOffset == 0) ? 0 : this.lastOffset; // Start before first new offset

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
        updateHeaderOnDisk();
        return offsets;
    }

    /**
     * Appends a single record and forces the changes to the underlying storage,
     * ensuring durability. Assumes single-threaded writer.
     *
     * @param data The byte array containing the record payload.
     * @return The global offset assigned to this record.
     * @throws IOException If the segment is full or an I/O error occurs, including during the force operation.
     */
    public long appendAndForce(final byte[] data) throws IOException {
        final long offset = append(data);

        buf.force();
        return offset;
    }

    /**
     * Appends a batch of records and forces the changes to the underlying storage,
     * ensuring durability. Assumes single-threaded writer.
     *
     * @param messages A list of byte arrays, each representing a record payload.
     * @return An array of global offsets assigned to each record in the batch.
     * @throws IOException If the segment is full or an I/O error occurs, including during the force operation.
     */
    public long[] appendBatchAndForce(final List<byte[]> messages) throws IOException {
        final long[] offsets = appendBatch(messages);

        if (messages.isEmpty()) return offsets;
        buf.force();

        return offsets;
    }

    private void updateHeaderOnDisk() {
        final int savedPosition = buf.position();
        try {
            // Update header fields in the MappedByteBuffer
            if (buf.getLong(FIRST_OFFSET_POS) == 0L && this.firstOffset != 0L) {
                buf.putLong(FIRST_OFFSET_POS, this.firstOffset);
            }

            // Recalculate and write CRC
            buf.putLong(LAST_OFFSET_POS, this.lastOffset);
            calculateAndWriteHeaderCrc(this.buf);
        } finally {
            buf.position(savedPosition);
        }
    }

    /**
     * Checks if the segment is full, meaning there isn't enough space for even a minimal record.
     * @return True if the segment is full, false otherwise.
     */
    public boolean isFull() {
        return capacity - buf.position() < MIN_RECORD_SIZE;
    }

    /**
     * Forces any changes made to this segment's content to be written to the storage device
     * and then closes the segment. The MappedByteBuffer will be unmapped.
     */
    @Override
    public void close() {
        if (buf != null) {
            try {
                buf.force();
            } catch (Exception e) {
                log.warn("Failed to force buffer during close for segment: {}", file, e);
            }
            try {
                UNSAFE.invokeCleaner(buf);
            } catch (Exception e) {
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