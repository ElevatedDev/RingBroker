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
import java.util.EnumSet;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;

/**
 * Append-only, memory-mapped segment optimised for single-threaded writers.
 */
@Slf4j
public final class LedgerSegment implements AutoCloseable {

    /* ── layout ─────────────────────────────────────────────────────────── */
    private static final int MAGIC_POS = 0;                               // int  (4)
    private static final int VERSION_POS = MAGIC_POS + Integer.BYTES;       // short(2)
    private static final int CRC_POS = VERSION_POS + Short.BYTES;       // int  (4)
    private static final int FIRST_OFFSET_POS = CRC_POS + Integer.BYTES;         // long (8)
    private static final int LAST_OFFSET_POS = FIRST_OFFSET_POS + Long.BYTES;   // long (8)

    private static final int HEADER_SIZE = LAST_OFFSET_POS + Long.BYTES;           // 26 bytes

    private static final Unsafe UNSAFE = initUnsafe();

    private static final long FIRST_OFF_OFFSET;
    private static final long LAST_OFF_OFFSET;

    private static final ThreadLocal<CRC32> CRC32_TL = ThreadLocal.withInitial(CRC32::new);
    private static final ThreadLocal<CRC32C> CRC32C_TL = ThreadLocal.withInitial(CRC32C::new);

    private static final int MIN_RECORD_SIZE = Integer.BYTES + Integer.BYTES + 1; // length + crc + min payload

    static {
        try {
            FIRST_OFF_OFFSET = fieldOffset(LedgerSegment.class.getDeclaredField("firstOffset"));
            LAST_OFF_OFFSET = fieldOffset(LedgerSegment.class.getDeclaredField("lastOffset"));
        } catch (final NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /* ── instance fields ────────────────────────────────────────────────── */
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

    private LedgerSegment(final Path file, final int capacity, final MappedByteBuffer buf, final boolean skipRecordCrc) throws IOException {
        this.file = file;
        this.capacity = capacity;
        this.buf = buf;
        this.skipRecordCrc = skipRecordCrc;

        readHeader();
    }

    private static long fieldOffset(final java.lang.reflect.Field f) {
        return UNSAFE.objectFieldOffset(f);
    }

    /* ── factory methods ────────────────────────────────────────────────── */
    public static LedgerSegment create(final Path file, final int capacity,
                                       final boolean skipRecordCrc) throws IOException {
        try (final FileChannel ch = FileChannel.open(file, EnumSet.of(
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        ))) {
            ch.truncate(capacity);

            final MappedByteBuffer map = ch.map(FileChannel.MapMode.READ_WRITE, 0, capacity);

            map.order(ByteOrder.LITTLE_ENDIAN);

            // header skeleton
            map.putInt(LedgerConstant.MAGIC)      // MAGIC
                    .putShort(LedgerConstant.VERSION)  // VERSION
                    .putInt(0)                         // CRC placeholder
                    .putLong(0L)                       // firstOffset
                    .putLong(0L);                      // lastOffset

            writeHeaderCrc(map);

            map.position(HEADER_SIZE);
            map.force();

            return new LedgerSegment(file, capacity, map, skipRecordCrc);
        }
    }

    public static LedgerSegment openExisting(final Path file, final boolean skipRecordCrc) throws IOException {
        try (final FileChannel ch = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            final MappedByteBuffer map = ch.map(FileChannel.MapMode.READ_WRITE, 0, ch.size());

            map.order(ByteOrder.LITTLE_ENDIAN);

            final LedgerSegment seg = new LedgerSegment(file, (int) ch.size(), map, skipRecordCrc);
            map.position((int) ch.size());

            return seg;
        }
    }

    /* ── public API ─────────────────────────────────────────────────────── */

    private static void writeHeaderCrc(final MappedByteBuffer map) {
        final ByteBuffer dup = map.duplicate().order(ByteOrder.LITTLE_ENDIAN);

        dup.position(0).limit(HEADER_SIZE);

        final byte[] tmp = new byte[HEADER_SIZE];

        dup.get(tmp);

        // zero CRC field
        for (int i = CRC_POS; i < CRC_POS + Integer.BYTES; i++) tmp[i] = 0;

        final CRC32C crc = CRC32C_TL.get();

        crc.reset();
        crc.update(tmp, 0, tmp.length);

        map.putInt(CRC_POS, (int) crc.getValue());
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

    /**
     * Append a single record; caller must be the only writer thread.
     */
    public long append(final byte[] data) throws IOException {
        final int needed = Integer.BYTES + Integer.BYTES + data.length;

        if (buf.position() + needed > capacity) throw new IOException("segment full");

        // write length
        buf.putInt(data.length);

        // write CRC (or zero)
        if (!skipRecordCrc) {
            final CRC32 crc = CRC32_TL.get();

            crc.reset();
            crc.update(data, 0, data.length);

            buf.putInt((int) crc.getValue());
        } else {
            buf.putInt(0);
        }

        // payload
        buf.put(data);

        final long offset = lastOffset + 1;
        UNSAFE.putOrderedLong(this, LAST_OFF_OFFSET, offset);

        if (firstOffset == 0) UNSAFE.putOrderedLong(this, FIRST_OFF_OFFSET, offset);

        updateHeader(offset);

        return offset;
    }

    /**
     * Batch append; still single-threaded writer assumption.
     */
    public long[] appendBatch(final List<byte[]> messages) throws IOException {
        final int total = messages.stream().mapToInt(m -> Integer.BYTES + Integer.BYTES + m.length).sum();

        if (buf.position() + total > capacity) throw new IOException("segment full");

        final long[] offsets = new long[messages.size()];

        for (int i = 0; i < messages.size(); i++) {
            final byte[] m = messages.get(i);

            buf.putInt(m.length);

            if (!skipRecordCrc) {
                final CRC32 crc = CRC32_TL.get();

                crc.reset();
                crc.update(m, 0, m.length);

                buf.putInt((int) crc.getValue());
            } else {
                buf.putInt(0);
            }

            buf.put(m);

            final long offset = lastOffset + 1;
            UNSAFE.putOrderedLong(this, LAST_OFF_OFFSET, offset);

            if (firstOffset == 0 && i == 0) UNSAFE.putOrderedLong(this, FIRST_OFF_OFFSET, offset);

            offsets[i] = offset;
        }

        updateHeader(lastOffset);

        return offsets;
    }

    /**
     * True if there isn’t even space for minimal record.
     */
    public boolean isFull() {
        return capacity - buf.position() < MIN_RECORD_SIZE;
    }

    @Override
    public void close() {
        buf.force();
        UNSAFE.invokeCleaner(buf);
    }

    private void readHeader() throws IOException {
        buf.position(MAGIC_POS);

        final int magic = buf.getInt();
        final short ver = buf.getShort();

        if (magic != LedgerConstant.MAGIC || ver != LedgerConstant.VERSION)
            throw new IOException("bad header: " + file);

        final int storedCrc = buf.getInt();
        final int posSnap = buf.position();

        writeHeaderCrc(buf);

        buf.position(CRC_POS);

        final int calc = buf.getInt();
        if (calc != storedCrc) throw new IOException("header CRC mismatch: " + file);

        buf.position(posSnap);

        firstOffset = buf.getLong();
        lastOffset = buf.getLong();

        buf.position(HEADER_SIZE);
    }

    private void updateHeader(final long newLastOffset) {
        final int savePos = buf.position();

        // firstOffset (write once)
        if (buf.getLong(FIRST_OFFSET_POS) == 0L) buf.putLong(FIRST_OFFSET_POS, firstOffset);

        // last offset always
        buf.putLong(LAST_OFFSET_POS, newLastOffset);
        writeHeaderCrc(buf);
        buf.position(savePos);
    }
}