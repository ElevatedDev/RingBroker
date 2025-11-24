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

    private static final Unsafe UNSAFE = initUnsafe();
    private static final long FIRST_OFF_OFFSET_IN_OBJECT;
    private static final long LAST_OFF_OFFSET_IN_OBJECT;

    static {
        try {
            FIRST_OFF_OFFSET_IN_OBJECT = UNSAFE.objectFieldOffset(LedgerSegment.class.getDeclaredField("firstOffset"));
            LAST_OFF_OFFSET_IN_OBJECT = UNSAFE.objectFieldOffset(LedgerSegment.class.getDeclaredField("lastOffset"));
        } catch (final NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Getter private final Path file;
    @Getter private final int capacity;
    private final MappedByteBuffer buf;
    private final boolean skipRecordCrc;

    private final CRC32 recordCrc = new CRC32();
    private final CRC32C headerCrc = new CRC32C();
    private final ByteBuffer headerScratch = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);

    @Getter private volatile long firstOffset;
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
            throw new RuntimeException(e);
        }
    }

    public static LedgerSegment create(final Path file, final int capacity, final boolean skipRecordCrc) throws IOException {
        if (capacity < HEADER_SIZE + MIN_RECORD_SIZE) throw new IllegalArgumentException("Capacity too small");
        try (final FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ch.truncate(capacity);
            final MappedByteBuffer map = ch.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
            map.order(ByteOrder.LITTLE_ENDIAN);

            map.putInt(MAGIC_POS, LedgerConstant.MAGIC);
            map.putShort(VERSION_POS, LedgerConstant.VERSION);
            map.putInt(CRC_POS, 0);
            map.putLong(FIRST_OFFSET_POS, 0L);
            map.putLong(LAST_OFFSET_POS, 0L);

            writeHeaderCrcStatic(map, 0L, 0L);
            map.load(); // Pre-touch
            map.force();
            map.position(HEADER_SIZE);

            return new LedgerSegment(file, capacity, map, skipRecordCrc);
        }
    }

    public static LedgerSegment openExisting(final Path file, final boolean skipRecordCrc) throws IOException {
        try (final FileChannel ch = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            final long size = ch.size();
            if (size < HEADER_SIZE) throw new IOException("Segment too small");
            final MappedByteBuffer map = ch.map(FileChannel.MapMode.READ_WRITE, 0, size);
            map.order(ByteOrder.LITTLE_ENDIAN);
            map.load();
            return new LedgerSegment(file, (int) size, map, skipRecordCrc);
        }
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
        if (magic != LedgerConstant.MAGIC || ver != LedgerConstant.VERSION) throw new IOException("Bad magic/version in " + file);

        final int storedCrc = buf.getInt(CRC_POS);
        final long fo = buf.getLong(FIRST_OFFSET_POS);
        final long lo = buf.getLong(LAST_OFFSET_POS);

        headerScratch.clear();
        headerScratch.putInt(LedgerConstant.MAGIC).putShort(LedgerConstant.VERSION).putInt(0).putLong(fo).putLong(lo).flip();
        headerCrc.reset();
        headerCrc.update(headerScratch);
        if ((int) headerCrc.getValue() != storedCrc) throw new IOException("Header CRC mismatch in " + file);

        this.firstOffset = fo;
        this.lastOffset = lo;
    }

    private int scanToEndOfWrittenData() {
        int pos = HEADER_SIZE;
        final int maxPos = capacity - MIN_RECORD_OVERHEAD - 1;
        while (pos <= maxPos) {
            final int len = buf.getInt(pos);
            // FIX: 0 is a valid EOF marker.
            if (len == 0) break;
            if (len < 0) {
                log.warn("Corrupt negative length {} at {} in {}", len, pos, file);
                break;
            }
            if (pos + MIN_RECORD_OVERHEAD + len > capacity) {
                log.warn("Record length {} exceeds capacity at {} in {}", len, pos, file);
                break;
            }
            pos += MIN_RECORD_OVERHEAD + len;
        }
        return pos;
    }

    public boolean hasSpaceFor(int payloadBytes) {
        // Overhead = Length(4) + CRC(4) = 8 bytes
        return (capacity - buf.position()) >= (payloadBytes + 8);
    }

    public MappedByteBuffer getBuf() { return buf; }

    public long[] appendBatch(final List<byte[]> msgs) throws IOException {
        if (msgs.isEmpty()) return new long[0];

        // Pre-flight check: do we have space?
        int totalPayload = 0;
        for (byte[] m : msgs) totalPayload += (MIN_RECORD_OVERHEAD + m.length);

        if (buf.position() + totalPayload > capacity) {
            throw new IOException("Segment full for batch: " + file);
        }

        final long[] outs = new long[msgs.size()];
        long curr = (firstOffset == 0 && lastOffset == 0) ? 0L : lastOffset;
        boolean firstSet = firstOffset != 0L;
        final boolean doCrc = !skipRecordCrc;

        for (int i = 0; i < msgs.size(); i++) {
            final byte[] d = msgs.get(i);
            buf.putInt(d.length);
            if (doCrc) {
                recordCrc.reset();
                recordCrc.update(d, 0, d.length);
                buf.putInt((int) recordCrc.getValue());
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

    public long[] appendBatchAndForce(final List<byte[]> msgs) throws IOException {
        final long[] offs = appendBatch(msgs);
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

    @Override
    public void close() {
        if (buf != null) {
            try { buf.force(); } catch (Exception ignored) {}
            try { UNSAFE.invokeCleaner(buf); } catch (Exception ignored) {}
        }
    }
}