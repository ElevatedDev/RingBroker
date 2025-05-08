package io.ringbroker.ledger.segment;

import io.ringbroker.ledger.constant.LedgerConstant;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.zip.CRC32C;

/**
 * A single append-only, memory-mapped log segment.
 * Header layout (little-endian):
 * [MAGIC(4)] [VERSION(2)] [CRC(4)] [firstOffset(8)] [lastOffset(8)]
 */
@Slf4j
public final class LedgerSegment implements AutoCloseable {
    private static final int HEADER_SIZE = 4 + 2 + 4 + 8 + 8;

    private final Path file;
    @Getter
    private final int capacity;
    private final MappedByteBuffer buf;
    @Getter
    private long firstOffset;
    @Getter
    private long lastOffset;

    public static LedgerSegment create(final Path file, final int capacity) throws IOException {
        try (final FileChannel ch = FileChannel.open(file,
                EnumSet.of(
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE))) {

            ch.truncate(capacity);
            final MappedByteBuffer map = ch.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
            map.order(java.nio.ByteOrder.LITTLE_ENDIAN);

            // write header skeleton
            map.putInt(LedgerConstant.MAGIC)
                    .putShort(LedgerConstant.VERSION)
                    .putInt(0)     // CRC placeholder
                    .putLong(0L)   // firstOffset placeholder
                    .putLong(0L);  // lastOffset placeholder

            // compute & write initial CRC
            final ByteBuffer headerCopy = map.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN);
            headerCopy.position(0).limit(HEADER_SIZE);
            final byte[] headerBytes = new byte[HEADER_SIZE];
            headerCopy.get(headerBytes);
            // zero out CRC bytes
            for (int i = 4 + 2; i < 4 + 2 + 4; i++) headerBytes[i] = 0;
            final CRC32C crc = new CRC32C();

            crc.update(headerBytes);
            map.position(4 + 2);
            map.putInt((int) crc.getValue());

            map.force();
            return new LedgerSegment(file, capacity, map);
        }
    }

    public static LedgerSegment openExisting(final Path file) throws IOException {
        final FileChannel ch = FileChannel.open(file,
                EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE));
        final MappedByteBuffer map = ch.map(FileChannel.MapMode.READ_WRITE, 0, ch.size());
        map.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        return new LedgerSegment(file, (int) ch.size(), map);
    }

    private LedgerSegment(final Path file, final int capacity, final MappedByteBuffer map) throws IOException {
        this.file = file;
        this.capacity = capacity;
        this.buf = map;
        readHeader();
    }

    /**
     * Append a single record (uses shared header update).
     */
    public synchronized long append(final byte[] data) throws IOException {
        final int required = Integer.BYTES + data.length;
        if (buf.position() + required > capacity) {
            throw new IOException("segment full");
        }
        buf.putInt(data.length).put(data);
        final long offset = ++lastOffset;
        updateHeaderCrcAndOffset(offset);
        return offset;
    }

    /**
     * New: append a batch of records in one go (one CRC + force).
     */
    public synchronized long[] appendBatch(final java.util.List<byte[]> msgs) throws IOException {
        final int total = msgs.stream().mapToInt(b -> Integer.BYTES + b.length).sum();
        final int pos = buf.position();
        if (pos + total > capacity) {
            throw new IOException("segment full");
        }

        final long[] offsets = new long[msgs.size()];
        for (int i = 0; i < msgs.size(); i++) {
            final byte[] b = msgs.get(i);
            buf.putInt(b.length).put(b);
            offsets[i] = ++lastOffset;
        }

        updateHeaderCrcAndOffset(lastOffset);
        return offsets;
    }

    public boolean isFull() {
        return buf.position() + Integer.BYTES >= capacity;
    }

    @Override
    public void close() {
        buf.force();
    }

    private void readHeader() throws IOException {
        buf.position(0);
        final int magic = buf.getInt();
        final short ver = buf.getShort();
        if (magic != LedgerConstant.MAGIC || ver != LedgerConstant.VERSION) {
            throw new IOException("Invalid segment header: " + file);
        }

        final int storedCrc = buf.getInt();
        final ByteBuffer headerCopy = buf.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN);
        headerCopy.position(0).limit(HEADER_SIZE);
        final byte[] bytes = new byte[HEADER_SIZE];
        headerCopy.get(bytes);
        for (int i = 4 + 2; i < 4 + 2 + 4; i++) bytes[i] = 0;
        final CRC32C crc = new CRC32C();
        crc.update(bytes);
        if ((int) crc.getValue() != storedCrc) {
            throw new IOException("Header CRC mismatch: " + file);
        }

        firstOffset = buf.getLong();
        lastOffset = buf.getLong();
        buf.position(Math.max(HEADER_SIZE, buf.position()));
    }

    /**
     * Shared helper: rewrite lastOffset + CRC, then restore buffer pos.
     */
    private void updateHeaderCrcAndOffset(final long offset) {
        final int payloadPos = buf.position();                // remember write pos

        // write new lastOffset
        buf.position(4 + 2 + 4 + 8);                    // skip MAGIC,VER,CRC,firstOffset
        buf.putLong(offset);

        // recompute CRC over header
        final ByteBuffer copy = buf.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN);
        copy.position(0).limit(HEADER_SIZE);
        final byte[] header = new byte[HEADER_SIZE];
        copy.get(header);

        // zero out CRC bytes
        for (int i = 4 + 2; i < 4 + 2 + 4; i++) header[i] = 0;
        final CRC32C crc = new CRC32C();
        crc.update(header);

        // write fresh CRC
        buf.position(4 + 2);
        buf.putInt((int) crc.getValue());

        buf.force();
        buf.position(payloadPos);
    }
}
