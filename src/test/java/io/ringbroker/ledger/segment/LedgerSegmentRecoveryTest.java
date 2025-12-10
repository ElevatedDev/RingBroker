package io.ringbroker.ledger.segment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class LedgerSegmentRecoveryTest {

    @TempDir
    Path dir;

    @Test
    void recoversFromTornRecordByTruncating() throws Exception {
        final Path segFile = dir.resolve("torn.seg");
        final LedgerSegment seg = LedgerSegment.create(segFile, LedgerSegment.HEADER_SIZE + 256, false, -1L);
        seg.appendBatchNoOffsets(java.util.List.of("ok".getBytes()), Integer.BYTES * 2 + 2);
        seg.close();

        // Corrupt by appending partial record bytes without header/CRC.
        try (final FileChannel ch = FileChannel.open(segFile, StandardOpenOption.WRITE)) {
            ch.position(ch.size());
            final ByteBuffer partial = ByteBuffer.allocate(3).order(ByteOrder.LITTLE_ENDIAN);
            partial.put((byte) 0xFF).put((byte) 0xEE).put((byte) 0xDD).flip();
            ch.write(partial);
        }

        // Recovery should truncate the torn bytes and reopen cleanly.
        final LedgerSegment recovered = LedgerSegment.openExisting(segFile, false);
        assertEquals(0L, recovered.getFirstOffset());
        assertEquals(0L, recovered.getLastOffset());
        recovered.close();
    }
}
