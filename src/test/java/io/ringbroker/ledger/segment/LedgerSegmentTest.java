package io.ringbroker.ledger.segment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

final class LedgerSegmentTest {

    @TempDir
    Path dir;

    @Test
    void appendAndVisitPreservesOffsets() throws Exception {
        final Path segFile = dir.resolve("seg.seg");
        final LedgerSegment segment = LedgerSegment.create(segFile, LedgerSegment.HEADER_SIZE + 512, false, -1L);

        final List<byte[]> payloads = List.of("a".getBytes(), "bb".getBytes(), "ccc".getBytes());
        final int totalBytes = payloads.stream().mapToInt(b -> b.length + Integer.BYTES * 2).sum();

        segment.appendBatchNoOffsets(payloads, totalBytes);
        assertEquals(0L, segment.getFirstOffset());
        assertEquals(2L, segment.getLastOffset());

        final AtomicInteger seen = new AtomicInteger();
        segment.visitFromOffset(0, 3, (off, buf, pos, len) -> {
            final byte[] dst = new byte[len];
            buf.position(pos).get(dst, 0, len);
            assertEquals(payloads.get((int) off).length, len);
            seen.incrementAndGet();
        });
        assertEquals(3, seen.get());

        segment.close();
        assertTrue(Files.exists(LedgerSegment.indexPathForSegment(segFile)), "dense index should be created on close");

        final LedgerSegment reopened = LedgerSegment.openExisting(segFile, false);
        assertEquals(0L, reopened.getFirstOffset());
        assertEquals(2L, reopened.getLastOffset());
        reopened.close();
    }

    @Test
    void visitFromOffsetReturnsZeroWhenBeyondTail() throws Exception {
        final Path segFile = dir.resolve("empty.seg");
        final LedgerSegment segment = LedgerSegment.create(segFile, LedgerSegment.HEADER_SIZE + 256, false, -1L);

        // No records written.
        assertEquals(0, segment.visitFromOffset(0, 10, (o, b, p, l) -> {
        }));
        segment.close();
    }

    @Test
    void hasSpaceForReflectsRemainingCapacity() throws IOException {
        final Path segFile = dir.resolve("capacity.seg");
        final LedgerSegment segment = LedgerSegment.create(segFile, LedgerSegment.HEADER_SIZE + 64, false, -1L);

        assertTrue(segment.hasSpaceFor(8));
        segment.appendBatchNoOffsets(List.of("12345678".getBytes()), 8 + Integer.BYTES * 2);
        assertFalse(segment.hasSpaceFor(64));
        segment.close();
    }
}
