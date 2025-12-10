package io.ringbroker.ledger.segment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

final class DenseOffsetIndexTest {

    @TempDir
    Path dir;

    @Test
    void openIfValidReturnsNullOnMismatch() throws Exception {
        final Path segFile = dir.resolve("seg.seg");
        final LedgerSegment seg = LedgerSegment.create(segFile, LedgerSegment.HEADER_SIZE + 256, false, -1L);
        seg.appendBatchNoOffsets(List.of("a".getBytes()), Integer.BYTES * 2 + 1);
        seg.buildDenseIndexIfMissingOrStale();
        seg.close();

        final Path idxPath = LedgerSegment.indexPathForSegment(segFile);
        assertNotNull(LedgerSegment.DenseOffsetIndex.openIfValid(idxPath, 1));
        assertNull(LedgerSegment.DenseOffsetIndex.openIfValid(idxPath, 2), "mismatched count should yield null");
    }
}
