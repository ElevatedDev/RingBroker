package io.ringbroker.ledger.orchestrator;

import io.ringbroker.ledger.segment.LedgerSegment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class LedgerOrchestratorTest {

    @TempDir
    Path dir;

    @Test
    void writesAndFetchesAcrossSegments() throws Exception {
        // Small capacity forces a roll to the next segment.
        final int capacity = LedgerSegment.HEADER_SIZE + 64;
        final LedgerOrchestrator orchestrator = LedgerOrchestrator.bootstrap(dir, capacity);

        final byte[] p0 = "one".getBytes(StandardCharsets.UTF_8);
        final byte[] p1 = "two".getBytes(StandardCharsets.UTF_8);

        final int batch0Bytes = p0.length + Integer.BYTES * 2;
        final int batch1Bytes = p1.length + Integer.BYTES * 2;

        orchestrator.writable(batch0Bytes).appendBatchNoOffsets(List.of(p0), batch0Bytes);

        // Force roll by requesting more than remaining space.
        orchestrator.writable(capacity).appendBatchNoOffsets(List.of(p1), batch1Bytes);

        final List<String> seen = new ArrayList<>();
        orchestrator.fetch(0, 10, (off, buf, pos, len) -> {
            final byte[] dst = new byte[len];
            buf.position(pos).get(dst);
            seen.add(new String(dst, StandardCharsets.UTF_8));
        });

        assertEquals(List.of("one", "two"), seen);
        orchestrator.close();

        final long segments = Files.list(dir).filter(p -> p.toString().endsWith(".seg")).count();
        assertTrue(segments >= 2, "rollover should create multiple segments");
    }

    @Test
    void fetchHonorsMaxMessages() throws Exception {
        final LedgerOrchestrator orchestrator = LedgerOrchestrator.bootstrap(dir, LedgerSegment.HEADER_SIZE + 512);
        final byte[] p0 = "a".getBytes(StandardCharsets.UTF_8);
        final byte[] p1 = "b".getBytes(StandardCharsets.UTF_8);
        final int batchBytes = p0.length + Integer.BYTES * 2;
        orchestrator.writable(batchBytes).appendBatchNoOffsets(List.of(p0, p1), batchBytes * 2);

        final List<String> seen = new ArrayList<>();
        orchestrator.fetch(0, 1, (off, buf, pos, len) -> {
            final byte[] dst = new byte[len];
            buf.position(pos).get(dst);
            seen.add(new String(dst, StandardCharsets.UTF_8));
        });

        assertEquals(List.of("a"), seen, "maxMessages=1 should only return first payload");
        orchestrator.close();
    }
}
