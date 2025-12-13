package io.ringbroker.ledger.orchestrator;

import io.ringbroker.ledger.segment.LedgerSegment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

final class VirtualLogTest {

    @TempDir
    Path dir;

    @Test
    void discoversExistingEpochsAndBootstrapsNewOnDemand() throws Exception {
        final VirtualLog log = new VirtualLog(dir, LedgerSegment.HEADER_SIZE + 256);

        // Create one epoch and ensure it exists on disk.
        final LedgerOrchestrator epoch0 = log.forEpoch(0L);
        assertTrue(Files.isDirectory(log.dirForEpoch(0L)));

        // Close and recreate to simulate restart.
        log.close();

        final VirtualLog restarted = new VirtualLog(dir, LedgerSegment.HEADER_SIZE + 256);
        restarted.discoverOnDisk();
        assertTrue(restarted.hasEpoch(0L), "existing epoch should be discovered without opening");

        // Opening another epoch should not affect the discovered one.
        assertNotNull(restarted.forEpoch(1L));
        assertTrue(restarted.hasEpoch(1L));
        restarted.close();
        epoch0.close();
    }

    @Test
    void missingEpochReportsFalse() {
        final VirtualLog log = new VirtualLog(dir, LedgerSegment.HEADER_SIZE + 128);
        assertFalse(log.hasEpoch(42L));
    }
}
