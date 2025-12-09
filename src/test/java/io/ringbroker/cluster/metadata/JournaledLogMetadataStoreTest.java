package io.ringbroker.cluster.metadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class JournaledLogMetadataStoreTest {

    @TempDir
    Path tmp;

    @Test
    void persistsAndReloadsEpochs() throws IOException {
        final EpochPlacement placement0 = new EpochPlacement(0L, List.of(1, 2, 3), 2);
        final JournaledLogMetadataStore store = new JournaledLogMetadataStore(tmp);

        final LogConfiguration cfg0 = store.bootstrapIfAbsent(7, placement0, 0);
        assertEquals(1L, cfg0.configVersion());
        assertEquals(0L, cfg0.activeEpoch().epoch());
        assertEquals(-1L, cfg0.activeEpoch().endSeq());

        final EpochPlacement placement1 = new EpochPlacement(1L, List.of(2, 3, 4), 2);
        final LogConfiguration cfg1 = store.sealAndCreateEpoch(7, 0L, 42L, placement1, 1L, 99L);
        assertEquals(3L, cfg1.configVersion());
        assertEquals(1L, cfg1.activeEpoch().epoch());
        assertEquals(99L, cfg1.activeEpoch().tieBreaker());

        // Reload from disk
        final JournaledLogMetadataStore reload = new JournaledLogMetadataStore(tmp);
        final LogConfiguration reCfg = reload.current(7).orElseThrow();
        assertEquals(3L, reCfg.configVersion());
        assertEquals(2, reCfg.epochs().size());
        assertEquals(42L, reCfg.epoch(0L).endSeq());
        assertEquals(99L, reCfg.activeEpoch().tieBreaker());
        assertEquals(List.of(2, 3, 4), reCfg.activeEpoch().placement().getStorageNodes());
    }

    @Test
    void applyRemoteHonorsHigherConfigVersion() throws IOException {
        final EpochPlacement placement0 = new EpochPlacement(0L, List.of(1, 2), 1);
        final JournaledLogMetadataStore store = new JournaledLogMetadataStore(tmp);
        store.bootstrapIfAbsent(3, placement0, 0);

        // Lower version should be ignored
        final LogConfiguration lower = new LogConfiguration(3, 0L, List.of(
                new EpochMetadata(0L, 0L, -1L, placement0, 0L)
        ));
        store.applyRemote(lower);
        assertEquals(1L, store.current(3).orElseThrow().configVersion());

        // Higher version should replace
        final EpochPlacement placement1 = new EpochPlacement(1L, List.of(9), 1);
        final LogConfiguration higher = new LogConfiguration(3, 5L, List.of(
                new EpochMetadata(1L, 10L, -1L, placement1, 7L)
        ));
        store.applyRemote(higher);
        final LogConfiguration cur = store.current(3).orElseThrow();
        assertEquals(5L, cur.configVersion());
        assertEquals(1L, cur.activeEpoch().epoch());
        assertEquals(7L, cur.activeEpoch().tieBreaker());
    }
}
