package io.ringbroker.cluster.metadata;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class LogConfigurationTest {

    @Test
    void activeEpochReflectsHighestConfigVersion() {
        final EpochPlacement placement = new EpochPlacement(0L, List.of(1, 2), 2);
        final EpochMetadata epoch0 = new EpochMetadata(0L, 0L, 10L, placement, 1L);
        final EpochMetadata epoch1 = new EpochMetadata(1L, 11L, 20L, placement.withEpoch(1L), 2L);

        final LogConfiguration cfg = new LogConfiguration(1, 2L, List.of(epoch0, epoch1));
        assertEquals(epoch1, cfg.activeEpoch());
        assertEquals(epoch0, cfg.epoch(0L));
        assertEquals(epoch1, cfg.epoch(1L));
    }
}
