package io.ringbroker.offset;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class InMemoryOffsetStoreTest {

    @TempDir
    Path dir;

    @Test
    void commitsArePersistedAndRecovered() throws Exception {
        final Path storage = dir.resolve("offsets");

        try (final InMemoryOffsetStore store = new InMemoryOffsetStore(storage)) {
            store.commit("orders/created", "g1", 0, 5L);
            store.commit("orders/created", "g1", 20, 7L); // forces partition array growth
            store.commit("orders/created", "g2", 1, 9L); // different group

            // Immediate read hits in-memory map
            assertEquals(5L, store.fetch("orders/created", "g1", 0));
            assertEquals(7L, store.fetch("orders/created", "g1", 20));
            assertEquals(9L, store.fetch("orders/created", "g2", 1));
        }

        // Reopen to ensure recovery from WAL.
        try (final InMemoryOffsetStore recovered = new InMemoryOffsetStore(storage)) {
            assertEquals(5L, recovered.fetch("orders/created", "g1", 0));
            assertEquals(7L, recovered.fetch("orders/created", "g1", 20));
            assertEquals(9L, recovered.fetch("orders/created", "g2", 1));
        }
    }
}
