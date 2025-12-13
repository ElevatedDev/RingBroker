package io.ringbroker.offset;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void recoveryIgnoresTrailingGarbageRecords() throws Exception {
        final Path storage = dir.resolve("offsets");

        try (final InMemoryOffsetStore store = new InMemoryOffsetStore(storage)) {
            store.commit("t", "g", 0, 1L);
            store.commit("t", "g", 1, 2L);
        }

        final Path segment;
        try (final java.util.stream.Stream<Path> stream = java.nio.file.Files.list(storage)) {
            final Optional<Path> seg = stream
                    .filter(p -> p.toString().endsWith(io.ringbroker.ledger.constant.LedgerConstant.SEGMENT_EXT))
                    .findFirst();
            assertTrue(seg.isPresent(), "offset WAL segment should exist after commits");
            segment = seg.get();
        }

        try (final FileChannel ch = FileChannel.open(segment, StandardOpenOption.APPEND)) {
            // Append an incomplete length field to simulate a torn write.
            final ByteBuffer garbage = ByteBuffer.allocate(2);
            garbage.putShort((short) 9999);
            garbage.flip();
            ch.write(garbage);
        }

        try (final InMemoryOffsetStore recovered = new InMemoryOffsetStore(storage)) {
            assertEquals(1L, recovered.fetch("t", "g", 0));
            assertEquals(2L, recovered.fetch("t", "g", 1));
        }
    }

    @Test
    void flushesPendingCommitsOnClose() throws Exception {
        final Path storage = dir.resolve("offsets");
        final int threads = 4;
        final int perThread = 50;

        try (final InMemoryOffsetStore store = new InMemoryOffsetStore(storage)) {
            final ExecutorService exec = Executors.newFixedThreadPool(threads);
            final CountDownLatch start = new CountDownLatch(1);

            for (int t = 0; t < threads; t++) {
                final int partition = t;
                exec.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            store.commit("t", "g", partition, i);
                        }
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    return null;
                });
            }

            start.countDown();
            exec.shutdown();
            assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS), "executor did not finish");
        }

        try (final InMemoryOffsetStore recovered = new InMemoryOffsetStore(storage)) {
            IntStream.range(0, threads)
                    .forEach(partition -> assertEquals(49L, recovered.fetch("t", "g", partition)));
        }
    }
}
