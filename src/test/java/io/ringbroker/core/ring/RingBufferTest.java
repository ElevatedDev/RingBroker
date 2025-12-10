package io.ringbroker.core.ring;

import io.ringbroker.core.wait.Blocking;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

final class RingBufferTest {

    private static void publishRange(final RingBuffer<Integer> ring,
                                     final AtomicInteger produced,
                                     final int start,
                                     final int count) {
        for (int i = 0; i < count; i++) {
            final long seq = ring.next();
            ring.publish(seq, start + i);
            produced.incrementAndGet();
        }
    }

    @Test
    void publishAndGetSingle() throws Exception {
        final RingBuffer<String> ring = new RingBuffer<>(8, new Blocking());
        final long seq = ring.next();
        ring.publish(seq, "hello");

        assertEquals(seq, ring.getCursor());
        assertEquals("hello", ring.get(seq));
    }

    @Test
    void rejectsNonPowerOfTwoSize() {
        assertThrows(IllegalArgumentException.class, () -> new RingBuffer<>(10, new Blocking()));
    }

    @Test
    void getBlocksUntilPublished() throws Exception {
        final RingBuffer<String> ring = new RingBuffer<>(4, new Blocking());
        final long seq = ring.next();

        final CompletableFuture<String> fetched = CompletableFuture.supplyAsync(() -> {
            try {
                return ring.get(seq);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(25);
        assertFalse(fetched.isDone(), "consumer should be waiting before publish");

        ring.publish(seq, "later");
        assertEquals("later", fetched.get());
    }

    @Test
    void publishBatchPreservesOrder() throws Exception {
        final RingBuffer<Integer> ring = new RingBuffer<>(8, new Blocking());
        final Integer[] payload = new Integer[]{1, 2, 3};
        final long endSeq = ring.next(payload.length);

        ring.publishBatch(endSeq, payload.length, payload);

        assertEquals(endSeq, ring.getCursor());
        assertEquals(1, ring.get(endSeq - 2));
        assertEquals(2, ring.get(endSeq - 1));
        assertEquals(3, ring.get(endSeq));
    }

    @Test
    void multiProducerSingleConsumerDeliversAll() throws Exception {
        final RingBuffer<Integer> ring = new RingBuffer<>(16, new Blocking());
        final AtomicInteger produced = new AtomicInteger();
        final AtomicInteger consumed = new AtomicInteger();

        final var pool = Executors.newFixedThreadPool(4);
        try {
            final CompletableFuture<?> p1 = CompletableFuture.runAsync(() -> publishRange(ring, produced, 0, 20), pool);
            final CompletableFuture<?> p2 = CompletableFuture.runAsync(() -> publishRange(ring, produced, 20, 20), pool);

            // Single consumer pulling 40 entries
            final CompletableFuture<?> consumer = CompletableFuture.runAsync(() -> {
                for (int i = 0; i < 40; i++) {
                    try {
                        ring.get(i);
                        consumed.incrementAndGet();
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
            });

            CompletableFuture.allOf(p1, p2, consumer).join();
            assertEquals(40, produced.get());
            assertEquals(40, consumed.get());
            assertEquals(39, ring.getCursor());
        } finally {
            pool.shutdownNow();
        }
    }
}
