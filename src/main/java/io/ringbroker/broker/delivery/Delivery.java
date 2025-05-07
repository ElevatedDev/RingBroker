package io.ringbroker.broker.delivery;

import io.ringbroker.core.ring.RingBuffer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.*;
import java.util.function.BiConsumer;

/** Streams bytes from ring to subscribers (each on its own virtual thread). */
@RequiredArgsConstructor
@Getter
public final class Delivery {
    private final RingBuffer<byte[]> ring;
    private final ExecutorService pool =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());

    public void subscribe(final long offset, final BiConsumer<Long, byte[]> h){
        pool.submit(()->{
            long seq = offset;
            try {
                for(;; seq++) {
                    final var msg = ring.get(seq);

                    h.accept(seq, msg);
                }
            } catch(final InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });
    }
}
