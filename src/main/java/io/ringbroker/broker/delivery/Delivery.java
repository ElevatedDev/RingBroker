package io.ringbroker.broker.delivery;

import io.ringbroker.core.ring.memory.RingBuffer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

/**
 * Delivery is responsible for streaming messages from a {@link RingBuffer} to subscribers.
 * <p>
 * Each subscriber is handled on its own virtual thread, allowing for efficient and scalable delivery.
 * Subscribers receive messages starting from a specified offset, and each message is delivered
 * along with its sequence number.
 * </p>
 *
 * <p>
 * The class uses a virtual-thread-based {@link ExecutorService} to manage subscriber tasks.
 * </p>
 */
@Getter
@RequiredArgsConstructor
public final class Delivery {

    /**
     * The ring buffer from which messages are streamed to subscribers.
     */
    private final RingBuffer<byte[]> ring;

    /**
     * Executor service that runs each subscriber on a separate virtual thread.
     */
    private final ExecutorService pool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());

    /**
     * Subscribes a consumer to the ring buffer, starting at the given offset.
     * <p>
     * The consumer will be invoked for each message, receiving the sequence number and the message bytes.
     * The subscription runs on a dedicated virtual thread and continues until interrupted.
     * </p>
     *
     * @param offset   the starting sequence number in the ring buffer
     * @param consumer the handler to process each message and its sequence number
     */
    public void subscribe(final long offset, final BiConsumer<Long, byte[]> consumer) {
        pool.submit(() -> {
            long sequence = offset;

            try {
                for (; ; sequence++) {
                    final var message = ring.get(sequence);

                    consumer.accept(sequence, message);
                }
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });
    }
}
