package io.ringbroker.core.ring;

import io.ringbroker.core.barrier.Barrier;
import io.ringbroker.core.sequence.Sequence;
import io.ringbroker.core.wait.WaitStrategy;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multi-producer, single-consumer ring buffer.
 *
 * @param <E> the type of entry stored in the ring
 */
public final class RingBuffer<E> {
    private static final VarHandle ARRAY_HANDLE;

    static {
        try {
            ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(Object[].class);
        } catch (final Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Object[] entries;
    private final int mask;
    private final Sequence cursor = new Sequence(-1);       // last published sequence
    private final Barrier barrier;
    private final AtomicLong claim = new AtomicLong(-1);    // last claimed sequence (for producers)

    /**
     * @param size         power-of-two number of slots in the buffer
     * @param waitStrategy strategy for consumer wait/notification
     */
    public RingBuffer(final int size, final WaitStrategy waitStrategy) {
        if (Integer.bitCount(size) != 1) {
            throw new IllegalArgumentException("RingBuffer size must be a power of two.");
        }
        this.entries = new Object[size];
        this.mask = size - 1;
        this.barrier = new Barrier(cursor, waitStrategy);
    }

    /**
     * Reserve the next sequence number for publishing.
     * Multi-producer safe.
     */
    public long next() {
        // Atomically increment the claim counter to get a unique sequence
        return claim.incrementAndGet();
    }

    /**
     * Publish an entry at the given sequence.
     * This makes the entry visible to the consumer.
     *
     * @param seq   the sequence obtained from {@link #next()}
     * @param entry the entry to publish
     */
    public void publish(final long seq, final E entry) {
        // Set the entry in the array (release semantics to ensure visibility)
        ARRAY_HANDLE.setRelease(entries, (int) (seq & mask), entry);
        // Move the cursor to seq (only when it was seq-1, ensuring order)
        while (!cursor.cas(seq - 1, seq)) {
            Thread.onSpinWait();
        }
        // Signal any waiting consumer that a new entry is available
        barrier.signal();
    }

    /**
     * Get the entry at the given sequence, waiting if necessary until it is available.
     *
     * @param seq the sequence to retrieve (must be <= published cursor)
     * @return the entry at that sequence
     * @throws InterruptedException if interrupted while waiting
     */
    @SuppressWarnings("unchecked")
    public E get(final long seq) throws InterruptedException {
        // Wait until the ring's cursor has advanced to at least seq
        barrier.waitFor(seq);
        // Load the entry (acquire semantics to ensure we see the published entry)
        return (E) ARRAY_HANDLE.getAcquire(entries, (int) (seq & mask));
    }

    /**
     * @return the highest published sequence in the ring buffer.
     */
    public long getCursor() {
        return cursor.getValue();
    }
}
