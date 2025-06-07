package io.ringbroker.core.ring;

import io.ringbroker.core.sequence.Sequence;
import io.ringbroker.core.barrier.Barrier;
import io.ringbroker.core.wait.WaitStrategy;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multi‐producer, single‐consumer ring buffer.
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
    private final PaddedSequence claim = new PaddedSequence(-1);

    /**
     * @param size         power‐of‐two number of slots in the buffer
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
     * Multi‐producer safe. Each producer gets a unique sequence by incrementing atomically.
     */
    public long next() {
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
        // 1) store the entry into the ring (release semantics ensures visibility)
        final int index = (int) (seq & mask);
        ARRAY_HANDLE.setRelease(entries, index, entry);

        // 2) spin only until the previous sequence has been published.
        //    Once cursor.getValue() == (seq - 1), *we* know it's our turn.
        while (cursor.getValue() != seq - 1) {
            Thread.onSpinWait();
        }

        // 3) update cursor via a release write (no CAS contention).
        cursor.setValue(seq);

        // 4) signal the consumer that a new entry is ready.
        barrier.signal();
    }

    /**
     * Get the entry at the given sequence, waiting if necessary until it is available.
     *
     * @param seq the sequence to retrieve (must be <= published cursor)
     * @return the entry at that sequence
     * @throws InterruptedException if the waiting thread is interrupted
     */
    @SuppressWarnings("unchecked")
    public E get(final long seq) throws InterruptedException {
        barrier.waitFor(seq);
        final int index = (int) (seq & mask);
        return (E) ARRAY_HANDLE.getAcquire(entries, index);
    }

    /**
     * @return the highest published sequence in the ring buffer.
     */
    public long getCursor() {
        return cursor.getValue();
    }
}

final class PaddedSequence {
    // 7 longs of pre‐padding
    @SuppressWarnings("unused")
    private long p1, p2, p3, p4, p5, p6, p7;

    private final AtomicLong value;

    // 7 longs of post‐padding
    @SuppressWarnings("unused")
    private long p8, p9, p10, p11, p12, p13, p14;

    public PaddedSequence(final long initial) {
        this.value = new AtomicLong(initial);
    }

    /**
     * Atomically increments by one and returns the updated value.
     */
    public long incrementAndGet() {
        return value.incrementAndGet();
    }

    /**
     * Atomically adds {@code delta} and returns the updated value.
     * (Optional helper if batching is introduced later.)
     */
    public long addAndGet(final long delta) {
        return value.addAndGet(delta);
    }

    /**
     * Returns the current value.
     */
    public long get() {
        return value.get();
    }
}
