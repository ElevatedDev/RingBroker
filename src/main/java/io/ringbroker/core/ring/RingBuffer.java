package io.ringbroker.core.ring;

import io.ringbroker.core.barrier.Barrier;
import io.ringbroker.core.sequence.Sequence;
import io.ringbroker.core.wait.WaitStrategy;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multi‐producer, single‐consumer ring buffer.
 * <p>
 * Optimized for batch claims and batch publishes:
 * {@link #next(int)} + {@link #publishBatch(long, int, Object[])}.
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
    private final Sequence cursor = new Sequence(-1L);
    private final Barrier barrier;
    private final PaddedSequence claim = new PaddedSequence(-1L);

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
     * <p>
     * Multi‐producer safe: each producer gets a unique seq.
     *
     * @return the claimed sequence
     */
    public long next() {
        return claim.incrementAndGet();
    }

    /**
     * Reserve {@code count} consecutive sequence numbers in one go.
     * <p>
     * Your batch will occupy
     * {@code [endSeq - count + 1 .. endSeq]}.
     *
     * @param count number of slots to claim
     * @return the highest sequence in the reserved range
     */
    public long next(final int count) {
        return claim.addAndGet(count);
    }

    /**
     * Publish a single entry at the given sequence.
     *
     * @param seq   the sequence obtained from {@link #next()}
     * @param entry the entry to publish
     */
    public void publish(final long seq, final E entry) {
        final int idx = (int) (seq & mask);
        ARRAY_HANDLE.setRelease(entries, idx, entry);

        // wait our turn
        while (cursor.getValue() != seq - 1) {
            Thread.onSpinWait();
        }

        cursor.setValue(seq);
        barrier.signal();
    }

    /**
     * Publish a batch of {@code count} entries whose highest sequence is {@code endSeq}.
     * <p>
     * Entries must be in order in the {@code batch} array.
     *
     * @param endSeq highest sequence in this batch
     * @param count  number of entries
     * @param batch  array of entries, in sequence order
     */
    @SuppressWarnings("unchecked")
    public void publishBatch(final long endSeq, final int count, final E[] batch) {
        final Object[] localEntries = this.entries;
        final int m = this.mask;

        long seq = endSeq - count + 1;
        for (int i = 0; i < count; i++, seq++) {
            ARRAY_HANDLE.setRelease(localEntries, (int) (seq & m), batch[i]);
        }

        // only wait for the first slot in the batch
        final long first = endSeq - count + 1;
        while (cursor.getValue() != first - 1) {
            Thread.onSpinWait();
        }

        // single cursor advance
        cursor.setValue(endSeq);
        barrier.signal();
    }

    /**
     * Retrieve an entry, blocking until it’s published.
     *
     * @param seq the sequence to retrieve (≤ published cursor)
     * @return the entry at that sequence
     * @throws InterruptedException if interrupted while waiting
     */
    @SuppressWarnings("unchecked")
    public E get(final long seq) throws InterruptedException {
        barrier.waitFor(seq);
        return (E) ARRAY_HANDLE.getAcquire(entries, (int) (seq & mask));
    }

    /**
     * @return the highest published sequence
     */
    public long getCursor() {
        return cursor.getValue();
    }
}

final class PaddedSequence {
    private final AtomicLong value;
    // 7 longs of pre‐padding
    @SuppressWarnings("unused")
    private long p1, p2, p3, p4, p5, p6, p7;
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
