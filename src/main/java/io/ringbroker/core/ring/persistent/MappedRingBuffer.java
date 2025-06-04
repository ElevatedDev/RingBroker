package io.ringbroker.core.ring.persistent;

import io.ringbroker.core.barrier.Barrier;
import io.ringbroker.core.sequence.Sequence;
import io.ringbroker.core.wait.WaitStrategy;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Power-loss–/crash-durable multi-producer, single-consumer ring buffer backed by
 * {@linkplain java.nio.MappedByteBuffer memory-mapped} file storage.
 * <p>
 * - Every slot is of <em>fixed size</em> {@code slotSize}.  A slot stores:
 * <pre>
 *   +---------+---------------------+
 *   | int len |   len bytes data    |
 *   +---------+---------------------+
 * </pre>
 * </p>
 */
public final class MappedRingBuffer {
    private static final VarHandle INT_HANDLE;         // 4-byte length header (volatile)
    private static final VarHandle ARRAY_HANDLE;       // VarHandle for in-JVM slots (optional cache)

    static {
        try {
            INT_HANDLE   = MethodHandles.byteBufferViewVarHandle(int[].class, java.nio.ByteOrder.BIG_ENDIAN);
            ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(Object[].class);
        } catch (Exception e) { throw new ExceptionInInitializerError(e); }
    }

    private final int slots;
    private final int slotSize;
    private final long fileSize;
    private final MappedByteBuffer mmap;
    private final Barrier barrier;
    private final Sequence cursor = new Sequence(-1);          // last published
    private final AtomicLong claim = new AtomicLong(-1);       // last claimed sequence
    private final Object[] heapCache;                        // optional read cache to avoid copy

    /**
     * @param path        file path; created if missing
     * @param slots       power-of-two number of ring slots
     * @param slotSize    bytes per slot (! must fit largest message + 4)
     * @param waitStrategy consumer wait strategy
     */
    public MappedRingBuffer(final Path path,
                            final int  slots,
                            final int  slotSize,
                            final WaitStrategy waitStrategy) throws IOException {

        if (Integer.bitCount(slots) != 1)
            throw new IllegalArgumentException("slots must be power of two");

        this.slots     = slots;
        this.slotSize  = slotSize;
        this.fileSize  = (long) slots * slotSize;
        this.barrier   = new Barrier(cursor, waitStrategy);
        this.heapCache = new Object[slots];

        final EnumSet<StandardOpenOption> opts = EnumSet.of(
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
        try (FileChannel ch = FileChannel.open(path, opts)) {
            this.mmap = ch.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        }
    }

    /** Reserve the next sequence for publishing (MPSC safe). */
    public long next() { return claim.incrementAndGet(); }

    /**
     * Publish a payload at the given sequence.  Length ≤ {@code slotSize-4}.
     */
    public void publish(final long seq, final byte[] payload) {
        if (payload.length > slotSize - 4)
            throw new IllegalArgumentException("Payload too large for slot");

        final int idx = (int) (seq & (slots - 1));
        final long pos = (long) idx * slotSize;

        // Write len atomically last (release)
        mmap.position((int) pos + 4).put(payload);
        INT_HANDLE.setRelease(mmap, (int) pos, payload.length);

        // Optional heap cache for zero-copy consumer
        ARRAY_HANDLE.setRelease(heapCache, idx, payload);

        // advance cursor
        while (!cursor.cas(seq - 1, seq)) Thread.onSpinWait();
        barrier.signal();
    }

    /**
     * Returns a fresh byte[] of the payload (copy) –  wait-blocks until seq is published.
     */
    @SuppressWarnings("unchecked")
    public byte[] get(final long seq) throws InterruptedException {
        barrier.waitFor(seq);
        int idx = (int) (seq & (slots - 1));

        // Fast path: heap cache present?
        byte[] cached = (byte[]) ARRAY_HANDLE.getAcquire(heapCache, idx);
        if (cached != null) return cached;

        long pos = (long) idx * slotSize;
        int len = (int) INT_HANDLE.getAcquire(mmap, (int) pos);
        byte[] dst = new byte[len];
        mmap.position((int) pos + 4).get(dst);
        return dst;
    }

    public long getCursor() { return cursor.getValue(); }

    /** Forces dirty pages to storage; call periodically for power-loss safety. */
    public void flush() { mmap.force(); }
}
