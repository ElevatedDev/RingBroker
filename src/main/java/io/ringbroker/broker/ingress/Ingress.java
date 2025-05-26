package io.ringbroker.broker.ingress;

import com.google.protobuf.DynamicMessage;
import io.ringbroker.core.ring.RingBuffer;
import io.ringbroker.ledger.orchestrator.LedgerOrchestrator;
import io.ringbroker.registry.TopicRegistry;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Allocation‑free, lock‑free ingress (no external libs).
 */
@Getter
public final class Ingress {
    private static final int MAX_RETRIES = 5;
    private static final int QUEUE_CAPACITY_FACTOR = 4;   // queue = FACTOR × batchSize

    private final TopicRegistry registry;
    private final RingBuffer<byte[]> ring;
    private final LedgerOrchestrator segments;
    private final ExecutorService pool;

    /*
     * Lock from MPMC bounded ring with reused array and reusable list over batchBuf.
     * This is done to eliminate the need for a separate allocation for each batch.
     */
    private final int batchSize;
    private final SlotRing queue;
    private final byte[][] batchBuf;
    private final ByteBatch batchView;

    private Ingress(final TopicRegistry registry,
                    final RingBuffer<byte[]> ring,
                    final LedgerOrchestrator segments,
                    final ExecutorService pool,
                    final int batchSize) {

        this.registry = registry;
        this.ring = ring;
        this.segments = segments;
        this.pool = pool;
        this.batchSize = batchSize;

        final int capacity = nextPowerOfTwo(batchSize * QUEUE_CAPACITY_FACTOR);
        this.queue = new SlotRing(capacity);
        this.batchBuf = new byte[batchSize][];
        this.batchView = new ByteBatch(batchBuf);
    }

    public static Ingress create(final TopicRegistry registry,
                                 final RingBuffer<byte[]> ring,
                                 final Path dataDir,
                                 final long segmentSize,
                                 final int threads,
                                 final int batchSize) throws IOException {

        final ExecutorService exec = Executors.newFixedThreadPool(
                threads, Thread.ofVirtual().factory());

        final LedgerOrchestrator mgr = LedgerOrchestrator.bootstrap(dataDir, (int) segmentSize);
        final Ingress ingress = new Ingress(registry, ring, mgr, exec, batchSize);

        /* start writer(s) */
        for (int i = 0; i < threads; i++) {
            exec.submit(ingress::writerLoop);
        }
        return ingress;
    }

    private static int nextPowerOfTwo(final int x) {
        final int highest = Integer.highestOneBit(x);
        return (x == highest) ? x : highest << 1;
    }

    /**
     * Convenience wrapper (no retries).
     */
    public void publish(final String topic, final byte[] payload) {
        publish(topic, 0, payload);
    }

    /**
     * Validate topic, route to DLQ if needed, schema-check, then enqueue.
     */
    public void publish(final String topic, final int retries, final byte[] rawPayload) {
        // 1) validate base topic
        if (!registry.contains(topic)) throw new IllegalArgumentException("topic not registered: " + topic);

        // 2) DLQ routing
        String outTopic = retries > MAX_RETRIES ? topic + ".DLQ" : topic;
        if (!registry.contains(outTopic)) throw new IllegalArgumentException("topic not registered: " + outTopic);

        // 3) schema-validate
        try {
            DynamicMessage.parseFrom(registry.descriptor(outTopic), rawPayload);
        } catch (final Exception ex) {
            outTopic = topic + ".DLQ";
            if (!registry.contains(outTopic)) throw new IllegalArgumentException("DLQ not registered: " + outTopic);
        }

        // 4) enqueue without allocation; spin if queue is momentarily full
        while (!queue.offer(rawPayload)) {
            Thread.onSpinWait();
        }
    }

    @PostConstruct
    @SuppressWarnings("unused")
    private void init() { /* DI hook – no-op */ }

    private void writerLoop() {
        try {
            while (!Thread.currentThread().isInterrupted()) {

                /* 1) wait for at least one element */
                final byte[] first = queue.poll();
                if (first == null) {
                    // queue is empty – back off briefly to avoid endless CPU burn
                    LockSupport.parkNanos(1_000);   // ≈1 µs
                    continue;
                }

                /* 2) drain up to batchSize elements in total */
                int count = 0;
                batchBuf[count++] = first;

                while (count < batchSize) {
                    final byte[] next = queue.poll();
                    if (next == null) break;
                    batchBuf[count++] = next;
                }

                /* 3) expose array as List<byte[]> without copying */
                batchView.setSize(count);

                /* 4) disk append */
                segments.writable().appendBatch(batchView);

                /* 5) publish to downstream ring */
                for (int i = 0; i < count; i++) {
                    final long seq = ring.next();
                    ring.publish(seq, batchBuf[i]);
                    batchBuf[i] = null; // reclaim slot for next batch
                }
            }
        } catch (final IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public void close() throws IOException {
        this.segments.writable().close();
    }

    /*
     * Allocation‑free bounded lock‑free multi‑producer / multi‑consumer queue
     * (heavily simplified Vyukov algorithm).
     * Only *one* array of references is allocated once in the constructor.
     */
    static final class SlotRing {
        private static final VarHandle SEQ, BUF;

        static {
            SEQ = MethodHandles.arrayElementVarHandle(long[].class);
            BUF = MethodHandles.arrayElementVarHandle(byte[][].class);
        }

        private final int mask;
        private final long[] seq;
        private final byte[][] buf;
        private final AtomicLong tail = new AtomicLong();
        private final AtomicLong head = new AtomicLong();

        SlotRing(int capacityPow2) {
            mask = capacityPow2 - 1;
            seq  = new long[capacityPow2];
            buf  = new byte[capacityPow2][];
            for (int i = 0; i < capacityPow2; i++) seq[i] = i;
        }

        boolean offer(byte[] e) {
            long t;
            while (true) {
                t = tail.get();
                long idx = t & mask;
                long s   = (long) SEQ.getVolatile(seq, (int) idx);
                long dif = s - t;
                if (dif == 0) {
                    if (tail.compareAndSet(t, t + 1)) break;
                } else if (dif < 0) {
                    return false;              // queue full
                } else {
                    Thread.onSpinWait();
                }
            }
            int i = (int) (t & mask);
            BUF.setRelease(buf, i, e);         // ① write payload
            SEQ.setRelease(seq, i, t + 1);     // ② publish slot
            return true;
        }

        byte[] poll() {
            long h;
            while (true) {
                h = head.get();
                long idx = h & mask;
                long s   = (long) SEQ.getVolatile(seq, (int) idx);
                long dif = s - (h + 1);
                if (dif == 0) {
                    if (head.compareAndSet(h, h + 1)) break;
                } else if (dif < 0) {
                    return null;               // queue empty
                } else {
                    Thread.onSpinWait();
                }
            }
            int i = (int) (h & mask);
            byte[] e = (byte[]) BUF.getAcquire(buf, i);
            SEQ.setRelease(seq, i, h + mask + 1); // ③ mark slot empty
            BUF.set(buf, i, null);
            return e;
        }
    }



    /*
     * Cache‑line‑padded AtomicLong to stop false sharing between head & tail.
     */
    @SuppressWarnings("unused")
    private static final class PaddedAtomicLong extends AtomicLong {
        // left padding
        volatile long p1, p2, p3, p4, p5, p6, p7;
        PaddedAtomicLong(final long initial) { super(initial); }
        // right padding
        volatile long q1, q2, q3, q4, q5, q6, q7;
    }

    /*
     * Tiny reusable List<byte[]> implementation backed by the reusable batchBuf array.
     */
    private static final class ByteBatch extends AbstractList<byte[]> {
        private final byte[][] backing;
        @Setter private int size;

        ByteBatch(final byte[][] backing) {
            this.backing = backing;
        }

        @Override
        public byte[] get(final int index) {
            if (index >= size) throw new IndexOutOfBoundsException();
            return backing[index];
        }

        @Override
        public int size() {
            return size;
        }
    }
}
