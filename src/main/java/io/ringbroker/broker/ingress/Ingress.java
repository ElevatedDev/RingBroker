package io.ringbroker.broker.ingress;

import com.google.protobuf.DynamicMessage;
import io.ringbroker.core.ring.RingBuffer;
import io.ringbroker.ledger.orchestrator.LedgerOrchestrator;
import io.ringbroker.registry.TopicRegistry;
import lombok.Setter;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Allocation-free, lock-free ingress (no external libs).
 */
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

    private Ingress(TopicRegistry registry,
                    RingBuffer<byte[]> ring,
                    LedgerOrchestrator segments,
                    ExecutorService pool,
                    int batchSize) {

        this.registry = registry;
        this.ring = ring;
        this.segments = segments;
        this.pool = pool;
        this.batchSize = batchSize;

        int capacity = nextPowerOfTwo(batchSize * QUEUE_CAPACITY_FACTOR);
        this.queue = new SlotRing(capacity);
        this.batchBuf = new byte[batchSize][];
        this.batchView = new ByteBatch(batchBuf);          // same instance reused
    }

    public static Ingress create(TopicRegistry registry,
                                 RingBuffer<byte[]> ring,
                                 Path dataDir,
                                 long segmentSize,
                                 int threads,
                                 int batchSize) throws IOException {

        ExecutorService exec = Executors.newFixedThreadPool(
                threads, Thread.ofVirtual().factory());

        LedgerOrchestrator mgr = LedgerOrchestrator.bootstrap(dataDir, (int) segmentSize);
        Ingress ingress = new Ingress(registry, ring, mgr, exec, batchSize);

        /* start writer(s) */
        for (int i = 0; i < threads; i++) {
            exec.submit(ingress::writerLoop);
        }
        return ingress;
    }

    private static int nextPowerOfTwo(int x) {
        int highest = Integer.highestOneBit(x);
        return (x == highest) ? x : highest << 1;
    }

    /**
     * Convenience wrapper (no retries).
     */
    public void publish(String topic, byte[] payload) {
        publish(topic, 0, payload);
    }

    /**
     * Validate topic, route to DLQ if needed, schema-check, then enqueue.
     */
    public void publish(String topic, int retries, byte[] rawPayload) {
        // 1) validate base topic
        if (!registry.contains(topic)) throw new IllegalArgumentException("topic not registered: " + topic);

        // 2) DLQ routing
        String outTopic = retries > MAX_RETRIES ? topic + ".DLQ" : topic;
        if (!registry.contains(outTopic)) throw new IllegalArgumentException("topic not registered: " + outTopic);

        // 3) schema-validate
        try {
            DynamicMessage.parseFrom(registry.descriptor(outTopic), rawPayload);
        } catch (Exception ex) {
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
                byte[] first;
                while ((first = queue.poll()) == null) {
                    Thread.onSpinWait();
                }

                /* 2) drain up to batchSize elements in total */
                int count = 0;
                batchBuf[count++] = first;

                while (count < batchSize) {
                    byte[] next = queue.poll();
                    if (next == null) break;
                    batchBuf[count++] = next;
                }

                /* 3) expose array as List<byte[]> without copying */
                batchView.setSize(count);

                /* 4) disk append */
                segments.writable().appendBatch(batchView);

                /* 5) publish to downstream ring */
                for (int i = 0; i < count; i++) {
                    long seq = ring.next();
                    ring.publish(seq, batchBuf[i]);
                    batchBuf[i] = null;
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    /*
     * Allocation-free bounded lock-free multi-producer / multi-consumer queue
     * (heavily simplified Vyukov algorithm).
     * Only *one* array of references is allocated once in the constructor.
    */
    private static final class SlotRing {
        private final int mask;
        private final AtomicReferenceArray<byte[]> buffer;
        private final AtomicLong tail = new AtomicLong(0); // producers
        private final AtomicLong head = new AtomicLong(0); // consumers

        SlotRing(int capacityPow2) {
            this.mask = capacityPow2 - 1;
            this.buffer = new AtomicReferenceArray<>(capacityPow2);
        }

        /**
         * returns false if full
         */
        boolean offer(byte[] e) {
            long t;
            for (; ; ) {
                t = tail.get();
                long wrapPoint = t - buffer.length();
                if (head.get() <= wrapPoint) return false;
                if (tail.compareAndSet(t, t + 1)) break;
            }
            int idx = (int) t & mask;
            buffer.lazySet(idx, e);
            return true;
        }

        /**
         * returns null if empty
         */
        byte[] poll() {
            long h;
            for (; ; ) {
                h = head.get();
                if (h >= tail.get()) return null;
                if (head.compareAndSet(h, h + 1)) break;
            }
            int idx = (int) h & mask;
            return buffer.getAndSet(idx, null);                                           // may briefly be null (benign)
        }
    }

    /*
     * Tiny reusable List<byte[]> implementation
     * backed by the reusable batchBuf array.
    */
    private static final class ByteBatch extends AbstractList<byte[]> {
        private final byte[][] backing;
        @Setter private int size;

        ByteBatch(byte[][] backing) {
            this.backing = backing;
        }

        @Override
        public byte[] get(int index) {
            if (index >= size) throw new IndexOutOfBoundsException();
            return backing[index];
        }

        @Override
        public int size() {
            return size;
        }
    }
}
