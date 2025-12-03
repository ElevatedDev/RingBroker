package io.ringbroker.broker.ingress;

import io.ringbroker.core.ring.RingBuffer;
import io.ringbroker.ledger.orchestrator.LedgerOrchestrator;
import io.ringbroker.ledger.orchestrator.VirtualLog;
import io.ringbroker.ledger.segment.LedgerSegment;
import io.ringbroker.registry.TopicRegistry;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.MappedByteBuffer;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public final class Ingress {

    private static final ExecutorService EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private static final int MAX_RETRIES = 5;
    private static final int QUEUE_CAPACITY_FACTOR = 4;

    // Writer idle park, and producer backoff park (kept tiny)
    private static final long PARK_NANOS = 1_000;

    @Getter private final TopicRegistry registry;
    @Getter private final RingBuffer<byte[]> ring;
    @Getter private final VirtualLog virtualLog;

    private final AtomicLong activeEpoch = new AtomicLong();

    private final int batchSize;
    private final SlotRing queue;
    private final byte[][] batchBuffer;
    private final ByteBatch batchView;
    private final boolean forceDurableWrites;

    private volatile Future<?> writerTask;

    private Ingress(final TopicRegistry registry,
                    final RingBuffer<byte[]> ring,
                    final VirtualLog virtualLog,
                    final long epoch,
                    final int batchSize,
                    final boolean forceDurableWrites) {

        this.registry = Objects.requireNonNull(registry, "registry");
        this.ring = Objects.requireNonNull(ring, "ring");
        this.virtualLog = Objects.requireNonNull(virtualLog, "virtualLog");

        if (batchSize <= 0) throw new IllegalArgumentException("batchSize must be > 0");

        this.activeEpoch.set(epoch);
        this.batchSize = batchSize;
        this.forceDurableWrites = forceDurableWrites;

        final int capacity = nextPowerOfTwo(batchSize * QUEUE_CAPACITY_FACTOR);
        this.queue = new SlotRing(capacity);
        this.batchBuffer = new byte[batchSize][];
        this.batchView = new ByteBatch(batchBuffer);
    }

    public static Ingress create(final TopicRegistry registry,
                                 final RingBuffer<byte[]> ring,
                                 final VirtualLog log,
                                 final long epoch,
                                 final int batchSize,
                                 final boolean durable) throws IOException {

        final Ingress ingress = new Ingress(registry, ring, log, epoch, batchSize, durable);
        ingress.writerTask = EXECUTOR.submit(ingress::writerLoop);
        return ingress;
    }

    private static int nextPowerOfTwo(final int x) {
        final int v = Math.max(2, x);
        final int highest = Integer.highestOneBit(v);
        return (v == highest) ? v : highest << 1;
    }

    public void publish(final String topic, final byte[] payload) {
        publish(topic, 0, payload);
    }

    public void publish(final String topic, final int retries, final byte[] rawPayload) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(rawPayload, "rawPayload");

        if (!registry.contains(topic)) throw new IllegalArgumentException("topic not registered: " + topic);

        // Keep original behavior (DLQ routing decision); note: payload does not embed topic.
        final String outTopic = retries > MAX_RETRIES ? topic + ".DLQ" : topic;
        if (!registry.contains(outTopic)) throw new IllegalArgumentException("topic not registered: " + outTopic);

        final long epoch = activeEpoch.get();
        offerWithBackoff(rawPayload, epoch);
    }

    public void publishForEpoch(final long epoch, final byte[] rawPayload) {
        Objects.requireNonNull(rawPayload, "rawPayload");
        offerWithBackoff(rawPayload, epoch);
    }

    private void offerWithBackoff(final byte[] payload, final long epoch) {
        int spins = 0;
        while (!queue.offer(payload, epoch)) {
            if (Thread.currentThread().isInterrupted()) {
                // Make shutdown responsive.
                throw new RuntimeException("Interrupted while publishing");
            }
            // Gentle backoff under contention/full queue.
            if ((++spins & 1023) == 0) {
                LockSupport.parkNanos(PARK_NANOS);
            } else {
                Thread.onSpinWait();
            }
        }
    }

    /**
     * Append a batch of payloads directly to the epoch's log (used for backfill).
     */
    public void appendBackfillBatch(final long epoch, final byte[][] payloads, final int count) throws IOException {
        if (count == 0) return;
        for (int i = 0; i < count; i++) {
            if (payloads[i] == null) {
                throw new IllegalArgumentException("Backfill payload[" + i + "] is null");
            }
        }

        final int totalBytes = computeTotalBytes(payloads, count);
        final LedgerSegment segment = virtualLog.forEpoch(epoch).writable(totalBytes);

        final ByteBatch view = new ByteBatch(payloads);
        view.setSize(count);

        segment.appendBatchNoOffsets(view, totalBytes);
        virtualLog.forEpoch(epoch).setHighWaterMark(segment.getLastOffset());
    }

    private int computeTotalBytes(final byte[][] payloads, final int count) {
        int total = 0;
        for (int i = 0; i < count; i++) {
            // LedgerSegment stores: [len:int][crc:int][payload:bytes]
            final int len = payloads[i].length;
            total = Math.addExact(total, Integer.BYTES + Integer.BYTES + len);
        }
        return total;
    }

    @PostConstruct
    @SuppressWarnings("unused")
    private void init() { /* no-op */ }

    @FunctionalInterface
    public interface FetchVisitor {
        void accept(long offset, MappedByteBuffer segmentBuffer, int payloadPos, int payloadLen);
    }

    public int fetch(final long offset, final int maxMessages, final FetchVisitor visitor) {
        return virtualLog.forEpoch(activeEpoch.get()).fetch(offset, maxMessages, visitor::accept);
    }

    public int fetchEpoch(final long epoch, final long offset, final int maxMessages, final FetchVisitor visitor) {
        return virtualLog.forEpoch(epoch).fetch(offset, maxMessages, visitor::accept);
    }

    /**
     * Writer loop:
     * - drains SlotRing
     * - batches by epoch (WITHOUT peekEpoch() to avoid double queue-touch per msg)
     * - appends to ledger
     * - publishes to ring
     */
    private void writerLoop() {
        final SlotRing.Entry entry = new SlotRing.Entry();
        final SlotRing.Entry carry = new SlotRing.Entry();
        boolean hasCarry = false;

        try {
            while (!Thread.currentThread().isInterrupted()) {

                // First element comes either from carry (epoch boundary) or queue.
                if (hasCarry) {
                    entry.payload = carry.payload;
                    entry.epoch = carry.epoch;
                    hasCarry = false;
                } else {
                    if (!queue.pollInto(entry)) {
                        LockSupport.parkNanos(PARK_NANOS);
                        continue;
                    }
                }

                if (entry.payload == null) {
                    // This should never happen after SlotRing fix; fail fast with context.
                    throw new IllegalStateException("SlotRing returned null payload (epoch=" + entry.epoch + ")");
                }

                int count = 0;
                int totalBytes = 0;
                final long batchEpoch = entry.epoch;

                batchBuffer[count++] = entry.payload;
                totalBytes = Math.addExact(totalBytes, Integer.BYTES + Integer.BYTES + entry.payload.length);

                // Drain as much as possible, but stop when epoch changes.
                while (count < batchSize) {
                    if (!queue.pollInto(entry)) break;

                    if (entry.payload == null) {
                        throw new IllegalStateException("SlotRing returned null payload (epoch=" + entry.epoch + ")");
                    }

                    if (entry.epoch != batchEpoch) {
                        carry.payload = entry.payload;
                        carry.epoch = entry.epoch;
                        hasCarry = true;
                        break;
                    }

                    batchBuffer[count++] = entry.payload;
                    totalBytes = Math.addExact(totalBytes, Integer.BYTES + Integer.BYTES + entry.payload.length);
                }

                batchView.setSize(count);

                // Avoid repeated forEpoch() calls
                final LedgerOrchestrator ledger = virtualLog.forEpoch(batchEpoch);
                final LedgerSegment segment = ledger.writable(totalBytes);

                if (forceDurableWrites) {
                    segment.appendBatchAndForceNoOffsets(batchView, totalBytes);
                } else {
                    segment.appendBatchNoOffsets(batchView, totalBytes);
                }

                ledger.setHighWaterMark(segment.getLastOffset());

                final long endSeq = ring.next(count);
                ring.publishBatch(endSeq, count, batchBuffer);

                Arrays.fill(batchBuffer, 0, count, null);
            }
        } catch (final IOException ioe) {
            log.error("Ingress writer loop encountered an I/O error and will terminate. Partition data may be at risk.", ioe);
        } catch (final Throwable t) {
            log.error("Ingress writer loop encountered a fatal error and will terminate.", t);
            throw (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t);
        }
    }

    public void close() throws IOException {
        final Future<?> t = writerTask;
        if (t != null) t.cancel(true);
        if (this.virtualLog != null) this.virtualLog.close();
    }

    public long getActiveEpoch() {
        return this.activeEpoch.get();
    }

    public void setActiveEpoch(final long epoch) {
        this.activeEpoch.set(epoch);
    }

    public LedgerOrchestrator getCurrentLedger() {
        return virtualLog.forEpoch(activeEpoch.get());
    }

    public long highWaterMark() {
        return getCurrentLedger().getHighWaterMark();
    }

    public long highWaterMark(final long epoch) {
        return virtualLog.forEpoch(epoch).getHighWaterMark();
    }

    // -------------------- SlotRing --------------------

    /**
     * MPSC-ish bounded ring (multi-producer, single-consumer).
     *
     * IMPORTANT FIX:
     * The consumer MUST clear the slot BEFORE publishing it as available to producers
     * (i.e., before updating the sequence to "free"). Otherwise, a producer may reuse
     * the slot and then the consumer clears it to null, corrupting the queue.
     */
    static final class SlotRing {
        private static final VarHandle SEQUENCE_HANDLE, BUFFER_HANDLE, EPOCH_HANDLE;

        static {
            SEQUENCE_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);
            BUFFER_HANDLE = MethodHandles.arrayElementVarHandle(byte[][].class);
            EPOCH_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);
        }

        private final long[] epochs;
        private final int mask;     // capacity - 1
        private final int capacity; // mask + 1
        private final long[] sequence;
        private final byte[][] buffer;

        private final PaddedAtomicLong tail = new PaddedAtomicLong(0);
        private final PaddedAtomicLong head = new PaddedAtomicLong(0);

        SlotRing(final int capacityPow2) {
            if (Integer.bitCount(capacityPow2) != 1) {
                throw new IllegalArgumentException("capacity must be power of two");
            }
            this.capacity = capacityPow2;
            this.mask = capacityPow2 - 1;

            this.sequence = new long[capacityPow2];
            this.buffer = new byte[capacityPow2][];
            this.epochs = new long[capacityPow2];

            for (int i = 0; i < capacityPow2; i++) sequence[i] = i;
        }

        boolean offer(final byte[] element, final long epoch) {
            if (element == null) throw new IllegalArgumentException("payload cannot be null");

            long tailSnapshot;

            while (true) {
                tailSnapshot = tail.get();
                final int index = (int) (tailSnapshot & mask);

                final long seqVal = (long) SEQUENCE_HANDLE.getVolatile(this.sequence, index);
                final long difference = seqVal - tailSnapshot;

                if (difference == 0) {
                    if (tail.compareAndSet(tailSnapshot, tailSnapshot + 1)) break;
                } else if (difference < 0) {
                    // full
                    return false;
                } else {
                    Thread.onSpinWait();
                }
            }

            final int bufferIndex = (int) (tailSnapshot & mask);

            // Publish payload+epoch, then publish sequence as "ready".
            BUFFER_HANDLE.setRelease(buffer, bufferIndex, element);
            EPOCH_HANDLE.setRelease(epochs, bufferIndex, epoch);
            SEQUENCE_HANDLE.setRelease(sequence, bufferIndex, tailSnapshot + 1);

            return true;
        }

        boolean pollInto(final Entry out) {
            long headSnapshot;

            while (true) {
                headSnapshot = head.get();
                final int index = (int) (headSnapshot & mask);

                final long seqVal = (long) SEQUENCE_HANDLE.getVolatile(this.sequence, index);
                final long difference = seqVal - (headSnapshot + 1);

                if (difference == 0) {
                    if (head.compareAndSet(headSnapshot, headSnapshot + 1)) break;
                } else if (difference < 0) {
                    // empty
                    return false;
                } else {
                    Thread.onSpinWait();
                }
            }

            final int bufferIndex = (int) (headSnapshot & mask);

            // Acquire loads match producer's release stores.
            final byte[] payload = (byte[]) BUFFER_HANDLE.getAcquire(buffer, bufferIndex);
            final long epoch = (long) EPOCH_HANDLE.getAcquire(epochs, bufferIndex);

            // FIX: clear slot BEFORE making it available to producers again.
            BUFFER_HANDLE.setRelease(buffer, bufferIndex, null);
            EPOCH_HANDLE.setRelease(epochs, bufferIndex, 0L);

            // Mark slot as free for the next cycle.
            SEQUENCE_HANDLE.setRelease(sequence, bufferIndex, headSnapshot + capacity);

            out.payload = payload;
            out.epoch = epoch;

            return true;
        }

        static final class Entry {
            byte[] payload;
            long epoch;
        }
    }

    @SuppressWarnings("unused")
    private static final class PaddedAtomicLong extends AtomicLong {
        volatile long p1, p2, p3, p4, p5, p6, p7;
        volatile long q1, q2, q3, q4, q5, q6, q7;

        PaddedAtomicLong(final long initial) {
            super(initial);
        }
    }

    private static final class ByteBatch extends AbstractList<byte[]> {
        private final byte[][] backing;

        @Setter
        private int size;

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
