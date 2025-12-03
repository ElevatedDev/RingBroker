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
    private static final long PARK_NANOS = 1_000;

    @Getter private final TopicRegistry registry;
    @Getter private final RingBuffer<byte[]> ring;
    @Getter private final VirtualLog virtualLog;

    private final AtomicLong activeEpoch = new AtomicLong();
    private final ExecutorService pool;

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
                    final ExecutorService pool,
                    final int batchSize,
                    final boolean forceDurableWrites) {

        this.registry = registry;
        this.ring = ring;
        this.virtualLog = virtualLog;
        this.activeEpoch.set(epoch);
        this.pool = pool;
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

        final Ingress ingress = new Ingress(registry, ring, log, epoch, EXECUTOR, batchSize, durable);
        ingress.writerTask = EXECUTOR.submit(ingress::writerLoop);
        return ingress;
    }

    private static int nextPowerOfTwo(final int x) {
        final int highest = Integer.highestOneBit(x);
        return (x == highest) ? x : highest << 1;
    }

    public void publish(final String topic, final byte[] payload) {
        publish(topic, 0, payload);
    }

    public void publish(final String topic, final int retries, final byte[] rawPayload) {
        if (!registry.contains(topic)) throw new IllegalArgumentException("topic not registered: " + topic);

        final String outTopic = retries > MAX_RETRIES ? topic + ".DLQ" : topic;
        if (!registry.contains(outTopic)) throw new IllegalArgumentException("topic not registered: " + outTopic);

        final long epoch = activeEpoch.get();
        while (!queue.offer(rawPayload, epoch)) {
            Thread.onSpinWait();
        }
    }

    public void publishForEpoch(final long epoch, final byte[] rawPayload) {
        while (!queue.offer(rawPayload, epoch)) {
            Thread.onSpinWait();
        }
    }

    /**
     * Append a batch of payloads directly to the epoch's log (used for backfill).
     */
    public void appendBackfillBatch(final long epoch, final byte[][] payloads, final int count) throws IOException {
        if (count == 0) return;
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
            total += (Integer.BYTES + Integer.BYTES + payloads[i].length); // len + crc + payload
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
        try {
            final SlotRing.Entry entry = new SlotRing.Entry();
            final SlotRing.Entry carry = new SlotRing.Entry();
            boolean hasCarry = false;

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

                int count = 0;
                int totalBytes = 0;
                final long batchEpoch = entry.epoch;

                batchBuffer[count++] = entry.payload;
                totalBytes += (8 + entry.payload.length);

                // Drain as much as possible, but stop when epoch changes.
                while (count < batchSize) {
                    if (!queue.pollInto(entry)) break;

                    if (entry.epoch != batchEpoch) {
                        carry.payload = entry.payload;
                        carry.epoch = entry.epoch;
                        hasCarry = true;
                        break;
                    }

                    batchBuffer[count++] = entry.payload;
                    totalBytes += (8 + entry.payload.length);
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
        } catch (final RuntimeException ex) {
            log.error("Ingress writer loop encountered an unexpected runtime error and will terminate.", ex);
            throw new RuntimeException("Ingress writer loop failed critically due to RuntimeException", ex);
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

    static final class SlotRing {
        private static final VarHandle SEQUENCE_HANDLE, BUFFER_HANDLE, EPOCH_HANDLE;

        static {
            SEQUENCE_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);
            BUFFER_HANDLE = MethodHandles.arrayElementVarHandle(byte[][].class);
            EPOCH_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);
        }

        private final long[] epochs;
        private final int mask;
        private final long[] sequence;
        private final byte[][] buffer;

        private final PaddedAtomicLong tail = new PaddedAtomicLong(0);
        private final PaddedAtomicLong head = new PaddedAtomicLong(0);

        SlotRing(final int capacityPow2) {
            mask = capacityPow2 - 1;

            sequence = new long[capacityPow2];
            buffer = new byte[capacityPow2][];
            epochs = new long[capacityPow2];

            for (int i = 0; i < capacityPow2; i++) sequence[i] = i;
        }

        boolean offer(final byte[] element, final long epoch) {
            long tailSnapshot;

            while (true) {
                tailSnapshot = tail.get();

                final long index = tailSnapshot & mask;
                final long seqVal = (long) SEQUENCE_HANDLE.getVolatile(this.sequence, (int) index);
                final long difference = seqVal - tailSnapshot;

                if (difference == 0) {
                    if (tail.compareAndSet(tailSnapshot, tailSnapshot + 1)) break;
                } else if (difference < 0) {
                    return false;
                } else {
                    Thread.onSpinWait();
                }
            }

            final int bufferIndex = (int) (tailSnapshot & mask);

            BUFFER_HANDLE.setRelease(buffer, bufferIndex, element);
            EPOCH_HANDLE.setRelease(epochs, bufferIndex, epoch);
            SEQUENCE_HANDLE.setRelease(sequence, bufferIndex, tailSnapshot + 1);

            return true;
        }

        boolean pollInto(final Entry out) {
            long headSnapshot;

            while (true) {
                headSnapshot = head.get();

                final long index = headSnapshot & mask;
                final long seqVal = (long) SEQUENCE_HANDLE.getVolatile(this.sequence, (int) index);
                final long difference = seqVal - (headSnapshot + 1);

                if (difference == 0) {
                    if (head.compareAndSet(headSnapshot, headSnapshot + 1)) break;
                } else if (difference < 0) {
                    return false;
                } else {
                    Thread.onSpinWait();
                }
            }

            final int bufferIndex = (int) (headSnapshot & mask);

            // Acquire loads match the producer's release stores.
            out.payload = (byte[]) BUFFER_HANDLE.getAcquire(buffer, bufferIndex);
            out.epoch = (long) EPOCH_HANDLE.getAcquire(epochs, bufferIndex);

            SEQUENCE_HANDLE.setRelease(sequence, bufferIndex, headSnapshot + mask + 1);
            BUFFER_HANDLE.set(buffer, bufferIndex, null);

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
