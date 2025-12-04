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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
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

    private final int batchSize;
    private final SlotRing queue;
    private final byte[][] batchBuffer;
    private final ByteBatch batchView;
    private final boolean forceDurableWrites;

    private volatile Future<?> writerTask;
    private volatile Throwable writerFailure;

    // --- NEW: waiters completed by writer thread when HWM advances ---
    private static final CompletableFuture<Void> DONE = CompletableFuture.completedFuture(null);

    private static final class SeqWaiter {
        final long seq;
        final CompletableFuture<Void> future;
        SeqWaiter(final long seq, final CompletableFuture<Void> future) {
            this.seq = seq;
            this.future = future;
        }
    }

    // epoch -> queue of waiters in increasing seq order (producer is pipeline; consumer is writer thread)
    private final ConcurrentMap<Long, ConcurrentLinkedQueue<SeqWaiter>> waitersByEpoch = new ConcurrentHashMap<>();

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

        final String outTopic = retries > MAX_RETRIES ? topic + ".DLQ" : topic;
        if (!registry.contains(outTopic)) throw new IllegalArgumentException("topic not registered: " + outTopic);

        final long epoch = activeEpoch.get();
        offerWithBackoff(rawPayload, epoch);
    }

    public void publishForEpoch(final long epoch, final byte[] rawPayload) {
        Objects.requireNonNull(rawPayload, "rawPayload");
        offerWithBackoff(rawPayload, epoch);
    }

    /**
     * NEW: completes when the epoch's high-watermark reaches at least seq (durable write done).
     * This is completed by the writer thread, so the pipeline never blocks/spins/polls.
     */
    public CompletableFuture<Void> whenPersisted(final long epoch, final long seq) {
        if (seq < 0) return DONE;

        // fast-path: already persisted
        try {
            if (highWaterMark(epoch) >= seq) return DONE;
        } catch (final Throwable t) {
            // if epoch bootstrapping fails, surface errors via future
            return CompletableFuture.failedFuture(t);
        }

        final CompletableFuture<Void> f = new CompletableFuture<>();
        waitersByEpoch.computeIfAbsent(epoch, __ -> new ConcurrentLinkedQueue<>())
                .offer(new SeqWaiter(seq, f));

        // NOTE: if writer already advanced, it’ll complete it on the next write;
        // but if the epoch goes idle, we avoid leaking by doing a final check:
        try {
            if (highWaterMark(epoch) >= seq) {
                // best-effort complete; writer may still drain later
                f.complete(null);
            }
        } catch (final Throwable t) {
            f.completeExceptionally(t);
        }

        return f;
    }

    private void completeWaiters(final long epoch, final long hwm) {
        final ConcurrentLinkedQueue<SeqWaiter> q = waitersByEpoch.get(epoch);
        if (q == null) return;

        for (;;) {
            final SeqWaiter w = q.peek();
            if (w == null) break;
            if (w.seq <= hwm) {
                q.poll();
                w.future.complete(null);
            } else {
                break;
            }
        }

        if (q.isEmpty()) {
            waitersByEpoch.remove(epoch, q);
        }
    }

    private void failAllWaiters(final Throwable t) {
        for (final var e : waitersByEpoch.entrySet()) {
            final ConcurrentLinkedQueue<SeqWaiter> q = e.getValue();
            SeqWaiter w;
            while ((w = q.poll()) != null) {
                w.future.completeExceptionally(t);
            }
        }
        waitersByEpoch.clear();
    }

    private void offerWithBackoff(final byte[] payload, final long epoch) {
        int spins = 0;

        for (;;) {
            final Throwable wf = writerFailure;
            if (wf != null) {
                throw new IllegalStateException("Ingress writer failed", wf);
            }

            if (queue.offer(payload, epoch)) return;

            if (Thread.currentThread().isInterrupted()) {
                throw new RuntimeException("Interrupted while publishing");
            }

            if ((++spins & 1023) == 0) {
                LockSupport.parkNanos(PARK_NANOS);
            } else {
                Thread.onSpinWait();
            }
        }
    }

    public void appendBackfillBatch(final long epoch, final byte[][] payloads, final int count) throws IOException {
        if (count == 0) return;
        for (int i = 0; i < count; i++) {
            if (payloads[i] == null) throw new IllegalArgumentException("Backfill payload[" + i + "] is null");
        }

        final int totalBytes = computeTotalBytes(payloads, count);
        final LedgerSegment segment = virtualLog.forEpoch(epoch).writable(totalBytes);

        final ByteBatch view = new ByteBatch(payloads);
        view.setSize(count);

        segment.appendBatchNoOffsets(view, totalBytes);
        final var ledger = virtualLog.forEpoch(epoch);
        ledger.setHighWaterMark(segment.getLastOffset());

        // complete any waiters that might be waiting on this epoch
        completeWaiters(epoch, ledger.getHighWaterMark());
    }

    private int computeTotalBytes(final byte[][] payloads, final int count) {
        int total = 0;
        for (int i = 0; i < count; i++) {
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

    private void writerLoop() {
        final SlotRing.Entry entry = new SlotRing.Entry();
        final SlotRing.Entry carry = new SlotRing.Entry();
        boolean hasCarry = false;

        try {
            while (!Thread.currentThread().isInterrupted()) {

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
                    throw new IllegalStateException("SlotRing returned null payload (epoch=" + entry.epoch + ")");
                }

                int count = 0;
                int totalBytes = 0;
                final long batchEpoch = entry.epoch;

                batchBuffer[count++] = entry.payload;
                totalBytes = Math.addExact(totalBytes, Integer.BYTES + Integer.BYTES + entry.payload.length);

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

                final LedgerOrchestrator ledger = virtualLog.forEpoch(batchEpoch);
                final LedgerSegment segment = ledger.writable(totalBytes);

                if (forceDurableWrites) {
                    segment.appendBatchAndForceNoOffsets(batchView, totalBytes);
                } else {
                    segment.appendBatchNoOffsets(batchView, totalBytes);
                }

                ledger.setHighWaterMark(segment.getLastOffset());

                // NEW: complete durability waiters for this epoch up to new HWM
                completeWaiters(batchEpoch, ledger.getHighWaterMark());

                final long endSeq = ring.next(count);
                ring.publishBatch(endSeq, count, batchBuffer);

                Arrays.fill(batchBuffer, 0, count, null);
            }
        } catch (final IOException ioe) {
            writerFailure = ioe;
            failAllWaiters(ioe);
            log.error("Ingress writer loop I/O error; terminating writer.", ioe);
        } catch (final Throwable t) {
            writerFailure = t;
            failAllWaiters(t);
            log.error("Ingress writer loop fatal error; terminating writer.", t);
            throw (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t);
        }
    }

    public void close() throws IOException {
        final Future<?> t = writerTask;
        if (t != null) t.cancel(true);
        failAllWaiters(new IOException("Ingress closed"));
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
        private final int capacity;
        private final long[] sequence;
        private final byte[][] buffer;

        private final PaddedAtomicLong tail = new PaddedAtomicLong(0);
        private final PaddedAtomicLong head = new PaddedAtomicLong(0);

        SlotRing(final int capacityPow2) {
            if (Integer.bitCount(capacityPow2) != 1) throw new IllegalArgumentException("capacity must be power of two");

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
                final int index = (int) (headSnapshot & mask);

                final long seqVal = (long) SEQUENCE_HANDLE.getVolatile(this.sequence, index);
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

            final byte[] payload = (byte[]) BUFFER_HANDLE.getAcquire(buffer, bufferIndex);
            final long epoch = (long) EPOCH_HANDLE.getAcquire(epochs, bufferIndex);

            // clear BEFORE publishing slot free
            BUFFER_HANDLE.setRelease(buffer, bufferIndex, null);
            EPOCH_HANDLE.setRelease(epochs, bufferIndex, 0L);
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

    private static final class PaddedAtomicLong extends AtomicLong {
        volatile long p1, p2, p3, p4, p5, p6, p7;
        volatile long q1, q2, q3, q4, q5, q6, q7;

        PaddedAtomicLong(final long initial) { super(initial); }
    }

    private static final class ByteBatch extends AbstractList<byte[]> {
        private final byte[][] backing;

        @Setter
        private int size;

        ByteBatch(final byte[][] backing) { this.backing = backing; }

        @Override
        public byte[] get(final int index) {
            if (index >= size) throw new IndexOutOfBoundsException();
            return backing[index];
        }

        @Override
        public int size() { return size; }
    }
}
