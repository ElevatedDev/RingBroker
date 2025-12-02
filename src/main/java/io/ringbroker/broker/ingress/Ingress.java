package io.ringbroker.broker.ingress;

import io.ringbroker.core.ring.RingBuffer;
import io.ringbroker.ledger.orchestrator.LedgerOrchestrator;
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
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

@Getter
@Slf4j
public final class Ingress {

    private static final ExecutorService EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private static final int MAX_RETRIES = 5;
    private static final int QUEUE_CAPACITY_FACTOR = 4;
    private static final long PARK_NANOS = 1_000;

    private final TopicRegistry registry;
    private final RingBuffer<byte[]> ring;
    private final LedgerOrchestrator segments;
    private final ExecutorService pool;

    private final int batchSize;
    private final SlotRing queue;
    private final byte[][] batchBuffer;
    private final ByteBatch batchView;
    private final boolean forceDurableWrites;

    private volatile Future<?> writerTask;

    private Ingress(final TopicRegistry registry,
                    final RingBuffer<byte[]> ring,
                    final LedgerOrchestrator segments,
                    final ExecutorService pool,
                    final int batchSize,
                    final boolean forceDurableWrites) {

        this.registry = registry;
        this.ring = ring;
        this.segments = segments;
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
                                 final Path dataDir,
                                 final long segmentSize,
                                 final int batchSize,
                                 final boolean durable) throws IOException {

        final LedgerOrchestrator mgr =
                LedgerOrchestrator.bootstrap(dataDir, (int) segmentSize);

        final Ingress ingress =
                new Ingress(registry, ring, mgr, EXECUTOR, batchSize, durable);

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

        String outTopic = retries > MAX_RETRIES ? topic + ".DLQ" : topic;
        if (!registry.contains(outTopic)) throw new IllegalArgumentException("topic not registered: " + outTopic);

        while (!queue.offer(rawPayload)) {
            Thread.onSpinWait();
        }
    }

    @PostConstruct
    @SuppressWarnings("unused")
    private void init() { /* no-op */ }

    @FunctionalInterface
    public interface FetchVisitor {
        void accept(long offset, MappedByteBuffer segmentBuffer, int payloadPos, int payloadLen);
    }

    public int fetch(final long offset, final int maxMessages, final FetchVisitor visitor) {
        return segments.fetch(offset, maxMessages, visitor::accept);
    }

    private void writerLoop() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                final byte[] first = queue.poll();
                if (first == null) {
                    LockSupport.parkNanos(PARK_NANOS);
                    continue;
                }

                int count = 0;
                int totalBytes = 0;

                batchBuffer[count++] = first;
                totalBytes += (8 + first.length);

                while (count < batchSize) {
                    final byte[] next = queue.poll();
                    if (next == null) break;
                    batchBuffer[count++] = next;
                    totalBytes += (8 + next.length);
                }

                batchView.setSize(count);
                final LedgerSegment segment = segments.writable(totalBytes);

                if (forceDurableWrites) {
                    segment.appendBatchAndForceNoOffsets(batchView, totalBytes);
                } else {
                    segment.appendBatchNoOffsets(batchView, totalBytes);
                }

                segments.setHighWaterMark(segment.getLastOffset());

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
        if (t != null) {
            t.cancel(true);
        }
        if (this.segments != null) {
            this.segments.close();
        }
    }

    static final class SlotRing {
        private static final VarHandle SEQUENCE_HANDLE, BUFFER_HANDLE;

        static {
            SEQUENCE_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);
            BUFFER_HANDLE = MethodHandles.arrayElementVarHandle(byte[][].class);
        }

        private final int mask;
        private final long[] sequence;
        private final byte[][] buffer;

        private final PaddedAtomicLong tail = new PaddedAtomicLong(0);
        private final PaddedAtomicLong head = new PaddedAtomicLong(0);

        SlotRing(final int capacityPow2) {
            mask = capacityPow2 - 1;

            sequence = new long[capacityPow2];
            buffer = new byte[capacityPow2][];

            for (int i = 0; i < capacityPow2; i++) sequence[i] = i;
        }

        boolean offer(final byte[] element) {
            long tailSnapshot;

            while (true) {
                tailSnapshot = tail.get();

                final long index = tailSnapshot & mask;
                final long sequence = (long) SEQUENCE_HANDLE.getVolatile(this.sequence, (int) index);
                final long difference = sequence - tailSnapshot;

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
            SEQUENCE_HANDLE.setRelease(sequence, bufferIndex, tailSnapshot + 1);

            return true;
        }

        byte[] poll() {
            long headSnapshot;

            while (true) {
                headSnapshot = head.get();

                final long index = headSnapshot & mask;
                final long sequence = (long) SEQUENCE_HANDLE.getVolatile(this.sequence, (int) index);
                final long difference = sequence - (headSnapshot + 1);

                if (difference == 0) {
                    if (head.compareAndSet(headSnapshot, headSnapshot + 1)) break;
                } else if (difference < 0) {
                    return null;
                } else {
                    Thread.onSpinWait();
                }
            }

            final int bufferIndex = (int) (headSnapshot & mask);
            final byte[] element = (byte[]) BUFFER_HANDLE.getAcquire(buffer, bufferIndex);

            SEQUENCE_HANDLE.setRelease(sequence, bufferIndex, headSnapshot + mask + 1);
            BUFFER_HANDLE.set(buffer, bufferIndex, null);

            return element;
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
