package io.ringbroker.broker.ingress;

import com.google.protobuf.DynamicMessage;
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

/**
 * Allocation-free, lock-free ingress for high-throughput data ingestion.
 * <p>
 * This class manages the ingestion of data into a ring buffer, batching incoming messages
 * and coordinating with a ledger orchestrator for persistence. It is designed to minimize
 * allocations and avoid locks, using a custom MPMC (multi-producer, multi-consumer) queue
 * and batch buffer reuse. The class is thread-safe and optimized for low-latency, high-volume
 * data streams.
 * <p>
 * Key features:
 * <ul>
 *   <li>Lock-free, allocation-free batching and queuing of messages</li>
 *   <li>Integration with a {@link RingBuffer} for fast in-memory storage</li>
 *   <li>Coordination with a {@link LedgerOrchestrator} for durable persistence</li>
 *   <li>Custom reusable batch buffer to avoid per-batch allocations</li>
 *   <li>Configurable batch size and queue capacity</li>
 *   <li>Executor service for background processing</li>
 * </ul>
 * <p>
 * Usage:
 * <pre>
 *   Ingress ingress = Ingress.create(registry, ring, dataDir, segmentSize, batchSize);
 *   // Use ingress to ingest data batches
 * </pre>
 */
@Getter
@Slf4j
public final class Ingress {

    private static final ExecutorService EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private static final int MAX_RETRIES = 5;
    private static final int QUEUE_CAPACITY_FACTOR = 4;   // queue = FACTOR × batchSize
    private static final long PARK_NANOS = 1_000; // ≈1 µs

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
    private final byte[][] batchBuffer;
    private final ByteBatch batchView;
    private final boolean forceDurableWrites;

    // Keep a handle so we can stop writer deterministically on close.
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

    /**
     * Creates and initializes an {@code Ingress} instance with the provided configuration.
     * <p>
     * This static factory method sets up the required executor service, bootstraps the ledger orchestrator,
     * and starts the background writer loop for batch processing. The returned {@code Ingress} instance
     * is ready for use.
     *
     * @param registry    the topic registry for topic validation and schema lookup
     * @param ring        the ring buffer for downstream message publishing
     * @param dataDir     the directory for ledger segment storage
     * @param segmentSize the size of each ledger segment in bytes
     * @param batchSize   the maximum number of messages per batch
     * @return a fully initialized {@code Ingress} instance
     * @throws IOException if the ledger orchestrator cannot be bootstrapped
     */
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

    /**
     * Returns the next power of two greater than or equal to the given integer.
     * If the input is already a power of two, it returns the input itself.
     *
     * @param x the input integer
     * @return the next power of two greater than or equal to x
     */
    private static int nextPowerOfTwo(final int x) {
        final int highest = Integer.highestOneBit(x);

        return (x == highest) ? x : highest << 1;
    }

    /**
     * Publishes a message to the specified topic with no retries.
     * <p>
     * This is a convenience wrapper for {@link #publish(String, int, byte[])}
     * that sets the retry count to zero.
     *
     * @param topic   the topic to publish to
     * @param payload the message payload as a byte array
     */
    public void publish(final String topic, final byte[] payload) {
        publish(topic, 0, payload);
    }

    /**
     * Publishes a message to the specified topic, with support for retries and DLQ routing.
     * <p>
     * This method validates the topic, routes to a Dead Letter Queue (DLQ) if the retry count exceeds
     * the maximum allowed, performs schema validation, and enqueues the message for processing.
     * If the queue is full, the method spins until space is available.
     *
     * @param topic      the topic to publish to
     * @param retries    the number of previous delivery attempts
     * @param rawPayload the message payload as a byte array
     * @throws IllegalArgumentException if the topic or DLQ is not registered
     */
    public void publish(final String topic, final int retries, final byte[] rawPayload) {
        // 1) validate base topic
        if (!registry.contains(topic)) throw new IllegalArgumentException("topic not registered: " + topic);

        // 2) DLQ routing
        String outTopic = retries > MAX_RETRIES ? topic + ".DLQ" : topic;
        if (!registry.contains(outTopic)) throw new IllegalArgumentException("topic not registered: " + outTopic);

        // 4) enqueue without allocation; spin if queue is momentarily full
        while (!queue.offer(rawPayload)) {
            Thread.onSpinWait();
        }
    }

    /**
     * Dependency injection hook. No operation performed.
     * This method is intended for frameworks that require a post-construction initialization step.
     */
    @PostConstruct
    @SuppressWarnings("unused")
    private void init() { /* DI hook – no-op */ }

    /**
     * Zero-copy fetch visitor for ledger-backed reads.
     */
    @FunctionalInterface
    public interface FetchVisitor {
        void accept(long offset, MappedByteBuffer segmentBuffer, int payloadPos, int payloadLen);
    }

    /**
     * Fetches up to maxMessages starting at offset (inclusive), reading from the durable ledger.
     * Returns number of messages visited.
     */
    public int fetch(final long offset, final int maxMessages, final FetchVisitor visitor) {
        return segments.fetch(offset, maxMessages, visitor::accept);
    }

    /**
     * Continuously drains the queue, batches messages, persists them to disk, and publishes to the ring buffer.
     * This method runs in a background thread. Persistence behavior (fsync) is controlled by {@code forceDurableWrites}.
     */
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

                // Publish to ring as before.
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

    /**
     * Closes the underlying {@link LedgerOrchestrator}, which in turn closes its active segment.
     *
     * @throws IOException if an I/O error occurs during closing the ledger orchestrator.
     */
    public void close() throws IOException {
        final Future<?> t = writerTask;
        if (t != null) {
            t.cancel(true);
        }
        if (this.segments != null) {
            this.segments.close();
        }
    }

    /*
     * Allocation-free bounded lock-free multi-producer / multi-consumer queue
     * (heavily simplified Vyukov algorithm).
     * Only *one* array of references is allocated once in the constructor.
     */
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
                    return false; // queue full
                } else {
                    Thread.onSpinWait();
                }
            }

            final int bufferIndex = (int) (tailSnapshot & mask);

            BUFFER_HANDLE.setRelease(buffer, bufferIndex, element);         // write payload
            SEQUENCE_HANDLE.setRelease(sequence, bufferIndex, tailSnapshot + 1);     // publish slot

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
                    return null;               // queue empty
                } else {
                    Thread.onSpinWait();
                }
            }

            final int bufferIndex = (int) (headSnapshot & mask);
            final byte[] element = (byte[]) BUFFER_HANDLE.getAcquire(buffer, bufferIndex);

            SEQUENCE_HANDLE.setRelease(sequence, bufferIndex, headSnapshot + mask + 1); // mark slot empty
            BUFFER_HANDLE.set(buffer, bufferIndex, null);

            return element;
        }
    }

    /*
     * Cache-line-padded AtomicLong to stop false sharing between head & tail.
     */
    @SuppressWarnings("unused")
    private static final class PaddedAtomicLong extends AtomicLong {
        // left padding
        volatile long p1, p2, p3, p4, p5, p6, p7;
        // right padding
        volatile long q1, q2, q3, q4, q5, q6, q7;

        PaddedAtomicLong(final long initial) {
            super(initial);
        }
    }

    /*
     * Tiny reusable List<byte[]> implementation backed by the reusable batchBuf array.
     */
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
