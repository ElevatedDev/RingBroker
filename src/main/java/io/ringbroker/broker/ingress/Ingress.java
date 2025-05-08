package io.ringbroker.broker.ingress;

import com.google.protobuf.DynamicMessage;
import io.ringbroker.core.ring.RingBuffer;
import io.ringbroker.ledger.orchestrator.LedgerOrchestrator;
import io.ringbroker.registry.TopicRegistry;
import lombok.RequiredArgsConstructor;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Batched ingress with DLQ + schema validation preserved.
 */
@RequiredArgsConstructor
public final class Ingress {
    private static final int MAX_RETRIES = 5;

    private final TopicRegistry registry;
    private final RingBuffer<byte[]> ring;
    private final LedgerOrchestrator segments;
    private final ExecutorService pool;
    private final int batchSize;
    private final BlockingQueue<Task> queue = new LinkedBlockingQueue<>();

    /**
     * Same signature plus batchSize.
     */
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

        for (int i = 0; i < threads; i++) {
            exec.submit(ingress::writerLoop);
        }
        return ingress;
    }

    /**
     * Convenience for non-retry calls
     */
    public void publish(final String topic, final byte[] payload) {
        publish(topic, 0, payload);
    }

    @PostConstruct
    private void init() {
        // no-op for DI
    }

    /**
     * Enqueues a message: does DLQ routing and schema validation here.
     */
    public void publish(final String topic, final int retries, final byte[] rawPayload) {
        // 1) validate base topic
        if (!registry.contains(topic)) {
            throw new IllegalArgumentException("topic not registered: " + topic);
        }
        // 2) DLQ routing
        String outTopic = retries > MAX_RETRIES ? topic + ".DLQ" : topic;
        if (!registry.contains(outTopic)) {
            throw new IllegalArgumentException("topic not registered: " + outTopic);
        }
        // 3) schema-validate
        try {
            DynamicMessage.parseFrom(registry.descriptor(outTopic), rawPayload);
        } catch (final Exception e) {
            // parse error → DLQ
            outTopic = topic + ".DLQ";
            if (!registry.contains(outTopic)) {
                throw new IllegalArgumentException("DLQ not registered: " + outTopic);
            }
        }
        // 4) enqueue
        queue.add(new Task(rawPayload));
    }

    /**
     * Writer loop: batch disk writes + ring publishes
     */
    private void writerLoop() {
        final List<Task> batch = new ArrayList<>(batchSize);
        try {
            while (!Thread.currentThread().isInterrupted()) {
                batch.clear();
                batch.add(queue.take());                 // block for first
                queue.drainTo(batch, batchSize - 1);     // up to batchSize

                // collect payloads
                final List<byte[]> payloads = new ArrayList<>(batch.size());
                for (final Task t : batch) {
                    payloads.add(t.payload);
                }

                // append all to disk at once
                segments.writable().appendBatch(payloads);

                // publish to ring
                for (final Task t : batch) {
                    final long seq = ring.next();
                    ring.publish(seq, t.payload);
                }
            }
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (final IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private static final class Task {
        final byte[] payload;

        Task(final byte[] payload) {
            this.payload = payload;
        }
    }
}
