package io.ringbroker.ledger.replica;

import io.ringbroker.core.ring.persistent.MappedRingBuffer;
import io.ringbroker.core.wait.SleepingWaitStrategy;
import io.ringbroker.ledger.append.Appendable;
import io.ringbroker.ledger.segment.LedgerSegment;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public final class ReplicatedLedger implements AutoCloseable, Appendable {
    private static final int RING_SLOTS = 1 << 15;    // 32 768
    private static final int SLOT_SIZE = 64 * 1024;  // 64 KB
    private static final int FLUSH_BATCH = 32 * 1024;  // 32 KB

    private final MappedRingBuffer ring;
    private final LedgerSegment segment;
    private final ExecutorService flusher = Executors.newSingleThreadExecutor(
            r -> { Thread t = new Thread(r, "mapped-flush"); t.setDaemon(true); return t; });

    public ReplicatedLedger(final Path segmentDir,
                            final long segmentSize) throws IOException {

        this.ring = new MappedRingBuffer(
                segmentDir.resolve("journal.mrb"),
                RING_SLOTS,
                SLOT_SIZE,
                new SleepingWaitStrategy());

        this.segment = LedgerSegment.create(segmentDir, (int) segmentSize, false);
        flusher.submit(this::drain);
    }

    /** Called from PB network handler – returns once data is crash-durable. */
    public void append(final ByteBuffer src) {
        byte[] payload = new byte[src.remaining()];
        src.get(payload);
        ring.publish(ring.next(), payload);
    }

    /** Background task: batch ring entries and write to NVMe segment. */
    private void drain() {
        try {
            long seq   = -1;
            ByteBuffer batchBuf = ByteBuffer.allocateDirect(FLUSH_BATCH);

            while (!Thread.currentThread().isInterrupted()) {
                long available = ring.getCursor();

                while (seq < available) {
                    seq++;
                    byte[] data = ring.get(seq);

                    if (batchBuf.remaining() < data.length) {
                        flushBatch(batchBuf);
                    }
                    batchBuf.put(data);
                }

                flushBatch(batchBuf);   // flush whatever we collected this round
                ring.flush();           // force dirty mmap pages to storage
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (IOException ioe) {
            log.error("Disk flush failed", ioe);
        }
    }

    private void flushBatch(ByteBuffer buf) throws IOException {
        if (buf.position() == 0) return;
        buf.flip();
        byte[] out = new byte[buf.remaining()];
        buf.get(out);
        segment.append(out);   // LedgerSegment expects byte[]
        buf.clear();
    }

    @Override
    public void close() throws Exception {
        flusher.shutdownNow();
        segment.close();
        ring.flush();
    }
}
