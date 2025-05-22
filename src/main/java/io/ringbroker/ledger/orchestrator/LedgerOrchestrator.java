package io.ringbroker.ledger.orchestrator;

import io.ringbroker.ledger.constant.LedgerConstant;
import io.ringbroker.ledger.segment.LedgerSegment;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Owns a directory full of segments; rolls and pre‑allocates transparently.
 */
@Slf4j
public final class LedgerOrchestrator implements AutoCloseable {

    private final Path dir;
    @Getter private final int segmentSize;
    @Getter private volatile long highWaterMark;

    private final ExecutorService allocator =
            Executors.newSingleThreadExecutor(
                    Thread.ofVirtual().name("segment-allocator").factory());

    private final AtomicReference<LedgerSegment> active = new AtomicReference<>();
    private Future<LedgerSegment> nextFuture;

    public static LedgerOrchestrator bootstrap(@NonNull final Path dir, final int segmentSize) throws IOException {
        Files.createDirectories(dir);

        // 1. recover & open existing segments
        final List<LedgerSegment> segments = Files.list(dir)
                .filter(p -> p.getFileName().toString().endsWith(LedgerConstant.SEGMENT_EXT))
                .sorted(Comparator.comparing(Path::toString))
                .peek(LedgerOrchestrator::recoverSegment)
                .map(LedgerOrchestrator::openSegment)
                .sorted(Comparator.comparingLong(LedgerSegment::getFirstOffset))
                .toList();

        final long hwm = segments.isEmpty() ? 0L : segments.getLast().getLastOffset();
        final LedgerOrchestrator mgr = new LedgerOrchestrator(dir, segmentSize, hwm);
        final LedgerSegment tail = segments.isEmpty() ? mgr.createSegment() : segments.getLast();

        mgr.active.set(tail);
        mgr.allocateNext();
        return mgr;
    }

    private LedgerOrchestrator(final Path dir, final int segmentSize, final long hwm) {
        this.dir = dir; this.segmentSize = segmentSize; this.highWaterMark = hwm;
    }

    public LedgerSegment writable() throws IOException {
        LedgerSegment seg = active.get();
        if (seg == null || seg.isFull()) {
            seg = pollNextOrCreate();
            active.set(seg);
            highWaterMark = seg.getLastOffset();
            allocateNext();
        }
        return seg;
    }

    private LedgerSegment pollNextOrCreate() throws IOException {
        if (nextFuture != null && nextFuture.isDone()) {
            try { return nextFuture.get(); }
            catch (final Exception e) {
                log.warn("pre‑allocation failed", e);
            }
        }
        return createSegment();
    }

    private void allocateNext() {
        nextFuture = allocator.submit(this::createSegment);
    }

    private synchronized LedgerSegment createSegment() throws IOException {
        Path p;
        do {
            p = dir.resolve(System.currentTimeMillis() + "-" + ThreadLocalRandom.current().nextInt(1_000_000) + LedgerConstant.SEGMENT_EXT);
        } while (Files.exists(p));
        log.info("Creating new segment {}", p);
        return LedgerSegment.create(p, segmentSize, false);
    }

    private static LedgerSegment openSegment(final Path p) {
        try {
            return LedgerSegment.openExisting(p, false);
        }
        catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void recoverSegment(final Path p) {
        try (final FileChannel ch = FileChannel.open(p, READ, WRITE);
             final DataInputStream in = new DataInputStream(Channels.newInputStream(ch))) {

            long pos = 0L;

            while (true) {
                final int len;
                try {
                    len = in.readInt();
                } catch (final EOFException eof) {
                    break;
                }

                final int storedCrc = in.readInt();
                final byte[] payload = in.readNBytes(len);

                if (payload.length < len) {
                    truncate(ch, pos);
                    break;
                }
                final CRC32 crc = new CRC32();
                crc.update(payload, 0, len);

                if ((int) crc.getValue() != storedCrc) {
                    truncate(ch, pos);
                    break;
                }

                pos += Integer.BYTES + Integer.BYTES + len;
            }
        } catch (final IOException e) {
            throw new RuntimeException("Recovery failed for " + p, e);
        }
    }

    private static void truncate(final FileChannel ch, final long pos) throws IOException {
        log.warn("Truncating corrupt segment {} at {}", ch, pos);
        ch.truncate(pos); ch.force(false);
    }

    @Override
    public void close() {
        allocator.shutdownNow();
        final LedgerSegment seg = active.get();
        if (seg != null) try { seg.close(); } catch (final Exception ignore) { }
    }
}