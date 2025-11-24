package io.ringbroker.ledger.orchestrator;

import io.ringbroker.ledger.constant.LedgerConstant;
import io.ringbroker.ledger.segment.LedgerSegment;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

@Slf4j
public final class LedgerOrchestrator implements AutoCloseable {

    private final Path directory;
    @Getter
    private final int segmentCapacity;
    private final ExecutorService segmentAllocatorService =
            Executors.newSingleThreadExecutor(Thread.ofVirtual().name("segment-allocator").factory());

    private final AtomicReference<LedgerSegment> activeSegment = new AtomicReference<>();
    private final Lock segmentLock = new ReentrantLock();
    @Getter
    private volatile long highWaterMark;
    private Future<LedgerSegment> nextSegmentFuture;

    private LedgerOrchestrator(final Path directory, final int segmentCapacity, final long initialHwm) {
        this.directory = directory;
        this.segmentCapacity = segmentCapacity;
        this.highWaterMark = initialHwm;
    }

    public static LedgerOrchestrator bootstrap(@NonNull final Path directory, final int segmentCapacity) throws IOException {
        Files.createDirectories(directory);

        final List<LedgerSegment> recoveredSegments = Files.list(directory)
                .filter(p -> p.getFileName().toString().endsWith(LedgerConstant.SEGMENT_EXT))
                .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                .map(path -> recoverAndOpenSegment(path, directory))
                .filter(java.util.Objects::nonNull)
                .sorted(Comparator.comparingLong(LedgerSegment::getFirstOffset).thenComparingLong(LedgerSegment::getLastOffset))
                .toList();

        final long currentHwm = recoveredSegments.isEmpty() ? 0L : recoveredSegments.getLast().getLastOffset();
        final LedgerOrchestrator orchestrator = new LedgerOrchestrator(directory, segmentCapacity, currentHwm);

        final LedgerSegment currentTailSegment;
        if (recoveredSegments.isEmpty()) {
            currentTailSegment = orchestrator.createNewSegment();
        } else {
            final LedgerSegment lastGoodSegment = recoveredSegments.getLast();
            if (lastGoodSegment.isFull()) {
                currentTailSegment = orchestrator.createNewSegment(lastGoodSegment.getLastOffset());
            } else {
                currentTailSegment = lastGoodSegment;
            }
        }

        orchestrator.activeSegment.set(currentTailSegment);
        orchestrator.preAllocateNextSegment();
        return orchestrator;
    }

    private static LedgerSegment recoverAndOpenSegment(final Path segmentPath, final Path baseDir) {
        Path tempRecoveryPath = null;
        try {
            tempRecoveryPath = baseDir.resolve(segmentPath.getFileName().toString() + ".recovery_tmp");
            Files.copy(segmentPath, tempRecoveryPath, StandardCopyOption.REPLACE_EXISTING);

            if (recoverSegmentFile(tempRecoveryPath)) {
                Files.move(tempRecoveryPath, segmentPath, StandardCopyOption.REPLACE_EXISTING);
                return LedgerSegment.openExisting(segmentPath, false);
            } else {
                return null;
            }
        } catch (final IOException e) {
            log.error("Recovery I/O error: {}", segmentPath, e);
            return null;
        } finally {
            if (tempRecoveryPath != null) {
                try {
                    Files.deleteIfExists(tempRecoveryPath);
                } catch (IOException ignored) {
                }
            }
        }
    }

    private static boolean recoverSegmentFile(final Path segmentPath) {
        try (final FileChannel ch = FileChannel.open(segmentPath, READ, WRITE);
             final DataInputStream in = new DataInputStream(Channels.newInputStream(ch))) {

            long currentFilePosition = LedgerSegment.HEADER_SIZE;
            if (ch.size() < LedgerSegment.HEADER_SIZE) {
                ch.truncate(0);
                ch.force(true);
                return true;
            }

            final ByteBuffer recordHeaderBuffer = ByteBuffer.allocate(Integer.BYTES * 2);
            ch.position(LedgerSegment.HEADER_SIZE);

            // Reuse CRC instance to reduce allocation churn during recovery
            final java.util.zip.CRC32C crcValidator = new java.util.zip.CRC32C();

            while (currentFilePosition < ch.size()) {
                recordHeaderBuffer.clear();
                int bytesRead = ch.read(recordHeaderBuffer);

                // 1. Check for EOF (Clean stop)
                if (bytesRead == -1 || bytesRead == 0) break;

                // 2. Check for partial header (Corruption)
                if (bytesRead < recordHeaderBuffer.capacity()) {
                    log.warn("Partial record header at pos {}. Truncating.", currentFilePosition);
                    truncateChannel(ch, currentFilePosition);
                    return true;
                }

                recordHeaderBuffer.flip();
                final int payloadLength = recordHeaderBuffer.getInt();
                final int storedCrc = recordHeaderBuffer.getInt();

                // 3. Check for Valid EOF Marker (0-filled space)
                if (payloadLength == 0) break;

                // 4. Sanity Check Length
                if (payloadLength < 0 || payloadLength > (ch.size() - currentFilePosition - Integer.BYTES * 2)) {
                    log.warn("Invalid payload length {} at {}. Truncating.", payloadLength, currentFilePosition);
                    truncateChannel(ch, currentFilePosition);
                    return true;
                }

                final ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadLength);
                bytesRead = ch.read(payloadBuffer);

                // 5. Check for Torn Payload
                if (bytesRead < payloadLength) {
                    log.warn("Torn record payload at {}. Truncating.", currentFilePosition);
                    truncateChannel(ch, currentFilePosition);
                    return true;
                }

                payloadBuffer.flip();
                crcValidator.reset();

                // CRC32C works with direct ByteBuffers or arrays.
                // Since we allocated on heap, .array() is safe.
                crcValidator.update(payloadBuffer.array(), 0, payloadLength);

                if ((int) crcValidator.getValue() != storedCrc && storedCrc != 0) {
                    log.warn("CRC32C Mismatch at {}. Stored: {}, Calc: {}. Truncating.",
                            currentFilePosition, storedCrc, (int) crcValidator.getValue());
                    truncateChannel(ch, currentFilePosition);
                    return true;
                }

                currentFilePosition += (Integer.BYTES * 2 + payloadLength);
            }
            return true;
        } catch (final IOException e) {
            log.error("Critical recovery error: {}", segmentPath, e);
            return false;
        }
    }

    private static void truncateChannel(final FileChannel ch, final long position) throws IOException {
        log.warn("Truncating segment to {}", position);
        ch.truncate(position);
        ch.force(true);
    }

    /**
     * Returns the active segment, rolling if it cannot fit `requiredBytes`.
     */
    public LedgerSegment writable(int requiredBytes) throws IOException {
        LedgerSegment current = activeSegment.get();

        if (current == null || !current.hasSpaceFor(requiredBytes)) {
            if (current != null) {
                log.debug("Rolling segment (capacity full for batch of {} bytes)", requiredBytes);
            }
            current = rollToNextSegment();
            activeSegment.set(current);
            preAllocateNextSegment();
        }
        return current;
    }

    public LedgerSegment writable() throws IOException {
        return writable(1024); // Default safety margin
    }

    private LedgerSegment rollToNextSegment() throws IOException {
        LedgerSegment nextActiveSegment = null;
        if (nextSegmentFuture != null && nextSegmentFuture.isDone()) {
            try {
                nextActiveSegment = nextSegmentFuture.get();
            } catch (final Exception ignored) {
            }
        }

        if (nextActiveSegment == null) {
            final LedgerSegment previousActive = activeSegment.get();
            final long baseOffset = (previousActive != null) ? previousActive.getLastOffset() : this.highWaterMark;
            nextActiveSegment = createNewSegment(baseOffset);
        }
        return nextActiveSegment;
    }

    private void preAllocateNextSegment() {
        if (nextSegmentFuture == null || nextSegmentFuture.isDone()) {
            final LedgerSegment current = activeSegment.get();
            final long base = (current != null) ? current.getLastOffset() : this.highWaterMark;
            nextSegmentFuture = segmentAllocatorService.submit(() -> createNewSegment(base));
        }
    }

    private LedgerSegment createNewSegment() throws IOException {
        segmentLock.lock();
        try {
            return createNewSegment(this.highWaterMark);
        } finally {
            segmentLock.unlock();
        }
    }

    private LedgerSegment createNewSegment(final long previousSegmentLastOffset) throws IOException {
        segmentLock.lock();
        try {
            Path segmentPath;
            String fileName;
            do {
                fileName = String.format("%d-%06d%s", System.currentTimeMillis(), ThreadLocalRandom.current().nextInt(1_000_000), LedgerConstant.SEGMENT_EXT);
                segmentPath = directory.resolve(fileName);
            } while (Files.exists(segmentPath));

            log.info("Creating segment: {}", segmentPath);
            return LedgerSegment.create(segmentPath, segmentCapacity, false);
        } finally {
            segmentLock.unlock();
        }
    }

    @Override
    public void close() {
        segmentAllocatorService.shutdownNow();
        final LedgerSegment current = activeSegment.getAndSet(null);
        if (current != null) try {
            current.close();
        } catch (Exception ignored) {
        }

        if (nextSegmentFuture != null && nextSegmentFuture.isDone() && !nextSegmentFuture.isCancelled()) {
            try {
                final LedgerSegment f = nextSegmentFuture.get();
                if (f != null) f.close();
            } catch (Exception ignored) {
            }
        }
    }

    public void setHighWaterMark(final long newHwm) {
        if (newHwm > this.highWaterMark) this.highWaterMark = newHwm;
    }
}