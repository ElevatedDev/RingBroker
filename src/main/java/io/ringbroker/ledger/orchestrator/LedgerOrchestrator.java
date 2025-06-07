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
import java.util.zip.CRC32;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Manages a directory of {@link LedgerSegment} files.
 * Handles segment creation, recovery, rolling to new segments, and pre-allocation of future segments.
 * The orchestrator ensures that there is always a writable segment available and maintains
 * a high-water mark representing the last successfully written global offset.
 */
@Slf4j
public final class LedgerOrchestrator implements AutoCloseable {

    private final Path directory;
    @Getter
    private final int segmentCapacity; // Renamed from segmentSize for clarity (size vs capacity)
    private final ExecutorService segmentAllocatorService =
            Executors.newSingleThreadExecutor(
                    Thread.ofVirtual().name("segment-allocator").factory());

    private final AtomicReference<LedgerSegment> activeSegment = new AtomicReference<>();
    @Getter
    private volatile long highWaterMark;
    private Future<LedgerSegment> nextSegmentFuture;

    private LedgerOrchestrator(final Path directory, final int segmentCapacity, final long initialHwm) {
        this.directory = directory;
        this.segmentCapacity = segmentCapacity;
        this.highWaterMark = initialHwm;
    }

    /**
     * Bootstraps a LedgerOrchestrator for the given directory.
     * This involves recovering any existing segments, opening them, and preparing for new appends.
     *
     * @param directory       The directory where ledger segments are stored.
     * @param segmentCapacity The capacity for each new ledger segment.
     * @return An initialized {@link LedgerOrchestrator} instance.
     * @throws IOException If an I/O error occurs during directory creation or segment recovery.
     */
    public static LedgerOrchestrator bootstrap(@NonNull final Path directory, final int segmentCapacity) throws IOException {
        Files.createDirectories(directory);

        final List<LedgerSegment> recoveredSegments = Files.list(directory)
                .filter(p -> p.getFileName().toString().endsWith(LedgerConstant.SEGMENT_EXT))
                .sorted(Comparator.comparing(p -> p.getFileName().toString())) // Sort by filename for consistent order
                .map(path -> recoverAndOpenSegment(path, directory)) // Pass baseDir for temp recovery file
                .filter(java.util.Objects::nonNull) // Filter out segments that failed recovery critically
                .sorted(Comparator.comparingLong(LedgerSegment::getFirstOffset).thenComparingLong(LedgerSegment::getLastOffset))
                .toList();

        final long currentHwm = recoveredSegments.isEmpty() ? 0L : recoveredSegments.getLast().getLastOffset();
        final LedgerOrchestrator orchestrator = new LedgerOrchestrator(directory, segmentCapacity, currentHwm);

        final LedgerSegment currentTailSegment;
        if (recoveredSegments.isEmpty()) {
            currentTailSegment = orchestrator.createNewSegment();
        } else {
            final LedgerSegment lastGoodSegment = recoveredSegments.getLast();
            // If the last segment is full, create a new one, otherwise use it.
            if (lastGoodSegment.isFull()) {
                log.info("Last recovered segment {} is full. Creating a new active segment.", lastGoodSegment.getFile());
                currentTailSegment = orchestrator.createNewSegment(lastGoodSegment.getLastOffset());
            } else {
                currentTailSegment = lastGoodSegment;
                log.info("Using last recovered segment {} as active segment.", currentTailSegment.getFile());
            }
        }

        orchestrator.activeSegment.set(currentTailSegment);
        orchestrator.preAllocateNextSegment(); // Start pre-allocating the next one
        return orchestrator;
    }

    private static LedgerSegment recoverAndOpenSegment(final Path segmentPath, final Path baseDir) {
        log.debug("Attempting to recover segment: {}", segmentPath);
        Path tempRecoveryPath = null;
        try {
            // Create a temporary file for recovery to avoid corrupting the original on partial recovery failure
            tempRecoveryPath = baseDir.resolve(segmentPath.getFileName().toString() + ".recovery_tmp");
            Files.copy(segmentPath, tempRecoveryPath, StandardCopyOption.REPLACE_EXISTING);

            if (recoverSegmentFile(tempRecoveryPath)) { // recoverSegmentFile truncates on corruption
                // If recovery was successful (or no corruption found), replace original with recovered (potentially truncated)
                Files.move(tempRecoveryPath, segmentPath, StandardCopyOption.REPLACE_EXISTING);
                log.info("Recovery successful for segment: {}. Opening it.", segmentPath);
                return LedgerSegment.openExisting(segmentPath, false); // false for skipRecordCrc by default
            } else {
                log.error("Critical recovery failure for segment: {}. Segment might be unusable.", segmentPath);
                // Optionally, move the corrupt segment to a 'corrupt' directory instead of deleting
                final Path corruptDir = baseDir.resolve("corrupt");
                Files.createDirectories(corruptDir);
                Files.move(segmentPath, corruptDir.resolve(segmentPath.getFileName()), StandardCopyOption.REPLACE_EXISTING);
                return null; // Indicate failure
            }
        } catch (final IOException e) {
            log.error("IOException during recovery or opening of segment: {}", segmentPath, e);
            return null; // Indicate failure
        } finally {
            if (tempRecoveryPath != null && Files.exists(tempRecoveryPath)) {
                try {
                    Files.delete(tempRecoveryPath);
                } catch (final IOException ignored) {
                }
            }
        }
    }

    /**
     * Recovers a single segment file by reading records sequentially, validating CRCs,
     * and truncating the file at the first point of corruption.
     *
     * @param segmentPath Path to the segment file to recover.
     * @return true if recovery process completed (even if truncation occurred), false on critical I/O error during recovery.
     */
    private static boolean recoverSegmentFile(final Path segmentPath) {
        try (final FileChannel ch = FileChannel.open(segmentPath, READ, WRITE);
             final DataInputStream in = new DataInputStream(Channels.newInputStream(ch))) {

            long currentFilePosition = LedgerSegment.HEADER_SIZE; // Start reading records after the header
            if (ch.size() < LedgerSegment.HEADER_SIZE) {
                log.warn("Segment {} is smaller than header size. Truncating to 0.", segmentPath);
                ch.truncate(0);
                ch.force(true); // metaData=true to persist truncation
                return true; // Considered "recovered" as an empty (invalid) segment
            }
            final ByteBuffer recordHeaderBuffer = ByteBuffer.allocate(Integer.BYTES * 2);
            ch.position(LedgerSegment.HEADER_SIZE);

            while (currentFilePosition < ch.size()) {
                recordHeaderBuffer.clear();
                int bytesRead = ch.read(recordHeaderBuffer);
                if (bytesRead < recordHeaderBuffer.capacity()) { // Partial read of record header
                    log.warn("Partial record header read at position {} in {}. Truncating.", currentFilePosition, segmentPath);
                    truncateChannel(ch, currentFilePosition);
                    return true;
                }
                recordHeaderBuffer.flip();
                final int payloadLength = recordHeaderBuffer.getInt();
                final int storedCrc = recordHeaderBuffer.getInt();

                if (payloadLength <= 0 || payloadLength > (ch.size() - currentFilePosition - Integer.BYTES * 2)) {
                    log.warn("Invalid payload length {} at position {} in {}. Truncating.", payloadLength, currentFilePosition, segmentPath);
                    truncateChannel(ch, currentFilePosition);
                    return true;
                }

                final ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadLength);
                bytesRead = ch.read(payloadBuffer);
                if (bytesRead < payloadLength) {
                    log.warn("Partial payload read for record at position {} in {}. Truncating.", currentFilePosition, segmentPath);
                    truncateChannel(ch, currentFilePosition);
                    return true;
                }
                payloadBuffer.flip();

                // Validate CRC (if not skipped during original write, though recovery should always check if present)
                final CRC32 crcValidator = new CRC32(); // Use a new instance or thread-local if available/appropriate
                crcValidator.update(payloadBuffer.array(), 0, payloadLength); // Assumes payloadBuffer has an array
                if ((int) crcValidator.getValue() != storedCrc && storedCrc != 0) { // Allow 0 CRC if it was skipped
                    log.warn("Record CRC mismatch at position {} in {}. Stored: {}, Calculated: {}. Truncating.",
                            currentFilePosition, segmentPath, storedCrc, (int) crcValidator.getValue());
                    truncateChannel(ch, currentFilePosition);
                    return true;
                }

                currentFilePosition += (Integer.BYTES * 2 + payloadLength);
                // ch.position() is already updated by reads.
            }
            // If loop completes, all records are valid.
            log.debug("No corruption found in segment data: {}", segmentPath);
            return true;
        } catch (final IOException e) {
            log.error("IOException during segment file recovery for {}. File may be severely corrupted.", segmentPath, e);
            // Don't truncate here, as the IO error itself is the problem.
            return false; // Critical error
        }
    }

    private static void truncateChannel(final FileChannel ch, final long position) throws IOException {
        log.warn("Truncating segment {} at position {}", ch, position);
        ch.truncate(position);
        ch.force(true); // metaData=true to persist truncation
    }

    /**
     * Gets the currently active writable ledger segment.
     * If the current active segment is full, it rolls over to the pre-allocated segment or creates a new one.
     *
     * @return The writable {@link LedgerSegment}.
     * @throws IOException If an I/O error occurs while creating or opening a segment.
     */
    public LedgerSegment writable() throws IOException {
        LedgerSegment current = activeSegment.get();
        if (current == null || current.isFull()) {
            log.info("Active segment {} is full or null. Rolling to next segment.", current != null ? current.getFile() : "N/A");
            current = rollToNextSegment();
            activeSegment.set(current);

            if (current.getFirstOffset() > 0 && current.getLastOffset() == 0) {

            }
            // Schedule pre-allocation for the *next* next segment
            preAllocateNextSegment();
        }
        return current;
    }

    private LedgerSegment rollToNextSegment() throws IOException {
        LedgerSegment nextActiveSegment = null;
        if (nextSegmentFuture != null && nextSegmentFuture.isDone()) {
            try {
                nextActiveSegment = nextSegmentFuture.get();
                log.info("Rolled to pre-allocated segment: {}", nextActiveSegment.getFile());
            } catch (final Exception e) {
                log.warn("Pre-allocation of next segment failed. Creating a new one on demand.", e);
            }
        }

        if (nextActiveSegment == null) {
            final LedgerSegment previousActive = activeSegment.get();
            final long baseOffsetForNewSegment = (previousActive != null) ? previousActive.getLastOffset() : this.highWaterMark;

            nextActiveSegment = createNewSegment(baseOffsetForNewSegment);
        }
        return nextActiveSegment;
    }

    private void preAllocateNextSegment() {
        if (nextSegmentFuture == null || nextSegmentFuture.isDone()) {
            final LedgerSegment currentActive = activeSegment.get();
            final long baseOffset = (currentActive != null) ? currentActive.getLastOffset() : this.highWaterMark;
            nextSegmentFuture = segmentAllocatorService.submit(() -> createNewSegment(baseOffset));
            log.debug("Submitted pre-allocation task for next segment based on offset {}.", baseOffset);
        }
    }

    private synchronized LedgerSegment createNewSegment() throws IOException {
        return createNewSegment(this.highWaterMark);
    }

    private synchronized LedgerSegment createNewSegment(final long previousSegmentLastOffset) throws IOException {
        Path segmentPath;
        String fileName;
        // Ensure unique filenames, can be based on time and a random number or sequence.
        // Using System.currentTimeMillis() and a random int.
        do {
            fileName = String.format("%d-%06d%s",
                    System.currentTimeMillis(),
                    ThreadLocalRandom.current().nextInt(1_000_000),
                    LedgerConstant.SEGMENT_EXT);
            segmentPath = directory.resolve(fileName);
        } while (Files.exists(segmentPath));

        log.info("Creating new ledger segment: {} with capacity {}", segmentPath, segmentCapacity);
        final LedgerSegment newSegment = LedgerSegment.create(segmentPath, segmentCapacity, false); // false for skipRecordCrc by default

        // If the new segment logically follows a previous one, its firstOffset should be previousSegmentLastOffset + 1
        // However, LedgerSegment.create initializes firstOffset to 0. This needs careful handling.
        // For now, LedgerSegment manages its internal first/last offsets based on appends.
        // The orchestrator's highWaterMark tracks the global sequence.

        return newSegment;
    }

    @Override
    public void close() {
        segmentAllocatorService.shutdownNow(); // Attempt to stop pre-allocation tasks

        final LedgerSegment currentActive = activeSegment.getAndSet(null); // Clear active segment
        if (currentActive != null) {
            try {
                currentActive.close();
                log.info("Closed active segment: {}", currentActive.getFile());
            } catch (final Exception e) {
                log.warn("Error closing active segment: {}", currentActive.getFile(), e);
            }
        }

        // Cancel and attempt to close the future segment if it was being pre-allocated
        if (nextSegmentFuture != null && !nextSegmentFuture.isDone()) {
            nextSegmentFuture.cancel(true);
        }
        if (nextSegmentFuture != null && nextSegmentFuture.isDone() && !nextSegmentFuture.isCancelled()) {
            try {
                final LedgerSegment futureSeg = nextSegmentFuture.get(); // Should not block if isDone
                if (futureSeg != null) {
                    futureSeg.close();
                    log.info("Closed pre-allocated segment: {}", futureSeg.getFile());
                }
            } catch (final Exception e) {
                log.warn("Error closing pre-allocated segment", e);
            }
        }
    }

    /**
     * Updates the high-water mark. This should be called after records are successfully
     * written and confirmed (e.g., after a quorum of replicas acknowledge).
     *
     * @param newHwm The new high-water mark.
     */
    public void setHighWaterMark(final long newHwm) {
        // Ensure HWM only moves forward
        if (newHwm > this.highWaterMark) {
            this.highWaterMark = newHwm;
            log.trace("High-water mark updated to {}", newHwm);
        }
    }
}