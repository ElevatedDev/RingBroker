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

    private static final ExecutorService INDEX_BUILDER =
            Executors.newSingleThreadExecutor(Thread.ofPlatform().name("ledger-idx-builder").factory());

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

    // Immutable snapshot for fast binary-search selection in fetch.
    private volatile LedgerSegment[] segmentSnapshot = new LedgerSegment[0];

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

        // Offsets start at 0; if there are no segments yet, HWM is -1.
        final long currentHwm = recoveredSegments.isEmpty() ? -1L : recoveredSegments.getLast().getLastOffset();
        final LedgerOrchestrator orchestrator = new LedgerOrchestrator(directory, segmentCapacity, currentHwm);

        final LedgerSegment currentTailSegment;
        if (recoveredSegments.isEmpty()) {
            currentTailSegment = orchestrator.createNewSegment(-1L);
        } else {
            final LedgerSegment lastGoodSegment = recoveredSegments.getLast();
            if (lastGoodSegment.isFull()) {
                currentTailSegment = orchestrator.createNewSegment(lastGoodSegment.getLastOffset());
            } else {
                currentTailSegment = lastGoodSegment;
            }
        }

        orchestrator.activeSegment.set(currentTailSegment);

        // Build snapshot: recovered segments + potentially a new tail segment.
        if (recoveredSegments.isEmpty()) {
            orchestrator.segmentSnapshot = new LedgerSegment[]{ currentTailSegment };
        } else {
            if (recoveredSegments.getLast() == currentTailSegment) {
                orchestrator.segmentSnapshot = recoveredSegments.toArray(new LedgerSegment[0]);
            } else {
                final LedgerSegment[] arr = new LedgerSegment[recoveredSegments.size() + 1];
                for (int i = 0; i < recoveredSegments.size(); i++) arr[i] = recoveredSegments.get(i);
                arr[arr.length - 1] = currentTailSegment;
                orchestrator.segmentSnapshot = arr;
            }
        }

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

                final LedgerSegment seg = LedgerSegment.openExisting(segmentPath, false);

                // Drop garbage empty segments (typically preallocated but unused).
                if (seg.isLogicallyEmpty()) {
                    try { seg.close(); } catch (Exception ignored) {}
                    try { Files.deleteIfExists(segmentPath); } catch (Exception ignored) {}
                    try { Files.deleteIfExists(LedgerSegment.indexPathForSegment(segmentPath)); } catch (Exception ignored) {}
                    return null;
                }

                return seg;
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
            final ByteBuffer payloadChunk = ByteBuffer.allocateDirect(64 * 1024);

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

                crcValidator.reset();
                long remaining = payloadLength;
                boolean torn = false;

                while (remaining > 0) {
                    payloadChunk.clear();
                    if (remaining < payloadChunk.capacity()) {
                        payloadChunk.limit((int) remaining);
                    }

                    int chunkRead = ch.read(payloadChunk);
                    if (chunkRead < 0) {
                        torn = true;
                        break;
                    }

                    payloadChunk.flip();
                    crcValidator.update(payloadChunk);
                    remaining -= chunkRead;
                }

                // 5. Check for Torn Payload
                if (torn || remaining > 0) {
                    log.warn("Torn record payload at {}. Truncating.", currentFilePosition);
                    truncateChannel(ch, currentFilePosition);
                    return true;
                }

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

            final LedgerSegment sealed = current;

            current = rollToNextSegment();
            activeSegment.set(current);
            addToSnapshotIfMissing(current);
            preAllocateNextSegment();

            // Build .idx off the hot path for sealed segments.
            if (sealed != null && !sealed.isLogicallyEmpty()) {
                INDEX_BUILDER.execute(sealed::buildDenseIndexIfMissingOrStale);
            }
        }
        return current;
    }

    public LedgerSegment writable() throws IOException {
        return writable(1024); // Default safety margin
    }

    private void addToSnapshotIfMissing(final LedgerSegment seg) {
        final LedgerSegment[] snap = segmentSnapshot;
        if (snap.length > 0 && snap[snap.length - 1] == seg) return;

        // Append; segments are monotonically increasing by offset.
        final LedgerSegment[] next = new LedgerSegment[snap.length + 1];
        System.arraycopy(snap, 0, next, 0, snap.length);
        next[next.length - 1] = seg;
        segmentSnapshot = next;
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

        // Once used (or ignored), clear the future reference.
        nextSegmentFuture = null;
        return nextActiveSegment;
    }

    private void preAllocateNextSegment() {
        if (nextSegmentFuture == null || nextSegmentFuture.isDone()) {
            final LedgerSegment current = activeSegment.get();
            final long base = (current != null) ? current.getLastOffset() : this.highWaterMark;
            nextSegmentFuture = segmentAllocatorService.submit(() -> createNewSegment(base));
        }
    }

    private LedgerSegment createNewSegment(final long previousSegmentLastOffset) throws IOException {
        segmentLock.lock();
        try {
            Path segmentPath;
            String fileName;
            do {
                fileName = String.format("%d-%06d%s", System.currentTimeMillis(),
                        ThreadLocalRandom.current().nextInt(1_000_000), LedgerConstant.SEGMENT_EXT);
                segmentPath = directory.resolve(fileName);
            } while (Files.exists(segmentPath));

            log.info("Creating segment: {}", segmentPath);
            return LedgerSegment.create(segmentPath, segmentCapacity, false, previousSegmentLastOffset);
        } finally {
            segmentLock.unlock();
        }
    }

    /**
     * Fetch visitor signature for zero-copy payload access from mmap.
     */
    @FunctionalInterface
    public interface PayloadVisitor {
        void accept(long offset, java.nio.MappedByteBuffer segmentBuffer, int payloadPos, int payloadLen);
    }

    /**
     * Visits up to maxMessages starting at offset (inclusive), across segment boundaries if needed.
     * Returns number of messages visited.
     */
    public int fetch(final long offset, final int maxMessages, final PayloadVisitor visitor) {
        if (maxMessages <= 0) return 0;

        long start = offset;
        if (start < 0) start = 0;

        int remaining = maxMessages;
        int visited = 0;

        while (remaining > 0) {
            final LedgerSegment seg = segmentForOffset(start);
            if (seg == null) break;

            final int n = seg.visitFromOffset(start, remaining, (off, buf, pos, len) ->
                    visitor.accept(off, buf, pos, len)
            );

            if (n <= 0) break;

            visited += n;
            remaining -= n;
            start += n;
        }

        return visited;
    }

    private LedgerSegment segmentForOffset(final long offset) {
        final LedgerSegment[] snap = segmentSnapshot;
        int lo = 0, hi = snap.length - 1;

        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final LedgerSegment s = snap[mid];

            final long fo = s.getFirstOffset();
            final long loff = s.getLastOffset();

            // Empty segments: firstOffset might be unset; treat as "no coverage".
            if (fo == Long.MIN_VALUE) {
                hi = mid - 1;
                continue;
            }

            if (offset < fo) {
                hi = mid - 1;
            } else if (offset > loff) {
                lo = mid + 1;
            } else {
                return s;
            }
        }
        return null;
    }

    @Override
    public void close() {
        // Stop allocator first to prevent creating new unused segments during close.
        segmentAllocatorService.shutdownNow();

        // If a preallocated segment exists but was never activated, close+delete it so tests don’t see it.
        if (nextSegmentFuture != null && nextSegmentFuture.isDone() && !nextSegmentFuture.isCancelled()) {
            try {
                final LedgerSegment pre = nextSegmentFuture.get();
                if (pre != null && !isInSnapshot(pre)) {
                    try { pre.close(); } catch (Exception ignored) {}
                    try { Files.deleteIfExists(pre.getFile()); } catch (Exception ignored) {}
                    try { Files.deleteIfExists(LedgerSegment.indexPathForSegment(pre.getFile())); } catch (Exception ignored) {}
                }
            } catch (Exception ignored) {
            }
        }

        // Deterministic: ensure every remaining .seg in snapshot has a .idx before returning.
        final LedgerSegment[] snap = segmentSnapshot;
        for (final LedgerSegment s : snap) {
            if (s != null) {
                try { s.buildDenseIndexIfMissingOrStale(); } catch (Exception ignored) {}
            }
        }

        // Close all segments (avoid mmap leaks).
        for (final LedgerSegment s : snap) {
            if (s != null) {
                try { s.close(); } catch (Exception ignored) {}
            }
        }

        final LedgerSegment current = activeSegment.getAndSet(null);
        if (current != null) try {
            current.close();
        } catch (Exception ignored) {
        }
    }

    private boolean isInSnapshot(final LedgerSegment seg) {
        final LedgerSegment[] snap = segmentSnapshot;
        for (final LedgerSegment s : snap) {
            if (s == seg) return true;
        }
        return false;
    }

    public void setHighWaterMark(final long newHwm) {
        if (newHwm > this.highWaterMark) this.highWaterMark = newHwm;
    }
}