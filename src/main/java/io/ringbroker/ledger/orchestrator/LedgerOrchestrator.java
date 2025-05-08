package io.ringbroker.ledger.orchestrator;

import io.ringbroker.ledger.constant.LedgerConstant;
import io.ringbroker.ledger.segment.LedgerSegment;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class LedgerOrchestrator {
    @NonNull
    private final Path dir;
    @Getter
    private final int segmentSize;
    @Getter
    private final long highWaterMark;

    // thread-safe holder of the current active segment
    private final AtomicReference<LedgerSegment> active = new AtomicReference<>();

    /**
     * Scan or create the directory, recover HWM, pick the tail segment.
     */
    public static LedgerOrchestrator bootstrap(@NonNull final Path dir,
                                               final int segmentSize) throws IOException {
        Files.createDirectories(dir);

        // Recover each existing segment file, then open it
        final List<LedgerSegment> segs = Files.list(dir)
                .filter(p -> p.getFileName().toString().endsWith(LedgerConstant.SEGMENT_EXT))
                .sorted(Comparator.comparing(Path::toString))
                .peek(path -> {
                    try {
                        recoverSegment(path);
                    } catch (final IOException e) {
                        throw new RuntimeException("Recovery failed for " + path, e);
                    }
                })
                .map(path -> {
                    try {
                        return LedgerSegment.openExisting(path);
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .sorted(Comparator.comparingLong(LedgerSegment::getFirstOffset))
                .toList();

        final long hwm = segs.isEmpty()
                ? 0L
                : segs.get(segs.size() - 1).getLastOffset();

        final LedgerOrchestrator mgr = new LedgerOrchestrator(dir, segmentSize, hwm);
        final LedgerSegment tail = segs.isEmpty()
                ? mgr.newSegment()
                : segs.get(segs.size() - 1);

        mgr.active.set(tail);
        return mgr;
    }

    /**
     * Return current segment or roll to a new one if full.
     */
    public LedgerSegment writable() throws IOException {
        final LedgerSegment seg = active.get();
        if (seg == null || seg.isFull()) {
            final LedgerSegment next = newSegment();
            active.set(next);
            return next;
        }
        return seg;
    }

    // must synchronize creation to avoid races
    private synchronized LedgerSegment newSegment() throws IOException {
        final String name = System.currentTimeMillis() + LedgerConstant.SEGMENT_EXT;
        final Path p = dir.resolve(name);
        log.info("Creating new segment {}", p);
        return LedgerSegment.create(p, segmentSize);
    }

    private static void recoverSegment(final Path segmentPath) throws IOException {
        try (final FileChannel channel = FileChannel.open(segmentPath, READ, WRITE);
             final DataInputStream in = new DataInputStream(Channels.newInputStream(channel))) {

            long pos = 0L;
            while (true) {
                final int length;
                try {
                    length = in.readInt();
                } catch (final EOFException eof) {
                    // clean EOF at record boundary
                    break;
                }

                final int storedCrc = in.readInt();

                final byte[] payload = new byte[length];
                final int actuallyRead = in.read(payload);
                if (actuallyRead < length) {
                    truncate(channel, pos);
                    break;
                }

                final Checksum crc = new CRC32();
                crc.update(payload, 0, length);

                if ((int) crc.getValue() != storedCrc) {
                    truncate(channel, pos);
                    break;
                }

                // advance to next record
                pos += 4 + 4 + length;
            }
        }
    }

    private static void truncate(final FileChannel channel, final long offset) throws IOException {
        log.warn("Truncating segment {} at offset {}", channel, offset);
        channel.truncate(offset);
        channel.force(false);
    }
}
