package io.ringbroker.broker.ingress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Minimal persistence for highestKnownEpoch per partition to enforce fencing across restarts.
 */
final class FenceStore {

    private FenceStore() {
    }

    static long loadHighest(final Path partitionDir) {
        final Path p = partitionDir.resolve("fence.meta");
        if (!Files.exists(p)) return -1L;
        try (final FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {
            final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
            if (ch.read(buf) != Long.BYTES) return -1L;
            buf.flip();
            return buf.getLong();
        } catch (final IOException ignored) {
            return -1L;
        }
    }

    static void storeHighest(final Path partitionDir, final long highest) {
        final Path p = partitionDir.resolve("fence.meta");
        final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        buf.putLong(highest).flip();
        try (final FileChannel ch = FileChannel.open(p,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            ch.write(buf);
            ch.force(true);
        } catch (final IOException ignored) {
        }
    }

    static void storeEpochFence(final Path partitionDir, final long epoch, final long sealedEndSeq, final long lastSeq, final boolean sealed) {
        final Path p = partitionDir.resolve(String.format("epoch-%010d.fence", epoch));
        final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * 2 + 1);
        buf.putLong(sealedEndSeq);
        buf.putLong(lastSeq);
        buf.put((byte) (sealed ? 1 : 0));
        buf.flip();
        try (final FileChannel ch = FileChannel.open(p,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            ch.write(buf);
            ch.force(true);
        } catch (final IOException ignored) {
        }
    }

    static PartitionFence loadEpochFence(final Path partitionDir, final long epoch) {
        final Path p = partitionDir.resolve(String.format("epoch-%010d.fence", epoch));
        if (!Files.exists(p)) return null;
        try (final FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {
            final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * 2 + 1);
            if (ch.read(buf) < buf.capacity()) return null;
            buf.flip();
            final long sealedEnd = buf.getLong();
            final long lastSeq = buf.getLong();
            final boolean sealed = buf.get() != 0;
            return new PartitionFence(sealedEnd, lastSeq, sealed);
        } catch (final IOException ignored) {
            return null;
        }
    }

    record PartitionFence(long sealedEndSeq, long lastSeq, boolean sealed) {
    }
}
