package io.ringbroker.ledger.orchestrator;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-partition virtual log that multiplexes epochs to independent LedgerOrchestrators,
 * each under partitionDir/epoch-XXXXXXXX.
 */
@Slf4j
@Getter
public final class VirtualLog implements AutoCloseable {
    private final Path partitionDir;
    private final int segmentCapacity;
    private final Map<Long, LedgerOrchestrator> epochs = new ConcurrentHashMap<>();
    private final Map<Long, Boolean> present = new ConcurrentHashMap<>();

    public VirtualLog(final Path partitionDir, final int segmentCapacity) {
        this.partitionDir = partitionDir;
        this.segmentCapacity = segmentCapacity;
    }

    public LedgerOrchestrator forEpoch(final long epoch) {
        return epochs.computeIfAbsent(epoch, e -> {
            try {
                final Path dir = dirForEpoch(e);
                Files.createDirectories(dir);
                present.put(e, Boolean.TRUE);
                return LedgerOrchestrator.bootstrap(dir, segmentCapacity);
            } catch (final IOException ex) {
                throw new RuntimeException("Failed to bootstrap epoch dir for " + epoch, ex);
            }
        });
    }

    public boolean hasEpoch(final long epoch) {
        if (epochs.containsKey(epoch)) return true;
        if (present.containsKey(epoch)) return true;
        final boolean exists = Files.isDirectory(dirForEpoch(epoch));
        if (exists) present.put(epoch, Boolean.TRUE);
        return exists;
    }

    public Path dirForEpoch(final long epoch) {
        return partitionDir.resolve(String.format("epoch-%010d", epoch));
    }

    /**
     * Discover existing epochs on disk and mark them present without opening.
     */
    public void discoverOnDisk() {
        try {
            Files.list(partitionDir)
                    .filter(p -> p.getFileName().toString().startsWith("epoch-"))
                    .forEach(p -> {
                        final String name = p.getFileName().toString();
                        try {
                            final long epoch = Long.parseLong(name.substring("epoch-".length()));
                            present.put(epoch, Boolean.TRUE);
                        } catch (final NumberFormatException ignored) {
                        }
                    });
        } catch (final IOException ignored) {
        }
    }

    @Override
    public void close() throws IOException {
        for (final LedgerOrchestrator lo : epochs.values()) {
            lo.close();
        }
    }
}
