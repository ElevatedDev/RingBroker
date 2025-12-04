package io.ringbroker.cluster.metadata;

import java.util.*;

public record LogConfiguration(int partitionId, long configVersion, List<EpochMetadata> epochs) {
    public LogConfiguration(final int partitionId,
                            final long configVersion,
                            final List<EpochMetadata> epochs) {
        this.partitionId = partitionId;
        this.configVersion = configVersion;
        if (epochs == null || epochs.isEmpty()) {
            throw new IllegalArgumentException("epochs must not be empty");
        }
        final List<EpochMetadata> sorted = new ArrayList<>(epochs);
        sorted.sort(Comparator.comparingLong(EpochMetadata::epoch));
        this.epochs = Collections.unmodifiableList(sorted);
    }

    public EpochMetadata activeEpoch() {
        return epochs.getLast();
    }

    public EpochMetadata epoch(final long epochId) {
        for (final EpochMetadata e : epochs) {
            if (e.epoch() == epochId) return e;
        }
        return null;
    }

    public LogConfiguration sealActive(final long sealedEndSeq) {
        final EpochMetadata active = activeEpoch();
        final List<EpochMetadata> copy = new ArrayList<>(epochs);
        copy.set(copy.size() - 1, active.seal(sealedEndSeq));
        return new LogConfiguration(partitionId, configVersion + 1, copy);
    }

    public LogConfiguration appendEpoch(final EpochMetadata next) {
        Objects.requireNonNull(next, "next");
        final List<EpochMetadata> copy = new ArrayList<>(epochs);
        copy.add(next);
        return new LogConfiguration(partitionId, configVersion + 1, copy);
    }
}
