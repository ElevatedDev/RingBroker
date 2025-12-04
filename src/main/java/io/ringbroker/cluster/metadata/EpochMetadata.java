package io.ringbroker.cluster.metadata;

import java.util.Objects;

/**
 * @param endSeq -1 while active
 */
public record EpochMetadata(long epoch, long startSeq, long endSeq, EpochPlacement placement, long tieBreaker) {
    public EpochMetadata(final long epoch,
                         final long startSeq,
                         final long endSeq,
                         final EpochPlacement placement,
                         final long tieBreaker) {
        if (startSeq < 0) {
            throw new IllegalArgumentException("startSeq must be >= 0");
        }
        if (endSeq >= 0 && endSeq < startSeq) {
            throw new IllegalArgumentException("endSeq cannot be < startSeq when sealed");
        }
        this.epoch = epoch;
        this.startSeq = startSeq;
        this.endSeq = endSeq;
        this.placement = Objects.requireNonNull(placement, "placement");
        this.tieBreaker = tieBreaker;
    }

    public boolean isSealed() {
        return endSeq >= 0;
    }

    public EpochMetadata seal(final long sealedEndSeq) {
        return new EpochMetadata(epoch, startSeq, sealedEndSeq, placement, tieBreaker);
    }
}
