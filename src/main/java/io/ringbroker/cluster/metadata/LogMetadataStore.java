package io.ringbroker.cluster.metadata;

import java.util.Optional;

/**
 * Strongly serialized metadata store for per-partition log configuration.
 * Implementations must fence stale writers using the monotonically increasing
 * configVersion in {@link LogConfiguration}.
 */
public interface LogMetadataStore {

    /**
     * Returns the current configuration for the partition, if any.
     */
    Optional<LogConfiguration> current(int partitionId);

    /**
     * Initialize a partition with the given placement and starting sequence, if absent.
     * Returns the resulting configuration (existing or newly created).
     */
    LogConfiguration bootstrapIfAbsent(int partitionId,
                                       EpochPlacement placement,
                                       long startSeqInclusive);

    /**
     * Seal the active epoch and start a new epoch with the provided placement.
     *
     * @param partitionId  partition identifier
     * @param activeEpoch  expected active epoch (for fencing)
     * @param sealedEndSeq final sequence in the sealed epoch
     * @param newPlacement placement for the new epoch
     * @param newEpochId   the new epoch id to append
     * @param tieBreaker   deterministic token to break concurrent open attempts
     * @return updated configuration
     */
    LogConfiguration sealAndCreateEpoch(int partitionId,
                                        long activeEpoch,
                                        long sealedEndSeq,
                                        EpochPlacement newPlacement,
                                        long newEpochId,
                                        long tieBreaker);

    /**
     * Apply a remote configuration update if it is newer than the local view.
     */
    void applyRemote(LogConfiguration cfg);
}
