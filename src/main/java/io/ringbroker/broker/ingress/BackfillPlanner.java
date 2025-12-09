package io.ringbroker.broker.ingress;

import io.ringbroker.cluster.metadata.EpochMetadata;
import io.ringbroker.cluster.metadata.LogConfiguration;
import io.ringbroker.cluster.metadata.LogMetadataStore;
import lombok.RequiredArgsConstructor;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tracks which epochs are missing locally and whether backfill is needed.
 * Light-weight: uses metadata store and a bitmap of known-present epochs.
 */
@RequiredArgsConstructor
final class BackfillPlanner {
    private final LogMetadataStore metadataStore;
    private final ConcurrentMap<Integer, ConcurrentMap<Long, Boolean>> presentByPartition = new ConcurrentHashMap<>();

    boolean hasEpoch(final int partitionId, final long epoch) {
        final ConcurrentMap<Long, Boolean> m = presentByPartition.get(partitionId);
        if (m != null && m.containsKey(epoch)) return true;
        final Optional<LogConfiguration> cfg = metadataStore.current(partitionId);
        return cfg.isPresent() && cfg.get().epoch(epoch) != null;
    }

    void markPresent(final int partitionId, final long epoch) {
        presentByPartition.computeIfAbsent(partitionId, __ -> new ConcurrentHashMap<>())
                .put(epoch, Boolean.TRUE);
    }

    Optional<EpochMetadata> epochMeta(final int partitionId, final long epoch) {
        final Optional<LogConfiguration> cfg = metadataStore.current(partitionId);
        if (cfg.isEmpty()) return Optional.empty();
        return Optional.ofNullable(cfg.get().epoch(epoch));
    }

    /**
     * Decide if this node should have the epoch: present in placement and epoch is sealed in metadata.
     */
    boolean shouldHaveSealedEpoch(final int partitionId, final long epoch) {
        final Optional<EpochMetadata> em = epochMeta(partitionId, epoch);
        if (em.isEmpty()) return false;
        return em.get().isSealed();
    }
}
