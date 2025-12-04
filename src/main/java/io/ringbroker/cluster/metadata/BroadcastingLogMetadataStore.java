package io.ringbroker.cluster.metadata;

import io.ringbroker.api.BrokerApi;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Wraps a local {@link LogMetadataStore} and broadcasts updates to peers.
 * Highest configVersion wins on receivers (no consensus; leader/owner should serialize writers).
 */
@RequiredArgsConstructor
@Slf4j
public final class BroadcastingLogMetadataStore implements LogMetadataStore {
    private final LogMetadataStore delegate;
    private final Map<Integer, RemoteBrokerClient> clients;
    private final int myNodeId;
    private final java.util.function.Supplier<Collection<Integer>> clusterView;

    public static LogConfiguration fromProto(final BrokerApi.MetadataUpdate update) {
        final List<EpochMetadata> epochs = update.getEpochsList().stream()
                .map(ec -> new EpochMetadata(
                        ec.getEpoch(),
                        ec.getStartSeq(),
                        ec.getEndSeq(),
                        new EpochPlacement(ec.getEpoch(), ec.getStorageNodesList(), ec.getAckQuorum()),
                        ec.getTieBreaker()
                ))
                .toList();
        return new LogConfiguration(update.getPartitionId(), update.getConfigVersion(), epochs);
    }

    @Override
    public Optional<LogConfiguration> current(final int partitionId) {
        return delegate.current(partitionId);
    }

    @Override
    public LogConfiguration bootstrapIfAbsent(final int partitionId,
                                              final EpochPlacement placement,
                                              final long startSeqInclusive) {
        final LogConfiguration cfg = delegate.bootstrapIfAbsent(partitionId, placement, startSeqInclusive);
        broadcast(cfg);
        return cfg;
    }

    @Override
    public LogConfiguration sealAndCreateEpoch(final int partitionId,
                                               final long activeEpoch,
                                               final long sealedEndSeq,
                                               final EpochPlacement newPlacement,
                                               final long newEpochId,
                                               final long tieBreaker) {
        final LogConfiguration cfg = delegate.sealAndCreateEpoch(partitionId, activeEpoch, sealedEndSeq, newPlacement, newEpochId, tieBreaker);
        broadcast(cfg);
        return cfg;
    }

    @Override
    public void applyRemote(final LogConfiguration cfg) {
        delegate.applyRemote(cfg);
    }

    private void broadcast(final LogConfiguration cfg) {
        final BrokerApi.MetadataUpdate payload = toProto(cfg);
        final BrokerApi.Envelope env = BrokerApi.Envelope.newBuilder()
                .setCorrelationId(System.nanoTime())
                .setMetadataUpdate(payload)
                .build();

        final Collection<Integer> view = clusterView.get();
        for (final Integer nodeId : view) {
            if (nodeId == myNodeId) continue;
            final RemoteBrokerClient client = clients.get(nodeId);
            if (client == null) continue;
            try {
                client.sendEnvelopeWithAck(env)
                        .orTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
                        .exceptionally(ex -> {
                            log.debug("Metadata broadcast to {} failed: {}", nodeId, ex.toString());
                            return null;
                        });
            } catch (final Throwable t) {
                log.debug("Metadata broadcast to {} failed: {}", nodeId, t.toString());
            }
        }
    }

    private BrokerApi.MetadataUpdate toProto(final LogConfiguration cfg) {
        final BrokerApi.MetadataUpdate.Builder b = BrokerApi.MetadataUpdate.newBuilder()
                .setPartitionId(cfg.partitionId())
                .setConfigVersion(cfg.configVersion());
        for (final EpochMetadata em : cfg.epochs()) {
            b.addEpochs(BrokerApi.EpochConfig.newBuilder()
                    .setEpoch(em.epoch())
                    .setStartSeq(em.startSeq())
                    .setEndSeq(em.endSeq())
                    .setAckQuorum(em.placement().getAckQuorum())
                    .addAllStorageNodes(em.placement().getStorageNodes())
                    .setTieBreaker(em.tieBreaker())
                    .build());
        }
        return b.build();
    }
}
