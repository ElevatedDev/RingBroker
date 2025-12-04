package io.ringbroker.cluster.metadata;

import io.ringbroker.api.BrokerApi;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

final class BroadcastingLogMetadataStoreTest {

    private static final class CapturingClient implements RemoteBrokerClient {
        final ConcurrentLinkedQueue<BrokerApi.Envelope> sent = new ConcurrentLinkedQueue<>();

        @Override
        public void sendMessage(final String topic, final byte[] key, final byte[] payload) {
            // not used
        }

        @Override
        public CompletableFuture<BrokerApi.ReplicationAck> sendEnvelopeWithAck(final BrokerApi.Envelope envelope) {
            sent.add(envelope);
            return CompletableFuture.completedFuture(
                    BrokerApi.ReplicationAck.newBuilder().setStatus(BrokerApi.ReplicationAck.Status.SUCCESS).build()
            );
        }

        @Override
        public CompletableFuture<BrokerApi.BackfillReply> sendBackfill(final BrokerApi.Envelope envelope) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void broadcastsOnSealAndCreate() throws Exception {
        final CapturingClient peer = new CapturingClient();
        final LogMetadataStore delegate = new LogMetadataStore() {
            LogConfiguration cfg;

            @Override
            public java.util.Optional<LogConfiguration> current(final int partitionId) {
                return java.util.Optional.ofNullable(cfg);
            }

            @Override
            public LogConfiguration bootstrapIfAbsent(final int partitionId, final EpochPlacement placement, final long startSeqInclusive) {
                if (cfg == null) {
                    cfg = new LogConfiguration(partitionId, 1L, List.of(new EpochMetadata(0L, startSeqInclusive, -1L, placement, 0L)));
                }
                return cfg;
            }

            @Override
            public LogConfiguration sealAndCreateEpoch(final int partitionId, final long activeEpoch, final long sealedEndSeq, final EpochPlacement newPlacement, final long newEpochId, final long tieBreaker) {
                final LogConfiguration sealed = cfg.sealActive(sealedEndSeq);
                cfg = sealed.appendEpoch(new EpochMetadata(newEpochId, sealedEndSeq + 1, -1L, newPlacement, tieBreaker));
                return cfg;
            }

            @Override
            public void applyRemote(final LogConfiguration cfg) {
                this.cfg = cfg;
            }
        };

        final Map<Integer, RemoteBrokerClient> clients = Map.of(2, peer);
        final Collection<Integer> view = List.of(1, 2);

        final BroadcastingLogMetadataStore store = new BroadcastingLogMetadataStore(delegate, clients, 1, () -> view);
        final EpochPlacement p0 = new EpochPlacement(0L, List.of(1, 2), 1);
        store.bootstrapIfAbsent(5, p0, 0);
        final EpochPlacement p1 = new EpochPlacement(1L, List.of(2), 1);
        store.sealAndCreateEpoch(5, 0L, 10L, p1, 1L, 77L);

        // Ensure a metadata_update was sent with the latest configVersion and tieBreaker
        BrokerApi.Envelope env = null;
        for (BrokerApi.Envelope e : peer.sent) {
            env = e;
        }
        assertNotNull(env);
        assertTrue(env.hasMetadataUpdate());
        final BrokerApi.MetadataUpdate upd = env.getMetadataUpdate();
        assertEquals(5, upd.getPartitionId());
        assertTrue(upd.getConfigVersion() >= 2L);
        assertEquals(2, upd.getEpochsCount());
        assertEquals(77L, upd.getEpochs(1).getTieBreaker());
    }
}
