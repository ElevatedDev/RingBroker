package io.ringbroker.broker.ingress;

import com.google.protobuf.ByteString;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.membership.member.Member;
import io.ringbroker.cluster.membership.replicator.AdaptiveReplicator;
import io.ringbroker.cluster.membership.resolver.ReplicaSetResolver;
import io.ringbroker.cluster.metadata.EpochPlacement;
import io.ringbroker.cluster.metadata.JournaledLogMetadataStore;
import io.ringbroker.core.wait.Blocking;
import io.ringbroker.offset.InMemoryOffsetStore;
import io.ringbroker.registry.TopicRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

final class ClusteredIngressTest {

    @Test
    void publishFailsWhenEpochSealed(@TempDir final Path dir) throws Exception {
        final Components c = singleNode(dir);

        // Publish once to establish seq>=0 on epoch 0.
        c.ingress.publish("t", null, "warmup".getBytes()).join();

        // Seal active epoch 0 locally.
        final BrokerApi.SealRequest seal = BrokerApi.SealRequest.newBuilder()
                .setPartitionId(0)
                .setEpoch(0)
                .setSealOnly(true)
                .build();
        c.ingress.handleSealAsync(seal).get();

        final CompletableFuture<Void> pub = c.ingress.publish("t", null, "x".getBytes());
        final CompletionException ex = assertThrows(CompletionException.class, pub::join, "publishes should fail on sealed epoch");
        assertInstanceOf(IllegalStateException.class, ex.getCause());

        c.close();
    }

    @Test
    void replicationTimeoutSurfacesToCaller(@TempDir final Path dir) throws Exception {
        final Components c = replicatedWithFailingRemote(dir);
        final CompletableFuture<Void> pub = c.ingress.publish("t", null, "y".getBytes());
        final CompletionException ex = assertThrows(CompletionException.class, pub::join, "replication timeout should reach caller");
        assertInstanceOf(TimeoutException.class, ex.getCause(), "inner cause should be TimeoutException");
        c.close();
    }

    @Test
    void backfillTickResumesFromLocalHighWatermark(@TempDir final Path dir) throws Exception {
        final RecordingBackfillClient peer = new RecordingBackfillClient(List.of(
                "m0".getBytes(),
                "m1".getBytes(),
                "m2".getBytes()
        ));

        final Components harness = backfillHarness(dir, peer);

        invokeBackfillTick(harness.ingress);

        final Ingress local = harness.ingress.getIngressMap().get(0);
        assertNotNull(local);
        assertEquals(2L, local.getVirtualLog().forEpoch(0L).getHighWaterMark(), "should backfill full sealed epoch");
        assertEquals(List.of(0L, 1L, 2L), peer.offsets(), "offset should advance between backfill batches");

        harness.close();
    }

    private Components singleNode(final Path base) throws Exception {
        final TopicRegistry registry = TopicRegistry.builder()
                .topic("t", BrokerApi.Message.getDescriptor())
                .build();
        final InMemoryOffsetStore offsets = new InMemoryOffsetStore(base.resolve("offsets"));
        final JournaledLogMetadataStore metadata = new JournaledLogMetadataStore(base.resolve("meta"));
        final AdaptiveReplicator replicator = new AdaptiveReplicator(1, Map.of(), 100);
        final List<Member> members = List.of(
                new Member(0, BrokerRole.PERSISTENCE, new java.net.InetSocketAddress("localhost", 0), System.currentTimeMillis(), 1)
        );
        final ReplicaSetResolver resolver = new ReplicaSetResolver(1, () -> members);

        final ClusteredIngress ingress = ClusteredIngress.create(
                registry,
                (key, total) -> 0,
                1,
                0,
                1,
                Map.of(),
                base.resolve("data"),
                8,
                new Blocking(),
                512,
                4,
                false,
                offsets,
                BrokerRole.PERSISTENCE,
                resolver,
                replicator,
                metadata
        );
        return new Components(ingress, offsets);
    }

    private Components replicatedWithFailingRemote(final Path base) throws Exception {
        final TopicRegistry registry = TopicRegistry.builder()
                .topic("t", BrokerApi.Message.getDescriptor())
                .build();
        final InMemoryOffsetStore offsets = new InMemoryOffsetStore(base.resolve("offsets"));
        final JournaledLogMetadataStore metadata = new JournaledLogMetadataStore(base.resolve("meta"));

        final RemoteBrokerClient failing = new RemoteBrokerClient() {
            @Override
            public void sendMessage(final String topic, final byte[] key, final byte[] payload) {
            }

            @Override
            public void sendEnvelope(final BrokerApi.Envelope envelope) {
            }

            @Override
            public CompletableFuture<BrokerApi.ReplicationAck> sendEnvelopeWithAck(final BrokerApi.Envelope envelope) {
                return CompletableFuture.failedFuture(new TimeoutException("replication timed out"));
            }
        };
        final Map<Integer, RemoteBrokerClient> clients = Map.of(1, failing);
        final AdaptiveReplicator replicator = new AdaptiveReplicator(1, clients, 10);

        final List<Member> members = List.of(
                new Member(0, BrokerRole.PERSISTENCE, new java.net.InetSocketAddress("localhost", 0), System.currentTimeMillis(), 1),
                new Member(1, BrokerRole.PERSISTENCE, new java.net.InetSocketAddress("localhost", 0), System.currentTimeMillis(), 1)
        );
        final ReplicaSetResolver resolver = new ReplicaSetResolver(2, () -> members);

        final ClusteredIngress ingress = ClusteredIngress.create(
                registry,
                (key, total) -> 0,
                1,
                0,
                2,
                clients,
                base.resolve("data"),
                8,
                new Blocking(),
                512,
                4,
                false,
                offsets,
                BrokerRole.PERSISTENCE,
                resolver,
                replicator,
                metadata
        );
        return new Components(ingress, offsets);
    }

    private Components backfillHarness(final Path base, final RecordingBackfillClient peer) throws Exception {
        final TopicRegistry registry = TopicRegistry.builder()
                .topic("t", BrokerApi.Message.getDescriptor())
                .build();
        final InMemoryOffsetStore offsets = new InMemoryOffsetStore(base.resolve("offsets"));
        final JournaledLogMetadataStore metadata = new JournaledLogMetadataStore(base.resolve("meta"));

        final EpochPlacement p0 = new EpochPlacement(0L, List.of(0, 1), 1);
        metadata.bootstrapIfAbsent(0, p0, 0L);
        final EpochPlacement p1 = new EpochPlacement(1L, List.of(0, 1), 1);
        metadata.sealAndCreateEpoch(0, 0L, peer.sealedEnd(), p1, 1L, 1L);

        final Map<Integer, RemoteBrokerClient> clients = Map.of(1, peer);
        final AdaptiveReplicator replicator = new AdaptiveReplicator(1, clients, 50);

        final List<Member> members = List.of(
                new Member(0, BrokerRole.PERSISTENCE, new java.net.InetSocketAddress("localhost", 0), System.currentTimeMillis(), 1),
                new Member(1, BrokerRole.PERSISTENCE, new java.net.InetSocketAddress("localhost", 0), System.currentTimeMillis(), 1)
        );
        final ReplicaSetResolver resolver = new ReplicaSetResolver(2, () -> members);

        final ClusteredIngress ingress = ClusteredIngress.create(
                registry,
                (key, total) -> 0,
                1,
                0,
                2,
                clients,
                base.resolve("data"),
                8,
                new Blocking(),
                512,
                4,
                false,
                offsets,
                BrokerRole.PERSISTENCE,
                resolver,
                replicator,
                metadata
        );
        return new Components(ingress, offsets);
    }

    private void invokeBackfillTick(final ClusteredIngress ingress) throws Exception {
        final var m = ClusteredIngress.class.getDeclaredMethod("backfillTick");
        m.setAccessible(true);
        m.invoke(ingress);
    }

    private static final class RecordingBackfillClient implements RemoteBrokerClient {
        private final List<byte[]> data;
        private final long sealedEnd;
        private final CopyOnWriteArrayList<Long> offsets = new CopyOnWriteArrayList<>();

        RecordingBackfillClient(final List<byte[]> data) {
            this.data = data;
            this.sealedEnd = data.size() - 1L;
        }

        long sealedEnd() {
            return sealedEnd;
        }

        List<Long> offsets() {
            return offsets;
        }

        @Override
        public void sendMessage(final String topic, final byte[] key, final byte[] payload) {
            // no-op for tests
        }

        @Override
        public CompletableFuture<BrokerApi.ReplicationAck> sendEnvelopeWithAck(final BrokerApi.Envelope envelope) {
            return CompletableFuture.completedFuture(
                    BrokerApi.ReplicationAck.newBuilder()
                            .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                            .build()
            );
        }

        @Override
        public CompletableFuture<BrokerApi.BackfillReply> sendBackfill(final BrokerApi.Envelope envelope) {
            final BrokerApi.BackfillRequest req = envelope.getBackfill();
            offsets.add(req.getOffset());

            final long off = req.getOffset();
            if (off >= data.size()) {
                return CompletableFuture.completedFuture(
                        BrokerApi.BackfillReply.newBuilder()
                                .setPayload(ByteString.EMPTY)
                                .setEndOfEpoch(true)
                                .build()
                );
            }

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            int count = 0;
            for (long i = off; i < data.size() && count < 1; i++) {
                final byte[] rec = data.get((int) i);
                out.write(rec.length & 0xFF);
                out.write((rec.length >>> 8) & 0xFF);
                out.write((rec.length >>> 16) & 0xFF);
                out.write((rec.length >>> 24) & 0xFF);
                out.write(rec, 0, rec.length);
                count++;
            }

            final boolean end = (off + count) > sealedEnd;
            return CompletableFuture.completedFuture(
                    BrokerApi.BackfillReply.newBuilder()
                            .setPayload(ByteString.copyFrom(out.toByteArray()))
                            .setEndOfEpoch(end)
                            .build()
            );
        }
    }

    private record Components(ClusteredIngress ingress, AutoCloseable offsets) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            ingress.shutdown();
            offsets.close();
        }
    }
}
