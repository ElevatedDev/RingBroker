package io.ringbroker.broker.ingress;

import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.membership.member.Member;
import io.ringbroker.cluster.membership.replicator.AdaptiveReplicator;
import io.ringbroker.cluster.membership.resolver.ReplicaSetResolver;
import io.ringbroker.cluster.metadata.JournaledLogMetadataStore;
import io.ringbroker.core.wait.Blocking;
import io.ringbroker.offset.InMemoryOffsetStore;
import io.ringbroker.registry.TopicRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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

    private record Components(ClusteredIngress ingress, AutoCloseable offsets) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            ingress.shutdown();
            offsets.close();
        }
    }
}
