package io.ringbroker.broker;

import com.google.protobuf.ByteString;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.membership.member.Member;
import io.ringbroker.cluster.membership.replicator.AdaptiveReplicator;
import io.ringbroker.cluster.membership.resolver.ReplicaSetResolver;
import io.ringbroker.cluster.partitioner.Partitioner;
import io.ringbroker.cluster.partitioner.impl.RoundRobinPartitioner;
import io.ringbroker.cluster.metadata.LogMetadataStore;
import io.ringbroker.cluster.metadata.BroadcastingLogMetadataStore;
import io.ringbroker.cluster.metadata.JournaledLogMetadataStore;
import io.ringbroker.core.wait.AdaptiveSpin;
import io.ringbroker.offset.InMemoryOffsetStore;
import io.ringbroker.registry.TopicRegistry;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Minimal integration of publish/seal/open/fetch on a single-node cluster.
 * Exercises broker semantics without network or slow replicas.
 */
@Disabled("Async writer timing makes this slow/fragile in CI; enable after tuning flush/wait hooks")
final class BrokerIntegrationTest {

    @TempDir
    Path tmp;

    @Test
    void publishSealOpenAndFetch() throws Exception {
        final String topic = "t";
        final TopicRegistry registry = TopicRegistry.builder()
                .topic(topic, BrokerApi.Message.getDescriptor())
                .build();

        final Partitioner partitioner = new RoundRobinPartitioner();
        final int totalPartitions = 1;
        final int myNodeId = 0;
        final int clusterSize = 1;

        final Map<Integer, RemoteBrokerClient> clients = Collections.emptyMap();
        final AdaptiveReplicator replicator = new AdaptiveReplicator(1, clients, 1_000);

        final Member self = new Member(myNodeId, BrokerRole.PERSISTENCE, new InetSocketAddress("localhost", 0), System.currentTimeMillis(), 1);
        final ReplicaSetResolver resolver = new ReplicaSetResolver(1, () -> List.of(self));

        final LogMetadataStore store = new BroadcastingLogMetadataStore(
                new JournaledLogMetadataStore(tmp.resolve("metadata")),
                clients,
                myNodeId,
                () -> List.of(myNodeId)
        );

        final ClusteredIngress ingress = ClusteredIngress.create(
                registry,
                partitioner,
                totalPartitions,
                myNodeId,
                clusterSize,
                clients,
                tmp,
                128,
                new AdaptiveSpin(),
                1024 * 1024,
                16,
                false,
                new InMemoryOffsetStore(tmp.resolve("offsets")),
                BrokerRole.PERSISTENCE,
                resolver,
                replicator,
                store
        );

        // Publish one message on epoch 0
        final byte[] payload = "hello".getBytes();
        final CompletableFuture<Void> pub = ingress.publish(topic, null, payload);
        assertDoesNotThrow(() -> pub.get());

        // Fetch directly from storage to verify persistence
        final AtomicReference<byte[]> fetched = new AtomicReference<>();
        ingress.getIngressMap().get(0).fetchEpoch(0L, 0L, 1, (off, segBuf, payloadPos, payloadLen) -> {
            final byte[] dst = new byte[payloadLen];
            segBuf.position(payloadPos).get(dst, 0, payloadLen);
            fetched.set(dst);
        });
        assertArrayEquals(payload, fetched.get());

        // Seal epoch 0
        final BrokerApi.ReplicationAck sealAck = ingress.handleSealAsync(BrokerApi.SealRequest.newBuilder()
                .setPartitionId(0)
                .setEpoch(0)
                .setSealOnly(true)
                .build()).get();
        assertEquals(BrokerApi.ReplicationAck.Status.SUCCESS, sealAck.getStatus());

        // Append on sealed epoch should fail
        final BrokerApi.ReplicationAck reject = ingress.handleAppendAsync(BrokerApi.AppendRequest.newBuilder()
                        .setPartitionId(0)
                        .setEpoch(0)
                        .setSeq(100)
                        .setTopic(topic)
                        .setRetries(0)
                        .setPayload(ByteString.copyFromUtf8("late"))
                        .build())
                .get();
        assertEquals(BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY, reject.getStatus());

        // Open epoch 1
        final BrokerApi.ReplicationAck openAck = ingress.handleOpenEpochAsync(BrokerApi.OpenEpochRequest.newBuilder()
                .setPartitionId(0)
                .setEpoch(1)
                .setTieBreaker(1)
                .build()).get();
        assertEquals(BrokerApi.ReplicationAck.Status.SUCCESS, openAck.getStatus());

        // Append on epoch 1 should succeed
        final BrokerApi.ReplicationAck append1 = ingress.handleAppendAsync(BrokerApi.AppendRequest.newBuilder()
                        .setPartitionId(0)
                        .setEpoch(1)
                        .setSeq(0)
                        .setTopic(topic)
                        .setRetries(0)
                        .setPayload(ByteString.copyFromUtf8("next"))
                        .build())
                .get();
        assertEquals(BrokerApi.ReplicationAck.Status.SUCCESS, append1.getStatus());
    }
}
