package io.ringbroker.broker.ingress;

import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.membership.member.Member;
import io.ringbroker.cluster.membership.replicator.AdaptiveReplicator;
import io.ringbroker.cluster.membership.resolver.ReplicaSetResolver;
import io.ringbroker.cluster.metadata.EpochMetadata;
import io.ringbroker.cluster.metadata.EpochPlacement;
import io.ringbroker.cluster.metadata.LogConfiguration;
import io.ringbroker.cluster.metadata.JournaledLogMetadataStore;
import io.ringbroker.core.lsn.Lsn;
import io.ringbroker.core.wait.Blocking;
import io.ringbroker.offset.InMemoryOffsetStore;
import io.ringbroker.registry.TopicRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.MappedByteBuffer;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
    void publishPrefersMetadataOwnerOverModulo(@TempDir final Path dir) throws Exception {
        final TopicRegistry registry = TopicRegistry.builder()
                .topic("t", BrokerApi.Message.getDescriptor())
                .build();
        final InMemoryOffsetStore offsets = new InMemoryOffsetStore(dir.resolve("offsets"));
        final JournaledLogMetadataStore metadata = new JournaledLogMetadataStore(dir.resolve("meta"));

        final RecordingRemoteClient owner2 = new RecordingRemoteClient();
        final ConcurrentHashMap<Integer, RemoteBrokerClient> clients = new ConcurrentHashMap<>();
        clients.put(2, owner2);

        final AdaptiveReplicator replicator = new AdaptiveReplicator(1, clients, 25);
        final List<Member> members = List.of(
                new Member(0, BrokerRole.PERSISTENCE, new java.net.InetSocketAddress("localhost", 0), System.currentTimeMillis(), 1),
                new Member(1, BrokerRole.PERSISTENCE, new java.net.InetSocketAddress("localhost", 0), System.currentTimeMillis(), 1),
                new Member(2, BrokerRole.PERSISTENCE, new java.net.InetSocketAddress("localhost", 0), System.currentTimeMillis(), 1)
        );
        final ReplicaSetResolver resolver = new ReplicaSetResolver(2, () -> members);

        final ClusteredIngress ingress = ClusteredIngress.create(
                registry,
                (key, total) -> 0,
                1,
                0,
                3,
                clients,
                dir.resolve("data"),
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

        // Force placement owner to node 2, different from modulo owner 0.
        final EpochPlacement owner2Placement = new EpochPlacement(0L, List.of(2, 0), 1);
        final LogConfiguration cfg = new LogConfiguration(0, 99L, List.of(
                new EpochMetadata(0L, 0L, -1L, owner2Placement, 0L)
        ));
        metadata.applyRemote(cfg);

        ingress.publish("t", null, "x".getBytes()).join();

        assertEquals(1, owner2.ackCalls.get(), "publish should be forwarded to metadata owner");
        assertTrue(owner2.lastPublish.get() != null && owner2.lastPublish.get().getPublish().getPartitionId() == 0);

        ingress.shutdown();
        offsets.close();
    }

    @Test
    void forwardRetriesOnTimeoutThenFails(@TempDir final Path dir) throws Exception {
        final TopicRegistry registry = TopicRegistry.builder()
                .topic("t", BrokerApi.Message.getDescriptor())
                .build();
        final InMemoryOffsetStore offsets = new InMemoryOffsetStore(dir.resolve("offsets"));
        final JournaledLogMetadataStore metadata = new JournaledLogMetadataStore(dir.resolve("meta"));

        final NeverAckRemoteClient hung = new NeverAckRemoteClient();
        final ConcurrentHashMap<Integer, RemoteBrokerClient> clients = new ConcurrentHashMap<>();
        clients.put(1, hung);

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
                dir.resolve("data"),
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

        final EpochPlacement remoteOwner = new EpochPlacement(0L, List.of(1, 0), 1);
        metadata.applyRemote(new LogConfiguration(0, 101L, List.of(
                new EpochMetadata(0L, 0L, -1L, remoteOwner, 0L)
        )));

        final CompletionException ex = assertThrows(CompletionException.class,
                () -> ingress.publish("t", null, "z".getBytes()).join());
        assertTrue(rootCause(ex) instanceof TimeoutException, "final error should be timeout");
        assertEquals(3, hung.calls.get(), "should attempt initial send + 2 retries");

        ingress.shutdown();
        offsets.close();
    }

    @Test
    void backfillPaginatesAndReconstructsEpoch(@TempDir final Path dir) throws Exception {
        final TopicRegistry registry = TopicRegistry.builder()
                .topic("t", BrokerApi.Message.getDescriptor())
                .build();
        final InMemoryOffsetStore offsets = new InMemoryOffsetStore(dir.resolve("offsets"));
        final JournaledLogMetadataStore metadata = new JournaledLogMetadataStore(dir.resolve("meta"));

        final PagedBackfillClient backfillClient = new PagedBackfillClient(
                List.of("a".getBytes(), "b".getBytes(), "c".getBytes())
        );
        final ConcurrentHashMap<Integer, RemoteBrokerClient> clients = new ConcurrentHashMap<>();
        clients.put(1, backfillClient);

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
                dir.resolve("data"),
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

        final EpochPlacement placement = new EpochPlacement(1L, List.of(0, 1), 1);
        metadata.applyRemote(new LogConfiguration(0, 77L, List.of(
                new EpochMetadata(1L, 0L, 2L, placement, 0L)
        )));

        final var tick = ClusteredIngress.class.getDeclaredMethod("backfillTick");
        tick.setAccessible(true);
        tick.invoke(ingress);

        assertEquals(List.of(0L, 2L), backfillClient.requestedOffsets, "backfill should page offsets");

        final List<String> seen = new ArrayList<>();
        final Ingress local = ingress.getIngressMap().get(0);
        assertNotNull(local);
        local.fetchEpoch(1L, 0L, 10, (off, segBuf, payloadPos, payloadLen) -> {
            final byte[] p = readPayload(segBuf, payloadPos, payloadLen);
            seen.add(new String(p));
        });
        assertEquals(List.of("a", "b", "c"), seen);

        ingress.shutdown();
        offsets.close();
    }

    @Test
    void subscribeResumesFromLsnAndFallsBackToLedgerWhenRingOverrun(@TempDir final Path dir) throws Exception {
        final TopicRegistry registry = TopicRegistry.builder()
                .topic("t", BrokerApi.Message.getDescriptor())
                .build();
        final InMemoryOffsetStore offsets = new InMemoryOffsetStore(dir.resolve("offsets"));
        final JournaledLogMetadataStore metadata = new JournaledLogMetadataStore(dir.resolve("meta"));
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
                dir.resolve("data"),
                4,   // intentionally tiny to force ring overrun
                new Blocking(),
                512,
                1,
                false,
                offsets,
                BrokerRole.PERSISTENCE,
                resolver,
                replicator,
                metadata
        );

        final int total = 12;
        for (int i = 0; i < total; i++) {
            ingress.publish("t", null, ("m" + i).getBytes()).join();
        }

        final long startSeq = 3L;
        final long startLsn = Lsn.encode(0L, startSeq);
        offsets.commit("t", "g", 0, startLsn);

        final CountDownLatch latch = new CountDownLatch(total - (int) startSeq);
        final CopyOnWriteArrayList<String> payloads = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<Long> offsetsSeen = new CopyOnWriteArrayList<>();

        ingress.subscribeTopic("t", "g", (lsn, payload) -> {
            payloads.add(new String(payload));
            offsetsSeen.add(lsn);
            latch.countDown();
        });

        assertTrue(latch.await(10, TimeUnit.SECONDS), "subscription should replay from committed LSN");
        assertEquals(total - (int) startSeq, payloads.size());
        assertEquals("m3", payloads.get(0));
        assertEquals("m11", payloads.get(payloads.size() - 1));
        assertEquals(Lsn.encode(0L, 3L), offsetsSeen.get(0));
        assertEquals(Lsn.encode(0L, 11L), offsets.fetch("t", "g", 0));

        ingress.shutdown();
        offsets.close();
    }

    @Test
    void appendBackfillEncodedBatchRollsAcrossSegments(@TempDir final Path dir) throws Exception {
        final TopicRegistry registry = TopicRegistry.builder()
                .topic("t", BrokerApi.Message.getDescriptor())
                .build();
        final InMemoryOffsetStore offsets = new InMemoryOffsetStore(dir.resolve("offsets"));
        final JournaledLogMetadataStore metadata = new JournaledLogMetadataStore(dir.resolve("meta"));
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
                dir.resolve("data"),
                8,
                new Blocking(),
                96, // tiny segment to force multiple segment rolls
                1,
                false,
                offsets,
                BrokerRole.PERSISTENCE,
                resolver,
                replicator,
                metadata
        );

        final Ingress local = ingress.getIngressMap().get(0);
        assertNotNull(local);

        final List<byte[]> records = List.of(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".getBytes(),
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".getBytes(),
                "cccccccccccccccccccccccccccccccccccccccc".getBytes()
        );

        final int appended = local.appendBackfillEncodedBatch(0L, ByteBuffer.wrap(encodeFramed(records)), 64);
        assertEquals(3, appended);
        assertEquals(2L, local.highWaterMark(0L));

        final List<String> seen = new ArrayList<>();
        local.fetchEpoch(0L, 0L, 10, (off, segBuf, payloadPos, payloadLen) -> {
            seen.add(new String(readPayload(segBuf, payloadPos, payloadLen)));
        });
        assertEquals(List.of(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "cccccccccccccccccccccccccccccccccccccccc"
        ), seen);

        ingress.shutdown();
        offsets.close();
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

    private static Throwable rootCause(final Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null) cur = cur.getCause();
        return cur;
    }

    private static byte[] readPayload(final MappedByteBuffer buffer, final int pos, final int len) {
        final byte[] out = new byte[len];
        final var dup = buffer.duplicate();
        dup.position(pos);
        dup.get(out, 0, len);
        return out;
    }

    private static byte[] encodeFramed(final List<byte[]> records) {
        int totalBytes = 0;
        for (final byte[] rec : records) totalBytes += Integer.BYTES + rec.length;

        final byte[] out = new byte[totalBytes];
        int pos = 0;
        for (final byte[] rec : records) {
            out[pos] = (byte) rec.length;
            out[pos + 1] = (byte) (rec.length >>> 8);
            out[pos + 2] = (byte) (rec.length >>> 16);
            out[pos + 3] = (byte) (rec.length >>> 24);
            pos += Integer.BYTES;
            System.arraycopy(rec, 0, out, pos, rec.length);
            pos += rec.length;
        }
        return out;
    }

    private static final class RecordingRemoteClient implements RemoteBrokerClient {
        final AtomicInteger ackCalls = new AtomicInteger();
        final AtomicReference<BrokerApi.Envelope> lastPublish = new AtomicReference<>();

        @Override
        public void sendMessage(final String topic, final byte[] key, final byte[] payload) {
        }

        @Override
        public CompletableFuture<BrokerApi.ReplicationAck> sendEnvelopeWithAck(final BrokerApi.Envelope envelope) {
            ackCalls.incrementAndGet();
            lastPublish.set(envelope);
            return CompletableFuture.completedFuture(BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                    .build());
        }
    }

    private static final class NeverAckRemoteClient implements RemoteBrokerClient {
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public void sendMessage(final String topic, final byte[] key, final byte[] payload) {
        }

        @Override
        public CompletableFuture<BrokerApi.ReplicationAck> sendEnvelopeWithAck(final BrokerApi.Envelope envelope) {
            calls.incrementAndGet();
            return new CompletableFuture<>();
        }
    }

    private static final class PagedBackfillClient implements RemoteBrokerClient {
        private final List<byte[]> records;
        private final CopyOnWriteArrayList<Long> requestedOffsets = new CopyOnWriteArrayList<>();

        PagedBackfillClient(final List<byte[]> records) {
            this.records = records;
        }

        @Override
        public void sendMessage(final String topic, final byte[] key, final byte[] payload) {
        }

        @Override
        public CompletableFuture<BrokerApi.ReplicationAck> sendEnvelopeWithAck(final BrokerApi.Envelope envelope) {
            return CompletableFuture.completedFuture(BrokerApi.ReplicationAck.newBuilder()
                    .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                    .build());
        }

        @Override
        public CompletableFuture<BrokerApi.BackfillReply> sendBackfill(final BrokerApi.Envelope envelope) {
            final long offset = envelope.getBackfill().getOffset();
            requestedOffsets.add(offset);

            final int start = (int) offset;
            if (start >= records.size()) {
                return CompletableFuture.completedFuture(BrokerApi.BackfillReply.newBuilder()
                        .setEndOfEpoch(true)
                        .build());
            }

            final int endExclusive = Math.min(records.size(), start + 2); // 2 records per page
            int totalBytes = 0;
            for (int i = start; i < endExclusive; i++) {
                totalBytes += Integer.BYTES + records.get(i).length;
            }
            final byte[] payload = new byte[totalBytes];
            int pos = 0;
            for (int i = start; i < endExclusive; i++) {
                final byte[] rec = records.get(i);
                payload[pos] = (byte) (rec.length);
                payload[pos + 1] = (byte) (rec.length >>> 8);
                payload[pos + 2] = (byte) (rec.length >>> 16);
                payload[pos + 3] = (byte) (rec.length >>> 24);
                pos += Integer.BYTES;
                System.arraycopy(rec, 0, payload, pos, rec.length);
                pos += rec.length;
            }

            return CompletableFuture.completedFuture(BrokerApi.BackfillReply.newBuilder()
                    .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                    .setEndOfEpoch(endExclusive >= records.size())
                    .build());
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
