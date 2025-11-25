package io.ringbroker.test;

import com.google.protobuf.Timestamp;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.broker.ingress.Ingress;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.membership.member.Member;
import io.ringbroker.cluster.membership.replicator.AdaptiveReplicator;
import io.ringbroker.cluster.membership.resolver.ReplicaSetResolver;
import io.ringbroker.cluster.partitioner.Partitioner;
import io.ringbroker.core.wait.AdaptiveSpin;
import io.ringbroker.ledger.segment.LedgerSegment;
import io.ringbroker.offset.InMemoryOffsetStore;
import io.ringbroker.proto.test.EventsProto;
import io.ringbroker.registry.TopicRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Strong single-test cluster sanity:
 * - 3-node in-process cluster (no Netty)
 * - verifies forwarding (non-owner -> owner) happens
 * - verifies publish blocks until quorum acks for one gated message
 * - publishes many messages concurrently from all nodes
 * - verifies exactly-once delivery by collecting IDs from payloads (no missing/no dupes)
 * - verifies durability by replaying segment files on owners (no missing/no dupes)
 *
 * NEW:
 * - verifies ledger-backed FETCH (dense .idx index) returns exact expected IDs on owner partitions
 * - asserts .idx exists for each .seg on owners
 *
 * NOTE: Does NOT verify replica *storage* (current design acks do not imply persisted replica copies).
 */
class ClusterSanityTest {

    private static final int CLUSTER_SIZE = 3;
    private static final int TOTAL_PARTITIONS = 12;

    private static final int RING_SIZE = 1 << 16;
    private static final int BATCH_SIZE = 128;

    // Reduced to force segment rollovers and index sidecar creation.
    private static final long SEGMENT_BYTES = 256L * 1024; // 256 KiB

    private static final long REPL_TIMEOUT_MS = 10_000;

    private static final String TOPIC = "orders/created";
    private static final String GROUP = "cluster-sanity";

    private static final int PRODUCERS = 6;
    private static final int MSGS_PER_PRODUCER = 250; // total = 1500
    private static final int TOTAL_MSGS = PRODUCERS * MSGS_PER_PRODUCER;

    static final class KeyHashPartitioner implements Partitioner {
        @Override
        public int selectPartition(final byte[] key, final int totalPartitions) {
            final int h = (key == null) ? 0 : Arrays.hashCode(key);
            return Math.floorMod(h, totalPartitions);
        }
    }

    static final class InProcessForwardClient implements RemoteBrokerClient {
        private final int targetNodeId;
        private final Map<Integer, ClusteredIngress> ingressesById;
        private final AtomicInteger forwardCalls = new AtomicInteger();

        InProcessForwardClient(final int targetNodeId,
                               final Map<Integer, ClusteredIngress> ingressesById) {
            this.targetNodeId = targetNodeId;
            this.ingressesById = ingressesById;
        }

        int calls() { return forwardCalls.get(); }

        @Override public void sendMessage(final String topic, final byte[] key, final byte[] payload) {
            throw new UnsupportedOperationException("sendMessage not used");
        }
        @Override public void sendEnvelope(final BrokerApi.Envelope envelope) {
            throw new UnsupportedOperationException("sendEnvelope not used");
        }

        @Override
        public CompletableFuture<BrokerApi.ReplicationAck> sendEnvelopeWithAck(final BrokerApi.Envelope envelope) {
            forwardCalls.incrementAndGet();

            final ClusteredIngress target = ingressesById.get(targetNodeId);
            if (target == null) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Target ingress not yet registered for node " + targetNodeId));
            }
            if (!envelope.hasPublish()) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("Expected Publish envelope"));
            }

            final BrokerApi.Message m = envelope.getPublish();

            return target.publish(
                            envelope.getCorrelationId(),
                            m.getTopic(),
                            m.getKey().toByteArray(),
                            m.getRetries(),
                            m.getPayload().toByteArray()
                    )
                    .thenApply(v -> BrokerApi.ReplicationAck.newBuilder()
                            .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                            .build());
        }
    }

    static final class BlockingAckReplicationClient implements RemoteBrokerClient {
        private final CountDownLatch release;

        BlockingAckReplicationClient(final CountDownLatch release) {
            this.release = release;
        }

        @Override public void sendMessage(final String topic, final byte[] key, final byte[] payload) {
            throw new UnsupportedOperationException("sendMessage not used");
        }
        @Override public void sendEnvelope(final BrokerApi.Envelope envelope) {
            throw new UnsupportedOperationException("sendEnvelope not used");
        }

        @Override
        public CompletableFuture<BrokerApi.ReplicationAck> sendEnvelopeWithAck(final BrokerApi.Envelope envelope) {
            final CompletableFuture<BrokerApi.ReplicationAck> f = new CompletableFuture<>();
            Thread.startVirtualThread(() -> {
                try {
                    if (!release.await(REPL_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                        f.completeExceptionally(new TimeoutException("Replication ack not released in time"));
                        return;
                    }
                    f.complete(BrokerApi.ReplicationAck.newBuilder()
                            .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                            .build());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    f.completeExceptionally(ie);
                }
            });
            return f;
        }
    }

    private static int readIntLE(final DataInputStream in) throws IOException {
        final int b0 = in.readUnsignedByte();
        final int b1 = in.readUnsignedByte();
        final int b2 = in.readUnsignedByte();
        final int b3 = in.readUnsignedByte();
        return (b3 << 24) | (b2 << 16) | (b1 << 8) | b0;
    }

    private static void parseSegmentLittleEndian(final Path seg, final Set<String> outSet) throws IOException {
        try (final FileChannel channel = FileChannel.open(seg, StandardOpenOption.READ);
             final DataInputStream input = new DataInputStream(Channels.newInputStream(channel))) {

            input.skipBytes(LedgerSegment.HEADER_SIZE);

            while (true) {
                final int len;
                try {
                    len = readIntLE(input);
                } catch (final EOFException e) {
                    break;
                }
                if (len == 0) break;
                if (len < 0) break;

                final int storedCrc = readIntLE(input);
                final byte[] buffer = input.readNBytes(len);
                if (buffer.length < len) break;

                final CRC32C crc = new CRC32C();
                crc.update(buffer, 0, len);
                if ((int) crc.getValue() != storedCrc) {
                    fail("CRC mismatch in " + seg.getFileName());
                    break;
                }

                final EventsProto.OrderCreated event = EventsProto.OrderCreated.parseFrom(buffer);
                outSet.add(event.getOrderId());
            }
        }
    }

    private static Set<String> diff(final Set<String> a, final Set<String> b) {
        final Set<String> d = new TreeSet<>(a);
        d.removeAll(b);
        return d;
    }

    private static byte[] findKeyOwnedBy(final Partitioner partitioner,
                                         final int totalPartitions,
                                         final int clusterSize,
                                         final int desiredOwner,
                                         final String prefix) {
        for (int i = 0; i < 50_000; i++) {
            final String id = prefix + "-" + i;
            final byte[] key = id.getBytes(StandardCharsets.UTF_8);
            final int pid = partitioner.selectPartition(key, totalPartitions);
            final int owner = Math.floorMod(pid, clusterSize);
            if (owner == desiredOwner) return key;
        }
        throw new IllegalStateException("Could not find key for owner " + desiredOwner);
    }

    private static Path idxForSeg(final Path seg) {
        final String name = seg.getFileName().toString();
        final int dot = name.lastIndexOf('.');
        final String base = (dot >= 0) ? name.substring(0, dot) : name;
        return seg.getParent().resolve(base + ".idx");
    }

    private static Set<String> fetchAllIdsFromLedger(final Ingress ingress) {
        final Set<String> out = new HashSet<>();
        long offset = 0L;
        final int step = 1024;

        while (true) {
            final int visited = ingress.fetch(offset, step, (off, segBuf, payloadPos, payloadLen) -> {
                try {
                    final ByteBuffer bb = segBuf.duplicate();
                    bb.position(payloadPos);
                    bb.limit(payloadPos + payloadLen);

                    final EventsProto.OrderCreated ev = EventsProto.OrderCreated.parseFrom(bb);
                    out.add(ev.getOrderId());
                } catch (Exception ignore) {
                }
            });

            if (visited <= 0) break;
            offset += visited;
        }

        return out;
    }

    @Test
    void cluster_sanity_strong_e2e(@TempDir final Path tempDir) throws Exception {

        final TopicRegistry registry = new TopicRegistry.Builder()
                .topic(TOPIC, EventsProto.OrderCreated.getDescriptor())
                .build();

        final List<Member> members = List.of(
                new Member(0, BrokerRole.PERSISTENCE, new InetSocketAddress("127.0.0.1", 10_001), System.currentTimeMillis(), 16),
                new Member(1, BrokerRole.PERSISTENCE, new InetSocketAddress("127.0.0.1", 10_002), System.currentTimeMillis(), 16),
                new Member(2, BrokerRole.PERSISTENCE, new InetSocketAddress("127.0.0.1", 10_003), System.currentTimeMillis(), 16)
        );

        // ReplicationFactor = cluster size => resolver returns all members => owner replicates to both others.
        final ReplicaSetResolver resolver = new ReplicaSetResolver(CLUSTER_SIZE, () -> members);
        final Partitioner partitioner = new KeyHashPartitioner();

        final Map<Integer, ClusteredIngress> ingressesById = new ConcurrentHashMap<>();
        final Map<Integer, InMemoryOffsetStore> offsetsById = new HashMap<>();
        final Map<Integer, AdaptiveReplicator> replicatorsById = new HashMap<>();
        final Map<Integer, Path> dataDirById = new HashMap<>();
        final Map<Integer, Map<Integer, InProcessForwardClient>> forwardingClientsByNode = new HashMap<>();

        // Quorum gating: only block the owner=1 replication clients until we release them.
        final CountDownLatch repAckFrom0 = new CountDownLatch(1);
        final CountDownLatch repAckFrom2 = new CountDownLatch(1);

        for (int nodeId = 0; nodeId < CLUSTER_SIZE; nodeId++) {
            final Path nodeBase = tempDir.resolve("node-" + nodeId);
            final Path dataDir = nodeBase.resolve("data");
            final Path offsetsDir = nodeBase.resolve("offsets");
            Files.createDirectories(dataDir);
            Files.createDirectories(offsetsDir);

            dataDirById.put(nodeId, dataDir);

            final InMemoryOffsetStore offsetStore = new InMemoryOffsetStore(offsetsDir);
            offsetsById.put(nodeId, offsetStore);

            final Map<Integer, RemoteBrokerClient> clusterNodes = new HashMap<>();
            final Map<Integer, InProcessForwardClient> myForwardClients = new HashMap<>();
            for (int other = 0; other < CLUSTER_SIZE; other++) {
                if (other == nodeId) continue;
                final InProcessForwardClient c = new InProcessForwardClient(other, ingressesById);
                clusterNodes.put(other, c);
                myForwardClients.put(other, c);
            }
            forwardingClientsByNode.put(nodeId, myForwardClients);

            final Map<Integer, RemoteBrokerClient> replClients = new HashMap<>();
            for (int other = 0; other < CLUSTER_SIZE; other++) {
                if (other == nodeId) continue;

                if (nodeId == 1 && other == 0) {
                    replClients.put(other, new BlockingAckReplicationClient(repAckFrom0));
                } else if (nodeId == 1 && other == 2) {
                    replClients.put(other, new BlockingAckReplicationClient(repAckFrom2));
                } else {
                    replClients.put(other, new RemoteBrokerClient() {
                        @Override public void sendMessage(String topic, byte[] key, byte[] payload) { throw new UnsupportedOperationException(); }
                        @Override public void sendEnvelope(BrokerApi.Envelope envelope) { throw new UnsupportedOperationException(); }
                        @Override public CompletableFuture<BrokerApi.ReplicationAck> sendEnvelopeWithAck(BrokerApi.Envelope envelope) {
                            return CompletableFuture.completedFuture(
                                    BrokerApi.ReplicationAck.newBuilder()
                                            .setStatus(BrokerApi.ReplicationAck.Status.SUCCESS)
                                            .build()
                            );
                        }
                    });
                }
            }

            // ackQuorum=2: owner will wait for both replica acks.
            final AdaptiveReplicator replicator = new AdaptiveReplicator(2, replClients, REPL_TIMEOUT_MS);
            replicatorsById.put(nodeId, replicator);

            final ClusteredIngress ingress = ClusteredIngress.create(
                    registry,
                    partitioner,
                    TOTAL_PARTITIONS,
                    nodeId,
                    CLUSTER_SIZE,
                    clusterNodes,
                    dataDir,
                    RING_SIZE,
                    new AdaptiveSpin(),
                    SEGMENT_BYTES,
                    BATCH_SIZE,
                    /* idempotentMode */ false,
                    offsetStore,
                    BrokerRole.PERSISTENCE,
                    resolver,
                    replicator
            );

            ingressesById.put(nodeId, ingress);
        }

        // Expected IDs per partition
        final Map<Integer, Set<String>> expectedByPartition = new HashMap<>();
        for (int p = 0; p < TOTAL_PARTITIONS; p++) expectedByPartition.put(p, ConcurrentHashMap.newKeySet());

        // Received IDs (global): used to assert "exactly once"
        final Set<String> receivedIds = ConcurrentHashMap.newKeySet();
        final AtomicInteger duplicateDeliveries = new AtomicInteger(0);

        // Subscribe BEFORE any publishes
        final CountDownLatch delivered = new CountDownLatch(TOTAL_MSGS + 1);
        for (int nodeId = 0; nodeId < CLUSTER_SIZE; nodeId++) {
            ingressesById.get(nodeId).subscribeTopic(TOPIC, GROUP, (seq, payload) -> {
                try {
                    final EventsProto.OrderCreated ev = EventsProto.OrderCreated.parseFrom(payload);
                    if (!receivedIds.add(ev.getOrderId())) {
                        duplicateDeliveries.incrementAndGet();
                    }
                } catch (Exception ignore) {
                    // If parse fails, count it as a delivery but let the replay + expected sets catch mismatch.
                } finally {
                    delivered.countDown();
                }
            });
        }

        // ------------------------------------------------------------
        // Phase A: one gated message to prove quorum gating + forwarding
        // ------------------------------------------------------------
        final byte[] keyOwnedBy1 = findKeyOwnedBy(partitioner, TOTAL_PARTITIONS, CLUSTER_SIZE, 1, "gate");
        final String gateId = new String(keyOwnedBy1, StandardCharsets.UTF_8);
        final int gatePid = partitioner.selectPartition(keyOwnedBy1, TOTAL_PARTITIONS);
        expectedByPartition.get(gatePid).add(gateId);

        final EventsProto.OrderCreated gateEvent = EventsProto.OrderCreated.newBuilder()
                .setOrderId(gateId)
                .setCustomer("cluster-sanity")
                .setCreatedAt(Timestamp.getDefaultInstance())
                .build();

        final CompletableFuture<Void> gateFuture =
                ingressesById.get(0).publish(TOPIC, keyOwnedBy1, gateEvent.toByteArray());

        Thread.sleep(50);
        assertFalse(gateFuture.isDone(), "Publish completed before quorum acks were released (expected to block).");

        repAckFrom0.countDown();
        Thread.sleep(50);
        assertFalse(gateFuture.isDone(), "Publish completed with only 1/2 acks released (quorum=2).");

        repAckFrom2.countDown();
        assertDoesNotThrow(() -> gateFuture.get(5, TimeUnit.SECONDS), "Publish did not complete after quorum reached.");

        final InProcessForwardClient forward0to1 = forwardingClientsByNode.get(0).get(1);
        assertNotNull(forward0to1);
        assertTrue(forward0to1.calls() >= 1, "Expected at least one forward call from node0 -> node1.");

        // ------------------------------------------------------------
        // Phase B: concurrent producers publish many messages
        // ------------------------------------------------------------
        final ExecutorService pubPool = Executors.newFixedThreadPool(PRODUCERS);
        final List<CompletableFuture<Void>> pubs = new ArrayList<>(TOTAL_MSGS);

        for (int producer = 0; producer < PRODUCERS; producer++) {
            final int producerId = producer;
            pubs.add(CompletableFuture.runAsync(() -> {
                for (int j = 0; j < MSGS_PER_PRODUCER; j++) {
                    final String id = "p" + producerId + "-msg-" + j;
                    final byte[] key = id.getBytes(StandardCharsets.UTF_8);

                    final int pid = partitioner.selectPartition(key, TOTAL_PARTITIONS);
                    expectedByPartition.get(pid).add(id);

                    final EventsProto.OrderCreated ev = EventsProto.OrderCreated.newBuilder()
                            .setOrderId(id)
                            .setCustomer("cluster-sanity")
                            .setCreatedAt(Timestamp.getDefaultInstance())
                            .build();

                    // publish from a rotating node to exercise forwarding
                    final int publisherNode = (producerId + j) % CLUSTER_SIZE;
                    ingressesById.get(publisherNode).publish(TOPIC, key, ev.toByteArray()).join();
                }
            }, pubPool));
        }

        CompletableFuture.allOf(pubs.toArray(new CompletableFuture[0])).join();
        pubPool.shutdown();

        assertTrue(delivered.await(45, TimeUnit.SECONDS),
                () -> "Delivered only " + ((TOTAL_MSGS + 1) - delivered.getCount()) + "/" + (TOTAL_MSGS + 1));

        assertEquals(0, duplicateDeliveries.get(), "Duplicate deliveries detected (same orderId delivered twice).");

        // Strong delivery check: expected global ID set == received ID set
        final Set<String> expectedGlobal = new HashSet<>();
        for (int p = 0; p < TOTAL_PARTITIONS; p++) expectedGlobal.addAll(expectedByPartition.get(p));

        assertEquals(expectedGlobal, receivedIds,
                "Delivered IDs mismatch.\nMissing: " + diff(expectedGlobal, receivedIds) +
                        "\nExtra: " + diff(receivedIds, expectedGlobal));

        // -------------------------------------------------------------------
        // NEW: ledger-backed FETCH verification on owner partitions (dense .idx)
        // -------------------------------------------------------------------
        final Map<Integer, Set<String>> fetchedByPartition = new HashMap<>();
        for (int p = 0; p < TOTAL_PARTITIONS; p++) fetchedByPartition.put(p, new HashSet<>());

        for (int nodeId = 0; nodeId < CLUSTER_SIZE; nodeId++) {
            final ClusteredIngress node = ingressesById.get(nodeId);
            for (final Map.Entry<Integer, Ingress> e : node.getIngressMap().entrySet()) {
                final int partitionId = e.getKey();
                final Ingress partIngress = e.getValue();

                final Set<String> fetched = fetchAllIdsFromLedger(partIngress);
                fetchedByPartition.get(partitionId).addAll(fetched);
            }
        }

        for (int p = 0; p < TOTAL_PARTITIONS; p++) {
            final Set<String> exp = new HashSet<>(expectedByPartition.get(p));
            final Set<String> got = fetchedByPartition.get(p);

            assertEquals(exp, got, "FETCH mismatch for partition " + p + ".\n" +
                    "Missing: " + diff(exp, got) + "\nExtra: " + diff(got, exp));
        }

        // ------------------------------------------------------------
        // Shutdown
        // ------------------------------------------------------------
        for (int nodeId = 0; nodeId < CLUSTER_SIZE; nodeId++) {
            ingressesById.get(nodeId).shutdown();
        }
        for (int nodeId = 0; nodeId < CLUSTER_SIZE; nodeId++) {
            offsetsById.get(nodeId).close();
            replicatorsById.get(nodeId).shutdown();
        }

        // ------------------------------------------------------------
        // Durability replay: owner partitions match expected exactly
        // ------------------------------------------------------------
        final Map<Integer, Set<String>> seenByPartition = new HashMap<>();
        for (int p = 0; p < TOTAL_PARTITIONS; p++) seenByPartition.put(p, new HashSet<>());

        for (int p = 0; p < TOTAL_PARTITIONS; p++) {
            final int owner = Math.floorMod(p, CLUSTER_SIZE);
            final Path ownerData = dataDirById.get(owner);
            final Path partDir = ownerData.resolve("partition-" + p);

            if (!Files.isDirectory(partDir)) continue;

            try (final Stream<Path> segs = Files.list(partDir)) {
                for (final Path seg : segs.filter(f -> f.toString().endsWith(".seg")).sorted().toList()) {
                    // NEW: assert .idx exists
                    assertTrue(Files.exists(idxForSeg(seg)), "Missing .idx for segment: " + seg.getFileName());
                    parseSegmentLittleEndian(seg, seenByPartition.get(p));
                }
            }
        }

        for (int p = 0; p < TOTAL_PARTITIONS; p++) {
            final Set<String> exp = new HashSet<>(expectedByPartition.get(p));
            final Set<String> got = seenByPartition.get(p);

            assertEquals(exp, got, "Partition " + p + " mismatch.\n" +
                    "Missing: " + diff(exp, got) + "\nExtra: " + diff(got, exp));
        }
    }
}
