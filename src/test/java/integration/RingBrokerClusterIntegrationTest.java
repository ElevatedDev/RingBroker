package integration;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.core.lsn.Lsn;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
final class RingBrokerClusterIntegrationTest {

    private static final DockerImageName BASE_IMAGE = DockerImageName.parse("eclipse-temurin:21-jre")
            .asCompatibleSubstituteFor("openjdk");
    private static final Network NETWORK = Network.newNetwork();
    private static final int GRPC_PORT = 9090;
    private static final int GOSSIP_PORT = 12000;
    private static final String TOPIC = "orders/created";
    private static final String GROUP = "itest";
    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(20);
    private static final long START_LSN = Lsn.encode(0L, 0L);

    private static final List<Node> CLUSTER = List.of(
            new Node(0, "frontdoor", BrokerRole.INGESTION),
            new Node(1, "store-a", BrokerRole.PERSISTENCE),
            new Node(2, "store-b", BrokerRole.PERSISTENCE)
    );

    private GenericContainer<?> frontdoor;
    private GenericContainer<?> storeA;
    private GenericContainer<?> storeB;
    private Path topicsFile;
    private Path fatJar;

    @AfterAll
    static void cleanupNetwork() {
        NETWORK.close();
    }

    @BeforeEach
    void setUp(@TempDir final Path tempDir) throws Exception {
        fatJar = locateShadowJar();
        topicsFile = writeTopicsFile(tempDir);

        final Map<Integer, Path> configs = new HashMap<>();
        for (final Node node : CLUSTER) {
            configs.put(node.id(), writeBrokerConfig(tempDir, node));
        }

        // Start persistence nodes first so the frontdoor can connect.
        storeA = startBroker(CLUSTER.get(1), configs.get(1));
        storeB = startBroker(CLUSTER.get(2), configs.get(2));
        frontdoor = startBroker(CLUSTER.get(0), configs.get(0));

        // Give gossip a moment to converge before running assertions.
        Thread.sleep(2_000L);
    }

    @AfterEach
    void tearDown() {
        Stream.of(frontdoor, storeA, storeB)
                .filter(Objects::nonNull)
                .forEach(GenericContainer::stop);
    }

    @Test
    void frontdoorReplicatesToPersistenceAndRedirectsFetches() throws Exception {
        try (final TestBrokerClient ingressClient = client(frontdoor);
             final TestBrokerClient storeAClient = client(storeA);
             final TestBrokerClient storeBClient = client(storeB)) {

            publish(ingressClient, "m-0");
            publish(ingressClient, "m-1");
            publish(ingressClient, "m-2");

            // Partition 0 is owned by the frontdoor but must be replicated to both persistence nodes.
            assertFetch(storeAClient, 0, List.of("m-0"));
            assertFetch(storeBClient, 0, List.of("m-0"));

            // Partitions 1 and 2 live on persistence nodes.
            assertFetch(storeAClient, 1, List.of("m-1"));
            assertFetch(storeBClient, 2, List.of("m-2"));

            // Fetching from the frontdoor for a persistence-owned partition should redirect.
            assertFetchAllowRedirect(ingressClient, 1, List.of("m-1"), List.of(1, 2));

            // Fetching from the wrong persistence node should also redirect.
            assertFetchAllowRedirect(storeAClient, 2, List.of("m-2"), List.of(2));
        }
    }

    @Test
    void frontdoorServesOwnerPartitionToAllClients() throws Exception {
        try (final TestBrokerClient ingressClient = client(frontdoor);
             final TestBrokerClient storeAClient = client(storeA);
             final TestBrokerClient storeBClient = client(storeB)) {

            publish(ingressClient, "p-0");

            // Frontdoor may redirect if not in placement; replicas should hold data.
            assertFetchAllowRedirect(ingressClient, 0, List.of("p-0"), List.of(1, 2));
            assertFetch(storeAClient, 0, List.of("p-0"));
            assertFetch(storeBClient, 0, List.of("p-0"));
        }
    }

    @Test
    void replicasServeDataWhenFrontdoorIsDown() throws Exception {
        try (final TestBrokerClient ingressClient = client(frontdoor);
             final TestBrokerClient storeAClient = client(storeA);
             final TestBrokerClient storeBClient = client(storeB)) {

            publish(ingressClient, "f-0");
            publish(ingressClient, "f-1");

            // Stop the owner to simulate outage.
            frontdoor.stop();

            // Replicas should still serve the replicated owner partition (0) and their own partitions.
            assertFetch(storeAClient, 0, List.of("f-0"));
            assertFetch(storeBClient, 0, List.of("f-0"));
            assertFetch(storeAClient, 1, List.of("f-1"));
        }
    }

    private void publish(final TestBrokerClient client, final String payload) throws Exception {
        final BrokerApi.PublishReply reply = client.publish(TOPIC, payload.getBytes(StandardCharsets.UTF_8))
                .get(CLIENT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertTrue(reply.getSuccess(), () -> "publish failed: " + reply.getError());
    }

    private void assertFetch(final TestBrokerClient client,
                             final int partition,
                             final List<String> expectedPayloads) throws Exception {
        BrokerApi.FetchReply reply = null;
        List<String> payloads = null;

        for (int attempt = 0; attempt < 30; attempt++) {
            reply = client.fetch(TOPIC, GROUP, partition, START_LSN, 16)
                    .get(CLIENT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            if (reply.getStatus() == BrokerApi.FetchReply.Status.OK) {
                payloads = reply.getMessagesList().stream()
                        .map(m -> m.getPayload().toStringUtf8())
                        .collect(Collectors.toList());
                if (payloads.equals(expectedPayloads)) {
                    return;
                }
            }

            // Allow time for replication/placement to converge.
            if (reply.getStatus() == BrokerApi.FetchReply.Status.EPOCH_MISSING
                    || reply.getStatus() == BrokerApi.FetchReply.Status.NOT_IN_PLACEMENT) {
                Thread.sleep(1_000L);
                continue;
            }

            break;
        }

        assertNotNull(reply, "fetch reply is null");
        assertEquals(BrokerApi.FetchReply.Status.OK, reply.getStatus(), "unexpected fetch status");
        assertEquals(expectedPayloads, payloads, "payload mismatch for partition " + partition);

        // Try fetching beyond available messages to assert empty OK response.
        final BrokerApi.FetchReply empty = client.fetch(TOPIC, GROUP, partition, Lsn.encode(0L, 1000L), 10)
                .get(CLIENT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(BrokerApi.FetchReply.Status.OK, empty.getStatus());
        assertTrue(empty.getMessagesList().isEmpty(), "fetch beyond tail should return empty list");
    }

    private void assertFetchAllowRedirect(final TestBrokerClient client,
                                          final int partition,
                                          final List<String> expectedPayloads,
                                          final List<Integer> expectedRedirects) throws Exception {
        BrokerApi.FetchReply reply = null;
        List<String> payloads = null;

        for (int attempt = 0; attempt < 30; attempt++) {
            reply = client.fetch(TOPIC, GROUP, partition, START_LSN, 16)
                    .get(CLIENT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            if (reply.getStatus() == BrokerApi.FetchReply.Status.OK) {
                payloads = reply.getMessagesList().stream()
                        .map(m -> m.getPayload().toStringUtf8())
                        .collect(Collectors.toList());
                if (payloads.equals(expectedPayloads)) return;
            } else if (reply.getStatus() == BrokerApi.FetchReply.Status.NOT_IN_PLACEMENT) {
                if (reply.getRedirectNodesList().containsAll(expectedRedirects)) return;
            }

            Thread.sleep(1_000L);
        }

        assertNotNull(reply, "fetch reply is null");
        fail("fetch did not return expected payloads or redirect; last status=" + reply.getStatus());
    }

    private TestBrokerClient client(final GenericContainer<?> container) throws InterruptedException {
        return new TestBrokerClient(container.getHost(), container.getMappedPort(GRPC_PORT));
    }

    private GenericContainer<?> startBroker(final Node node, final Path configPath) {
        final GenericContainer<?> c = new GenericContainer<>(BASE_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases(node.alias())
                .withExposedPorts(GRPC_PORT)
                .withCopyFileToContainer(MountableFile.forHostPath(fatJar), "/opt/ringbroker/app/ringbroker.jar")
                .withCopyFileToContainer(MountableFile.forHostPath(configPath), "/opt/ringbroker/conf/broker.yaml")
                .withCopyFileToContainer(MountableFile.forHostPath(topicsFile), "/opt/ringbroker/conf/topics.yaml")
                .waitingFor(Wait.forLogMessage(".*RingBroker started on gRPC port.*", 1))
                // Delay startup slightly so peers have time to start their listeners.
                .withCommand("sh", "-c", "sleep 2 && java -jar /opt/ringbroker/app/ringbroker.jar /opt/ringbroker/conf/broker.yaml");

        c.start();
        return c;
    }

    private Path writeTopicsFile(final Path dir) throws IOException {
        final Path path = dir.resolve("topics.yaml");
        final String yaml = """
                topics:
                  - name: orders/created
                    protoClass: io.ringbroker.proto.test.EventsProto$OrderCreated
                """;
        Files.writeString(path, yaml);
        return path;
    }

    private Path writeBrokerConfig(final Path dir, final Node node) throws IOException {
        final Path path = dir.resolve("broker-" + node.id() + ".yaml");

        final StringBuilder yaml = new StringBuilder()
                .append("grpcPort: ").append(GRPC_PORT).append("\n")
                .append("ringSize: ").append(1 << 16).append("\n")
                .append("ledgerPath: /data/ledger\n")
                .append("segmentBytes: ").append(1 << 20).append("\n")
                .append("topicsFile: /opt/ringbroker/conf/topics.yaml\n")
                .append("totalPartitions: 3\n")
                .append("nodeId: ").append(node.id()).append("\n")
                .append("clusterSize: ").append(CLUSTER.size()).append("\n")
                .append("batchSize: 16\n")
                .append("replicationTimeoutMillis: 5000\n")
                .append("idempotentMode: false\n")
                .append("replicationFactor: 2\n")
                .append("ackQuorum: 2\n")
                .append("role: ").append(node.role().name()).append("\n")
                .append("bind:\n")
                .append("  host: ").append(node.alias()).append("\n")
                .append("  port: ").append(GOSSIP_PORT).append("\n")
                .append("seedNodes:\n");

        for (final Node n : CLUSTER) {
            yaml.append("  - host: ").append(n.alias()).append("\n")
                    .append("    port: ").append(GOSSIP_PORT).append("\n");
        }

        yaml.append("clusterNodes:\n");
        for (final Node n : CLUSTER) {
            yaml.append("  - id: ").append(n.id()).append("\n")
                    .append("    host: ").append(n.alias()).append("\n")
                    .append("    port: ").append(GRPC_PORT).append("\n")
                    .append("    role: ").append(n.role().name()).append("\n");
        }

        Files.writeString(path, yaml.toString());
        return path;
    }

    private Path locateShadowJar() throws IOException {
        final Path libs = Path.of("build", "libs");
        if (!Files.isDirectory(libs)) {
            throw new IOException("Shadow jar not found: build/libs does not exist");
        }

        try (final Stream<Path> stream = Files.list(libs)) {
            return stream
                    .filter(p -> p.getFileName().toString().endsWith("-all.jar"))
                    .findFirst()
                    .orElseThrow(() -> new IOException("Shadow jar (-all.jar) not found under build/libs"));
        }
    }

    private record Node(int id, String alias, BrokerRole role) {
    }

    private static final class TestBrokerClient implements AutoCloseable {
        private final EventLoopGroup group;
        private final Channel channel;
        private final ConcurrentMap<Long, CompletableFuture<BrokerApi.Envelope>> inflight = new ConcurrentHashMap<>();
        private final AtomicLong corr = new AtomicLong(1L);

        TestBrokerClient(final String host, final int port) throws InterruptedException {
            this.group = new NioEventLoopGroup(1);

            final Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(final SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new ProtobufVarint32FrameDecoder())
                                    .addLast(new ProtobufDecoder(BrokerApi.Envelope.getDefaultInstance()))
                                    .addLast(new InboundHandler())
                                    .addLast(new ProtobufVarint32LengthFieldPrepender())
                                    .addLast(new ProtobufEncoder());
                        }
                    });

            this.channel = bootstrap.connect(host, port).sync().channel();
        }

        CompletableFuture<BrokerApi.PublishReply> publish(final String topic, final byte[] payload) {
            final BrokerApi.Envelope env = BrokerApi.Envelope.newBuilder()
                    .setPublish(BrokerApi.Message.newBuilder()
                            .setTopic(topic)
                            .setPayload(com.google.protobuf.UnsafeByteOperations.unsafeWrap(payload))
                            .build())
                    .build();
            return send(env).thenApply(BrokerApi.Envelope::getPublishReply);
        }

        CompletableFuture<BrokerApi.FetchReply> fetch(final String topic,
                                                      final String group,
                                                      final int partition,
                                                      final long offset,
                                                      final int maxMessages) {
            final BrokerApi.Envelope env = BrokerApi.Envelope.newBuilder()
                    .setFetch(BrokerApi.FetchRequest.newBuilder()
                            .setTopic(topic)
                            .setGroup(group)
                            .setPartition(partition)
                            .setOffset(offset)
                            .setMaxMessages(maxMessages)
                            .build())
                    .build();
            return send(env).thenApply(BrokerApi.Envelope::getFetchReply);
        }

        private CompletableFuture<BrokerApi.Envelope> send(final BrokerApi.Envelope envelope) {
            final long id = corr.getAndIncrement();
            final BrokerApi.Envelope toSend = BrokerApi.Envelope.newBuilder(envelope)
                    .setCorrelationId(id)
                    .build();

            final CompletableFuture<BrokerApi.Envelope> future = new CompletableFuture<>();
            inflight.put(id, future);

            channel.writeAndFlush(toSend).addListener(f -> {
                if (!f.isSuccess()) {
                    future.completeExceptionally(f.cause());
                }
            });

            return future.orTimeout(CLIENT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .whenComplete((__, __2) -> inflight.remove(id));
        }

        @Override
        public void close() {
            inflight.forEach((id, fut) -> fut.completeExceptionally(new ClosedChannelException()));
            inflight.clear();
            try {
                if (channel != null) channel.close().syncUninterruptibly();
            } finally {
                if (group != null) group.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
            }
        }

        private final class InboundHandler extends SimpleChannelInboundHandler<BrokerApi.Envelope> {
            @Override
            protected void channelRead0(final ChannelHandlerContext ctx, final BrokerApi.Envelope msg) {
                final CompletableFuture<BrokerApi.Envelope> fut = inflight.remove(msg.getCorrelationId());
                if (fut != null) {
                    fut.complete(msg);
                }
            }

            @Override
            public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
                inflight.forEach((id, f) -> f.completeExceptionally(cause));
                inflight.clear();
                ctx.close();
            }
        }
    }
}
