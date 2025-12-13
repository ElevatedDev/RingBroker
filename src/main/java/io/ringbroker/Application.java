package io.ringbroker;

import com.google.protobuf.Descriptors.Descriptor;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.client.impl.NettyClusterClient;
import io.ringbroker.cluster.membership.gossip.impl.SwimGossipService;
import io.ringbroker.cluster.membership.member.Member;
import io.ringbroker.cluster.membership.replicator.AdaptiveReplicator;
import io.ringbroker.cluster.membership.resolver.ReplicaSetResolver;
import io.ringbroker.cluster.metadata.BroadcastingLogMetadataStore;
import io.ringbroker.cluster.metadata.JournaledLogMetadataStore;
import io.ringbroker.cluster.metadata.LogMetadataStore;
import io.ringbroker.cluster.partitioner.Partitioner;
import io.ringbroker.cluster.partitioner.impl.RoundRobinPartitioner;
import io.ringbroker.config.impl.BrokerConfig;
import io.ringbroker.config.impl.TopicConfig;
import io.ringbroker.config.type.ConfigLoader;
import io.ringbroker.core.wait.AdaptiveSpin;
import io.ringbroker.offset.InMemoryOffsetStore;
import io.ringbroker.registry.TopicRegistry;
import io.ringbroker.transport.type.NettyTransport;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Main class to start the RingBroker application.
 */
@Slf4j
public class Application {
    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java -jar broker.jar <broker-config.yaml>");
            System.exit(1);
        }

        /* Load broker settings and topic definitions */
        final BrokerConfig cfg = ConfigLoader.load(args[0]);
        final List<TopicConfig> topics = ConfigLoader.loadTopics(cfg.getTopicsFile());

        /* Prepare ledger directory */
        final Path dataDir = Paths.get(cfg.getLedgerPath()).toAbsolutePath();
        final Path offsetsDir = dataDir.resolve("offsets");

        final boolean wipeOnStart = Boolean.parseBoolean(
                System.getProperty("ringbroker.wipeDataOnStart",
                        System.getenv().getOrDefault("RINGBROKER_WIPE_DATA_ON_START", "false"))
        );

        if (wipeOnStart && Files.exists(dataDir)) {
            log.warn("ringbroker.wipeDataOnStart=true -> wiping ledger directory at {}", dataDir);
            try (final Stream<Path> stream = Files.walk(dataDir)) {
                stream.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(file -> {
                            if (!file.delete()) {
                                log.warn("Failed to delete file: {}", file.getAbsolutePath());
                            }
                        });
            }
        } else if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        } else {
            log.info("Reusing existing ledger directory at {}", dataDir);
        }

        Files.createDirectories(offsetsDir);

        /* Build topic registry via reflection */
        final TopicRegistry.Builder registryBuilder = new TopicRegistry.Builder();
        for (final TopicConfig t : topics) {
            final Class<?> protoClass = Class.forName(t.getProtoClass());
            final Method descriptorMethod = protoClass.getMethod("getDescriptor");
            final Descriptor descriptor = (Descriptor) descriptorMethod.invoke(null);
            registryBuilder.topic(t.getName(), descriptor);
        }
        final TopicRegistry registry = registryBuilder.build();

        /* Partitioner and cluster node map */
        final Partitioner partitioner = new RoundRobinPartitioner();
        final Map<Integer, RemoteBrokerClient> clusterNodes = new HashMap<>();
        final Map<Integer, Member> staticMembers = new HashMap<>();

        /* Load cluster node addresses from broker.yaml under "clusterNodes" */
        final Yaml yaml = new Yaml();

        try (final InputStream in = Files.newInputStream(Paths.get(args[0]))) {
            final Map<String, Object> root = yaml.load(in);
            final List<Map<String, Object>> nodesCfg = (List<Map<String, Object>>) root.get("clusterNodes");
            if (nodesCfg != null) {
                for (final Map<String, Object> node : nodesCfg) {
                    final Integer nid = (Integer) node.get("id");
                    if (!nid.equals(cfg.getNodeId())) {
                        final String host = (String) node.get("host");
                        final Integer port = (Integer) node.get("port");
                        final BrokerRole role = node.containsKey("role")
                                ? BrokerRole.valueOf(((String) node.get("role")).toUpperCase())
                                : BrokerRole.PERSISTENCE;
                        clusterNodes.put(nid, new LazyRemoteBrokerClient(host, port));
                        staticMembers.put(nid, new Member(
                                nid,
                                role,
                                new InetSocketAddress(host, cfg.getBindAddress().getPort()),
                                System.currentTimeMillis(),
                                16));
                    }
                }
            }
        }
        staticMembers.putIfAbsent(cfg.getNodeId(), new Member(
                cfg.getNodeId(),
                cfg.getBrokerRole(),
                cfg.getBindAddress(),
                System.currentTimeMillis(),
                16));

        /*
         * FIX: Offset store is now durable and requires a storage path.
         * We use a dedicated sub-directory to separate it from partition segments.
         */
        final InMemoryOffsetStore store = new InMemoryOffsetStore(offsetsDir);

        final var gossip = new SwimGossipService(
                cfg.getNodeId(),
                cfg.getBrokerRole(),
                cfg.getBindAddress(),
                cfg.getSeedAddresses());
        gossip.start();
        awaitMembership(gossip, cfg.getClusterSize(), Duration.ofSeconds(10));

        final ReplicaSetResolver resolver = new ReplicaSetResolver(
                cfg.getReplicationFactor(),
                () -> {
                    final Map<Integer, Member> merged = new HashMap<>(staticMembers);
                    merged.putAll(gossip.view());
                    return merged.values();
                });

        final AdaptiveReplicator replicator = new AdaptiveReplicator(
                cfg.getAckQuorum(),
                clusterNodes,
                cfg.getReplicationTimeoutMillis());

        final LogMetadataStore metadataStore = new BroadcastingLogMetadataStore(
                new JournaledLogMetadataStore(dataDir.resolve("metadata")),
                clusterNodes,
                cfg.getNodeId(),
                () -> gossip.view().keySet()
        );

        /* Create the clustered ingress */
        final ClusteredIngress ingress = ClusteredIngress.create(
                registry,
                partitioner,
                cfg.getTotalPartitions(),
                cfg.getNodeId(),
                cfg.getClusterSize(),
                clusterNodes,
                dataDir,
                cfg.getRingSize(),
                new AdaptiveSpin(),
                cfg.getSegmentBytes(),
                cfg.getBatchSize(),
                cfg.isIdempotentMode(),
                store,
                cfg.getBrokerRole(),
                resolver,
                replicator,
                metadataStore
        );

        /* Start gRPC transport */
        final NettyTransport transport = new NettyTransport(
                cfg.getGrpcPort(),
                ingress,
                store
        );
        transport.start();

        /* Ensure graceful shutdown to flush mmap logs */
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                log.info("Shutting down RingBroker...");
                transport.stop();
                ingress.shutdown();
                store.close();
                gossip.close();
                log.info("Shutdown complete.");
            } catch (final Exception e) {
                log.error("Error during shutdown", e);
            }
        }));

        log.info("RingBroker started on gRPC port {}", cfg.getGrpcPort());
    }

    private static void awaitMembership(final SwimGossipService gossip,
                                        final int expected,
                                        final Duration timeout) throws InterruptedException {
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (gossip.view().size() >= expected) return;
            Thread.sleep(200L);
        }
        log.warn("Gossip view size {} did not reach expected {} before timeout {}", gossip.view().size(), expected, timeout);
    }

    private static RemoteBrokerClient connectWithRetry(final String host, final int port) throws InterruptedException {
        // Allow slower peer bootstrap (e.g., when containers start in sequence).
        final Duration backoff = Duration.ofMillis(1_000);
        final int maxAttempts = 120;
        Exception lastError = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return new NettyClusterClient(host, port);
            } catch (final Exception e) {
                lastError = e;
                log.warn("Failed to connect to broker {}:{} (attempt {}/{}): {}", host, port, attempt, maxAttempts, e.getMessage());
                Thread.sleep(backoff.toMillis());
            }
        }

        throw new IllegalStateException("Unable to connect to broker " + host + ":" + port + " after " + maxAttempts + " attempts", lastError);
    }

    /**
     * Lazy connector that defers dialing peers until first use.
     * Prevents startup from failing when peers are not yet running (e.g., in Testcontainers).
     */
    private static final class LazyRemoteBrokerClient implements RemoteBrokerClient {
        private final String host;
        private final int port;
        private volatile RemoteBrokerClient delegate;

        LazyRemoteBrokerClient(final String host, final int port) {
            this.host = host;
            this.port = port;
        }

        private RemoteBrokerClient ensureConnected() {
            RemoteBrokerClient c = delegate;
            if (c != null) return c;

            synchronized (this) {
                c = delegate;
                if (c == null) {
                    try {
                        delegate = connectWithRetry(host, port);
                    } catch (final InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while connecting to peer " + host + ":" + port, ie);
                    }
                    c = delegate;
                }
            }
            return c;
        }

        @Override
        public void sendMessage(final String topic, final byte[] key, final byte[] payload) {
            ensureConnected().sendMessage(topic, key, payload);
        }

        @Override
        public void sendEnvelope(final BrokerApi.Envelope envelope) {
            ensureConnected().sendEnvelope(envelope);
        }

        @Override
        public CompletableFuture<BrokerApi.ReplicationAck> sendEnvelopeWithAck(final BrokerApi.Envelope envelope) {
            return ensureConnected().sendEnvelopeWithAck(envelope);
        }

        @Override
        public CompletableFuture<BrokerApi.BackfillReply> sendBackfill(final BrokerApi.Envelope envelope) {
            return ensureConnected().sendBackfill(envelope);
        }

        @Override
        public void close() {
            final RemoteBrokerClient c = delegate;
            if (c != null) c.close();
        }
    }
}
