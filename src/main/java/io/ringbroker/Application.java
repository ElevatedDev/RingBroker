package io.ringbroker;

import com.google.protobuf.Descriptors.Descriptor;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.client.impl.NettyClusterClient;
import io.ringbroker.cluster.membership.gossip.impl.SwimGossipService;
import io.ringbroker.cluster.membership.replicator.FlashReplicator;
import io.ringbroker.cluster.membership.resolver.ReplicaSetResolver;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

        // Load broker settings and topic definitions
        final BrokerConfig cfg = ConfigLoader.load(args[0]);
        final List<TopicConfig> topics = ConfigLoader.loadTopics(cfg.getTopicsFile());

        // Prepare ledger directory
        final Path dataDir = Paths.get(cfg.getLedgerPath());

        if (Files.exists(dataDir)) {
            try (final Stream<Path> stream = Files.walk(dataDir)) {
                stream
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(file -> {
                            if (!file.delete()) {
                                log.warn("Failed to delete file: {}", file.getAbsolutePath());
                            }
                        });
            }
        }

        Files.createDirectories(dataDir);

        // Build topic registry via reflection
        final TopicRegistry.Builder registryBuilder = new TopicRegistry.Builder();
        for (final TopicConfig t : topics) {
            final Class<?> protoClass = Class.forName(t.getProtoClass());
            final Method descriptorMethod = protoClass.getMethod("getDescriptor");
            final Descriptor descriptor = (Descriptor) descriptorMethod.invoke(null);
            registryBuilder.topic(t.getName(), descriptor);
        }
        final TopicRegistry registry = registryBuilder.build();

        // Partitioner and cluster node map
        final Partitioner partitioner = new RoundRobinPartitioner();
        final Map<Integer, RemoteBrokerClient> clusterNodes = new HashMap<>();

        // Load cluster node addresses from broker.yaml under "clusterNodes"
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
                        clusterNodes.put(nid, new NettyClusterClient(host, port));
                    }
                }
            }
        }

        // Offset store
        final InMemoryOffsetStore store = new InMemoryOffsetStore();

        final var gossip = new SwimGossipService(
                cfg.getNodeId(),
                cfg.getBrokerRole(),
                cfg.getBindAddress(),
                cfg.getSeedAddresses());
        gossip.start();

        final ReplicaSetResolver resolver = new ReplicaSetResolver(
                cfg.getReplicationFactor(),
                () -> gossip.view().values());

        final FlashReplicator replicator = new FlashReplicator(
                cfg.getAckQuorum(),
                clusterNodes,
                cfg.getReplicationTimeoutMillis());

        // Create the clustered ingress
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
                replicator
        );

        // Start gRPC transport
        final NettyTransport transport = new NettyTransport(
                cfg.getGrpcPort(),
                ingress,
                store
        );
        transport.start();

        log.info("RingBroker started on gRPC port {}", cfg.getGrpcPort());
    }
}
