package io.ringbroker.config.impl;

import lombok.Getter;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * {@code BrokerConfig} holds the configuration settings for a broker node as read from a YAML file (typically {@code broker.yaml}).
 * <p>
 * This class provides access to various broker parameters such as network ports, partitioning, cluster size, file paths,
 * threading, batching, and operational modes. It also provides a static method to load configuration values from a YAML file
 * using SnakeYAML.
 * <p>
 * Example usage:
 * <pre>
 *   BrokerConfig config = BrokerConfig.load("/path/to/broker.yaml");
 *   int port = config.getGrpcPort();
 * </pre>
 * </p>
 *
 * <b>Configuration properties:</b>
 * <ul>
 *   <li>grpcPort: gRPC server port</li>
 *   <li>topicsFile: path to topics configuration file</li>
 *   <li>totalPartitions: number of partitions in the cluster</li>
 *   <li>nodeId: unique identifier for this broker node</li>
 *   <li>clusterSize: total number of broker nodes</li>
 *   <li>ledgerPath: path to the broker's ledger storage</li>
 *   <li>ringSize: size of the ring buffer</li>
 *   <li>segmentBytes: segment file size in bytes</li>
 *   <li>ingressThreads: number of threads for ingress (if present)</li>
 *   <li>batchSize: batch size for message processing</li>
 *   <li>idempotentMode: whether idempotent mode is enabled</li>
 * </ul>
 */
@Getter
public final class BrokerConfig {

    private int grpcPort;
    private String topicsFile;
    private int totalPartitions;
    private int nodeId;
    private int clusterSize;
    private String ledgerPath;
    private int ringSize;
    private int segmentBytes;
    private int ingressThreads;
    private int batchSize;
    private boolean idempotentMode;

    /**
     * Loads broker configuration from a YAML file at the specified path.
     * <p>
     * This method parses the YAML file using SnakeYAML and populates a new {@code BrokerConfig} instance
     * with the configuration properties defined in the file. The expected properties include grpcPort,
     * topicsFile, totalPartitions, nodeId, clusterSize, ledgerPath, ringSize, segmentBytes, batchSize,
     * and idempotentMode.
     * </p>
     *
     * @param path the path to the YAML configuration file
     * @return a populated {@code BrokerConfig} instance
     * @throws IOException if the file cannot be read or parsed
     * @throws ClassCastException if a property in the YAML file does not match the expected type
     */
    public static BrokerConfig load(final String path) throws IOException {
        final Yaml yaml = new Yaml();

        try (final InputStream in = Files.newInputStream(Paths.get(path))) {
            final Map<String, Object> map = yaml.load(in);
            final BrokerConfig cfg = new BrokerConfig();

            cfg.grpcPort = (Integer) map.get("grpcPort");
            cfg.topicsFile = (String) map.get("topicsFile");
            cfg.totalPartitions = (Integer) map.get("totalPartitions");
            cfg.nodeId = (Integer) map.get("nodeId");
            cfg.clusterSize = (Integer) map.get("clusterSize");
            cfg.ledgerPath = (String) map.get("ledgerPath");
            cfg.ringSize = (Integer) map.get("ringSize");
            cfg.segmentBytes = (Integer) map.get("segmentBytes");
            cfg.batchSize = (Integer) map.get("batchSize");
            cfg.idempotentMode = (Boolean) map.get("idempotentMode");

            return cfg;
        }
    }
}
