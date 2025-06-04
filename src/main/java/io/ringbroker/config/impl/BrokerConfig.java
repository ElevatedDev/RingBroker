// ── src/main/java/io/ringbroker/config/impl/BrokerConfig.java
package io.ringbroker.config.impl;

import io.ringbroker.broker.role.BrokerRole;
import lombok.Getter;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * Immutable config holder loaded from broker.yaml
 */
@Getter
public final class BrokerConfig {

    private int grpcPort;
    private String topicsFile;
    private int totalPartitions;
    private int nodeId;
    private int clusterSize;
    private String  ledgerPath;
    private int ringSize;
    private int segmentBytes;
    private int batchSize;
    private boolean idempotentMode;

    private BrokerRole brokerRole;        
    private List<InetSocketAddress> seedAddresses;     
    private int replicationFactor; 
    private int ackQuorum;
    private InetSocketAddress bindAddress;

    @SuppressWarnings("unchecked")
    public static BrokerConfig load(final String path) throws IOException {
        Yaml yaml = new Yaml();

        try (InputStream in = Files.newInputStream(Paths.get(path))) {
            Map<String, Object> m = yaml.load(in);
            BrokerConfig cfg = new BrokerConfig();

            cfg.grpcPort        = (Integer) m.get("grpcPort");
            cfg.topicsFile      = (String)  m.get("topicsFile");
            cfg.totalPartitions = (Integer) m.get("totalPartitions");
            cfg.nodeId          = (Integer) m.get("nodeId");
            cfg.clusterSize     = (Integer) m.get("clusterSize");
            cfg.ledgerPath      = (String)  m.get("ledgerPath");
            cfg.ringSize        = (Integer) m.get("ringSize");
            cfg.segmentBytes    = (Integer) m.get("segmentBytes");
            cfg.batchSize       = (Integer) m.get("batchSize");
            cfg.idempotentMode  = (Boolean) m.get("idempotentMode");

            /* ── NEW  ─────────────────────────── */
            cfg.brokerRole = BrokerRole.valueOf(((String) m.getOrDefault("role", "PERSISTENCE")).toUpperCase());

            Map<String,Object> bind = (Map<String, Object>) m.get("bind");
            cfg.bindAddress = new InetSocketAddress(
                    (String) bind.get("host"),
                    (Integer) bind.get("port"));

            List<Map<String,Object>> seeds = (List<Map<String, Object>>) m.get("seedNodes");
            cfg.seedAddresses = seeds.stream()
                    .map(s -> new InetSocketAddress((String) s.get("host"), (Integer) s.get("port")))
                    .toList();

            cfg.replicationFactor = (Integer) m.getOrDefault("replicationFactor", 2);
            cfg.ackQuorum         = (Integer) m.getOrDefault("ackQuorum", 2);

            return cfg;
        }
    }
}
