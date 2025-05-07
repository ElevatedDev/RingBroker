package io.ringbroker.config.impl;

import lombok.Getter;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Holds broker settings as read from broker.yaml.
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

    /** Load from a YAML file at the given path. */
    @SuppressWarnings("unchecked")
    public static BrokerConfig load(final String path) throws IOException {
        final Yaml yaml = new Yaml();
        try (final InputStream in = Files.newInputStream(Paths.get(path))) {
            final Map<String,Object> map = yaml.load(in);
            final BrokerConfig cfg = new BrokerConfig();
            cfg.grpcPort = (Integer)   map.get("grpcPort");
            cfg.topicsFile = (String)    map.get("topicsFile");
            cfg.totalPartitions = (Integer)   map.get("totalPartitions");
            cfg.nodeId  = (Integer)   map.get("nodeId");
            cfg.clusterSize  = (Integer)   map.get("clusterSize");
            cfg.ledgerPath = (String)    map.get("ledgerPath");
            cfg.ringSize  = (Integer)   map.get("ringSize");
            cfg.segmentBytes = (Integer)   map.get("segmentBytes");
            cfg.ingressThreads = (Integer)   map.get("ingressThreads");
            cfg.batchSize = (Integer)   map.get("batchSize");
            cfg.idempotentMode = (Boolean)   map.get("idempotentMode");
            return cfg;
        }
    }
}
