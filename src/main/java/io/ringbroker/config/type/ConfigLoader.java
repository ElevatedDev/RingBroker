// src/main/java/io/ringbroker/config/ConfigLoader.java
package io.ringbroker.config.type;

import io.ringbroker.config.impl.BrokerConfig;
import io.ringbroker.config.impl.TopicConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility to load BrokerConfig and TopicConfig from YAML files.
 */
public final class ConfigLoader {

    /** Delegates to BrokerConfig.load(...) */
    public static BrokerConfig load(final String path) throws IOException {
        return BrokerConfig.load(path);
    }

    /**
     * Expects a YAML file structured as:
     *
     * topics:
     *   - name: some/topic
     *     protoClass: com.example.ProtoClass
     *   - name: other/topic
     *     protoClass: com.example.OtherProto
     */
    @SuppressWarnings("unchecked")
    public static List<TopicConfig> loadTopics(final String path) throws IOException {
        final Yaml yaml = new Yaml();
        try (final InputStream in = Files.newInputStream(Paths.get(path))) {
            final Map<String,Object> root = yaml.load(in);
            final var list = (List<Map<String,Object>>) root.get("topics");
            return list.stream()
                    .map(m -> new TopicConfig(
                            (String) m.get("name"),
                            (String) m.get("protoClass")))
                    .collect(Collectors.toList());
        }
    }
}
