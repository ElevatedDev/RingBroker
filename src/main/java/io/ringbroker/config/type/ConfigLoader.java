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

public final class ConfigLoader {

    /**
     * Loads broker configuration from a YAML file by delegating to {@link BrokerConfig#load(String)}.
     *
     * @param path the path to the broker YAML configuration file
     * @return a populated {@link BrokerConfig} instance
     * @throws IOException if the file cannot be read or parsed
     */
    public static BrokerConfig load(final String path) throws IOException {
        return BrokerConfig.load(path);
    }

    /**
     * Loads a list of topic configurations from a YAML file.
     * <p>
     * The YAML file is expected to have the following structure:
     * <pre>
     * topics:
     *   - name: some/topic
     *     protoClass: com.example.ProtoClass
     *   - name: other/topic
     *     protoClass: com.example.OtherProto
     * </pre>
     *
     * @param path the path to the topics YAML configuration file
     * @throws ClassCastException if the YAML structure does not match the expected format
     */
    @SuppressWarnings("unchecked")
    public static List<TopicConfig> loadTopics(final String path) throws IOException {
        final Yaml yaml = new Yaml();

        try (final InputStream in = Files.newInputStream(Paths.get(path))) {
            final Map<String, Object> root = yaml.load(in);
            final var list = (List<Map<String, Object>>) root.get("topics");

            return list.stream()
                    .map(m -> new TopicConfig(
                            (String) m.get("name"),
                            (String) m.get("protoClass")))
                    .collect(Collectors.toList());
        }
    }
}
