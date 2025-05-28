package io.ringbroker.config.impl;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Represents a single topic configuration entry as defined in the topics.yaml file.
 * <p>
 * Each {@code TopicConfig} instance holds the topic's name and the fully qualified name of the
 * associated Protocol Buffers message class used for serialization and deserialization.
 * <p>
 * This class is typically used for loading and managing topic definitions in the broker's configuration.
 * </p>
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public final class TopicConfig {
    private String name;
    private String protoClass;
}
