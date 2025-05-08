package io.ringbroker.config.impl;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Represents one entry in the topics.yaml list.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public final class TopicConfig {
    private String name;
    private String protoClass;
}
