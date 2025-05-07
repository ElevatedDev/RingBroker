package io.ringbroker.registry;

import com.google.protobuf.Descriptors.Descriptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Registry of allowed topics and their optional Protobuf descriptors.
 */
public final class TopicRegistry {
    private final ConcurrentMap<String, Descriptor> topics;

    private TopicRegistry(final Map<String, Descriptor> topics) {
        this.topics = new ConcurrentHashMap<>(topics);
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean contains(final String topic) {
        return topics.containsKey(topic);
    }

    public Descriptor descriptor(final String topic) {
        return topics.get(topic);
    }

    public Set<String> listTopics() {
        return Collections.unmodifiableSet(topics.keySet());
    }

    /**
     * Add or replace a topic. Descriptor may be null to disable schema validation.
     */
    public void addTopic(final String topic, final Descriptor descriptor) {
        topics.put(topic, descriptor);
    }

    public void removeTopic(final String topic) {
        topics.remove(topic);
    }

    public static final class Builder {
        private final Map<String, Descriptor> map = new HashMap<>();

        public Builder topic(final String topic, final Descriptor descriptor) {
            map.put(topic, descriptor);
            return this;
        }

        public TopicRegistry build() {
            return new TopicRegistry(map);
        }
    }
}