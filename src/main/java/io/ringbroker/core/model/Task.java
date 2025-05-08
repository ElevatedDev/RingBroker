package io.ringbroker.core.model;

/**
 * A task representing a message to be persisted and then published.
 */
public record Task(String topic, byte[] key, byte[] payload) {
}