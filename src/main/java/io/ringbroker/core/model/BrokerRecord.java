package io.ringbroker.core.model;

/**
 * A record stored in the ring buffer, preserving topic, key, and payload.
 */
public record BrokerRecord(String topic, byte[] key, byte[] payload) {
}