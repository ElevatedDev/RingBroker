package io.ringbroker.cluster.type;

/**
 * Simple abstraction over the gRPC stub for forwarding
 * messages to another broker node.
 */
public interface RemoteBrokerClient {
    /**
     * Forward a message for the given topic/partition to the remote broker.
     *
     * @param topic   the topic name
     * @param key     message key bytes (maybe empty)
     * @param payload message payload bytes
     */
    void sendMessage(String topic, byte[] key, byte[] payload);
}
