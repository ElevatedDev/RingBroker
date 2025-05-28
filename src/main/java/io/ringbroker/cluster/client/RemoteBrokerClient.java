package io.ringbroker.cluster.client;

/**
 * The {@code RemoteBrokerClient} interface provides an abstraction over the gRPC stub for forwarding
 * messages to another broker node in a distributed messaging system. Implementations of this interface
 * are responsible for transmitting messages to remote brokers, typically as part of a cluster or sharded
 * deployment.
 * <p>
 * This interface is intended to decouple the message forwarding logic from the underlying network or RPC
 * implementation, allowing for easier testing and extensibility.
 * </p>
 */
public interface RemoteBrokerClient {
    /**
     * Forwards a message for the given topic and partition to a remote broker node.
     *
     * @param topic   the topic name
     * @param key     message key bytes (may be empty or null)
     * @param payload message payload bytes
     */
    void sendMessage(String topic, byte[] key, byte[] payload);
}
