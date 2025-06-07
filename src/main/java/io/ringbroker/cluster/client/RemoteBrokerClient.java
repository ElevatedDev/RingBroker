package io.ringbroker.cluster.client;

import io.ringbroker.api.BrokerApi;

import java.util.concurrent.CompletableFuture;

/**
 * Abstraction over the broker-to-broker transport.
 */
public interface RemoteBrokerClient {

    /**
     * Legacy method — still used by classic single-owner forwarders.
     */
    void sendMessage(String topic, byte[] key, byte[] payload);

    /**
     * NEW: zero-copy path for replication.  Default impl falls back to
     * {@link #sendMessage(String, byte[], byte[])} if you only have a
     * basic client implementation.
     */
    default void sendEnvelope(final BrokerApi.Envelope envelope) {
        if (envelope.hasPublish()) {
            final var m = envelope.getPublish();
            sendMessage(m.getTopic(),
                    m.getKey().isEmpty() ? null : m.getKey().toByteArray(),
                    m.getPayload().toByteArray());
        } else {
            throw new UnsupportedOperationException("Unsupported envelope type");
        }
    }


    CompletableFuture<BrokerApi.ReplicationAck> sendEnvelopeWithAck(final BrokerApi.Envelope envelope);
}
