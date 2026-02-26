package io.ringbroker.cluster.client;

import com.google.protobuf.ByteString;
import io.ringbroker.api.BrokerApi;

import java.util.concurrent.CompletableFuture;

/**
 * Abstraction over the broker-to-broker transport.
 */
public interface RemoteBrokerClient extends AutoCloseable {
    byte[] EMPTY_BYTES = new byte[0];

    /**
     * Legacy method still used by single-owner forwarders.
     */
    void sendMessage(String topic, byte[] key, byte[] payload);

    /**
     * Envelope path for replication. Default implementation falls back to
     * {@link #sendMessage(String, byte[], byte[])} for basic clients.
     */
    default void sendEnvelope(final BrokerApi.Envelope envelope) {
        if (envelope.hasPublish()) {
            final var m = envelope.getPublish();
            sendMessage(m.getTopic(),
                    byteStringToArrayOrNull(m.getKey()),
                    byteStringToArray(m.getPayload()));
        } else {
            throw new UnsupportedOperationException("Unsupported envelope type");
        }
    }

    private static byte[] byteStringToArray(final ByteString bytes) {
        if (bytes == null || bytes.isEmpty()) return EMPTY_BYTES;
        final int len = bytes.size();
        final byte[] out = new byte[len];
        bytes.copyTo(out, 0);
        return out;
    }

    private static byte[] byteStringToArrayOrNull(final ByteString bytes) {
        if (bytes == null || bytes.isEmpty()) return null;
        return byteStringToArray(bytes);
    }

    CompletableFuture<BrokerApi.ReplicationAck> sendEnvelopeWithAck(final BrokerApi.Envelope envelope);

    default CompletableFuture<io.ringbroker.api.BrokerApi.BackfillReply> sendBackfill(final BrokerApi.Envelope envelope) {
        final CompletableFuture<io.ringbroker.api.BrokerApi.BackfillReply> f = new CompletableFuture<>();
        f.completeExceptionally(new UnsupportedOperationException("sendBackfill not implemented"));
        return f;
    }

    @Override
    default void close() {
        // no-op
    }
}
