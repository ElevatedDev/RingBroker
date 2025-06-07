package io.ringbroker.cluster.membership.channel;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.ringbroker.api.BrokerApi;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

/**
 * A Netty inbound handler that listens for BrokerApi.Envelope messages.
 * When an Envelope with a ReplicationAck arrives, it completes the matching
 * CompletableFuture (based on correlationId) from the provided pendingAcks map.
 */
@Slf4j
public final class ClientReplicationHandler extends SimpleChannelInboundHandler<BrokerApi.Envelope> {

    /**
     * Maps correlationId → CompletableFuture to be completed when a matching
     * ReplicationAck arrives. Once an ack is seen, the future is removed and completed.
     */
    private final ConcurrentMap<Long, CompletableFuture<BrokerApi.ReplicationAck>> pendingAcks;

    public ClientReplicationHandler(final ConcurrentMap<Long, CompletableFuture<BrokerApi.ReplicationAck>> pendingAcks) {
        this.pendingAcks = pendingAcks;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final BrokerApi.Envelope envelope) {
        if (envelope.hasReplicationAck()) {
            final long corrId = envelope.getCorrelationId();
            final BrokerApi.ReplicationAck ack = envelope.getReplicationAck();

            final CompletableFuture<BrokerApi.ReplicationAck> future = pendingAcks.remove(corrId);
            if (future != null) {
                future.complete(ack);
                log.debug("Completed future for correlationId {} with status {}", corrId, ack.getStatus());
            } else {
                log.warn("Received ReplicationAck for unknown correlationId {}. Ignoring.", corrId);
            }
        } else {
            // If other messages (unlikely) appear here, pass them along or ignore.
            ctx.fireChannelRead(envelope);
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        log.error("ClientReplicationHandler encountered exception. Completing all pending futures exceptionally.", cause);
        // Fail all pending futures so they don't hang.
        pendingAcks.forEach((corrId, future) -> future.completeExceptionally(cause));
        pendingAcks.clear();
        ctx.close();
    }
}
