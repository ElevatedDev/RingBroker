package io.ringbroker.cluster.membership.channel;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.ringbroker.api.BrokerApi;
import lombok.extern.slf4j.Slf4j;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

/**
 * A Netty inbound handler that listens for BrokerApi.Envelope messages.
 * When an Envelope with a ReplicationAck arrives, it completes the matching
 * CompletableFuture (based on correlationId) from the provided pendingAcks map.
 *
 * FIX: Server replies to PUBLISH/BATCH with PublishReply, while the client was
 * waiting for ReplicationAck. We now also adapt PublishReply -> ReplicationAck
 * so sendEnvelopeWithAck() completes correctly.
 */
@Slf4j
public final class ClientReplicationHandler extends SimpleChannelInboundHandler<BrokerApi.Envelope> {

    /**
     * Maps correlationId → CompletableFuture to be completed when a matching
     * ack arrives. Once an ack is seen, the future is removed and completed.
     */
    private final ConcurrentMap<Long, CompletableFuture<BrokerApi.ReplicationAck>> pendingAcks;

    public ClientReplicationHandler(final ConcurrentMap<Long, CompletableFuture<BrokerApi.ReplicationAck>> pendingAcks) {
        this.pendingAcks = pendingAcks;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final BrokerApi.Envelope envelope) {
        final long corrId = envelope.getCorrelationId();

        // 1) Native replication ack (ideal path)
        if (envelope.hasReplicationAck()) {
            final BrokerApi.ReplicationAck ack = envelope.getReplicationAck();

            final CompletableFuture<BrokerApi.ReplicationAck> future = pendingAcks.remove(corrId);
            if (future != null) {
                future.complete(ack);
                log.debug("Completed future for correlationId {} with ReplicationAck status={}", corrId, ack.getStatus());
            } else {
                log.warn("Received ReplicationAck for unknown correlationId {}. Ignoring.", corrId);
            }
            return;
        }

        // 2) Compatibility path: server replies to PUBLISH/BATCH with PublishReply.
        if (envelope.hasPublishReply()) {
            final BrokerApi.PublishReply pr = envelope.getPublishReply();

            final CompletableFuture<BrokerApi.ReplicationAck> future = pendingAcks.remove(corrId);
            if (future != null) {
                // Adjust FAILURE to match your actual enum values if needed.
                final BrokerApi.ReplicationAck.Status status =
                        pr.getSuccess()
                                ? BrokerApi.ReplicationAck.Status.SUCCESS
                                : BrokerApi.ReplicationAck.Status.ERROR_PERSISTENCE_FAILED;

                future.complete(BrokerApi.ReplicationAck.newBuilder()
                        .setStatus(status)
                        .build());

                log.debug("Completed future for correlationId {} via PublishReply(success={})", corrId, pr.getSuccess());
            } else {
                log.warn("Received PublishReply for unknown correlationId {}. Ignoring.", corrId);
            }
            return;
        }

        // 3) Not an ack envelope, pass downstream
        ctx.fireChannelRead(envelope);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        if (!pendingAcks.isEmpty()) {
            log.warn("Channel inactive. Failing {} pending replication futures.", pendingAcks.size());
            final ClosedChannelException ex = new ClosedChannelException();
            pendingAcks.forEach((id, future) -> future.completeExceptionally(ex));
            pendingAcks.clear();
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        log.error("ClientReplicationHandler encountered exception. Completing all pending futures exceptionally.", cause);
        pendingAcks.forEach((id, future) -> future.completeExceptionally(cause));
        pendingAcks.clear();
        ctx.close();
    }
}
