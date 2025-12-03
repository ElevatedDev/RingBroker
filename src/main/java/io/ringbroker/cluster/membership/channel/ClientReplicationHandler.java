package io.ringbroker.cluster.membership.channel;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.ringbroker.api.BrokerApi;
import lombok.extern.slf4j.Slf4j;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

/**
 * Completes pending replication and backfill futures keyed by correlationId.
 */
@Slf4j
public final class ClientReplicationHandler extends SimpleChannelInboundHandler<BrokerApi.Envelope> {

    private final ConcurrentMap<Long, CompletableFuture<BrokerApi.ReplicationAck>> pendingAcks;
    private final ConcurrentMap<Long, CompletableFuture<BrokerApi.BackfillReply>> pendingBackfill;

    public ClientReplicationHandler(final ConcurrentMap<Long, CompletableFuture<BrokerApi.ReplicationAck>> pendingAcks,
                                    final ConcurrentMap<Long, CompletableFuture<BrokerApi.BackfillReply>> pendingBackfill) {
        this.pendingAcks = pendingAcks;
        this.pendingBackfill = pendingBackfill;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final BrokerApi.Envelope envelope) {
        final long corrId = envelope.getCorrelationId();

        if (envelope.hasReplicationAck()) {
            final CompletableFuture<BrokerApi.ReplicationAck> fut = pendingAcks.remove(corrId);
            if (fut != null) {
                fut.complete(envelope.getReplicationAck());
            } else {
                log.warn("ReplicationAck for unknown corrId {}", corrId);
            }
            return;
        }

        if (envelope.hasPublishReply()) {
            final CompletableFuture<BrokerApi.ReplicationAck> fut = pendingAcks.remove(corrId);
            if (fut != null) {
                final BrokerApi.ReplicationAck.Status status =
                        envelope.getPublishReply().getSuccess()
                                ? BrokerApi.ReplicationAck.Status.SUCCESS
                                : BrokerApi.ReplicationAck.Status.ERROR_PERSISTENCE_FAILED;
                fut.complete(BrokerApi.ReplicationAck.newBuilder().setStatus(status).build());
            } else {
                log.warn("PublishReply for unknown corrId {}", corrId);
            }
            return;
        }

        if (envelope.hasBackfillReply()) {
            final CompletableFuture<BrokerApi.BackfillReply> fut = pendingBackfill.remove(corrId);
            if (fut != null) {
                fut.complete(envelope.getBackfillReply());
            } else {
                log.warn("BackfillReply for unknown corrId {}", corrId);
            }
            return;
        }

        ctx.fireChannelRead(envelope);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        if (!pendingAcks.isEmpty() || !pendingBackfill.isEmpty()) {
            final ClosedChannelException ex = new ClosedChannelException();
            pendingAcks.forEach((id, f) -> f.completeExceptionally(ex));
            pendingBackfill.forEach((id, f) -> f.completeExceptionally(ex));
            pendingAcks.clear();
            pendingBackfill.clear();
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        pendingAcks.forEach((id, f) -> f.completeExceptionally(cause));
        pendingBackfill.forEach((id, f) -> f.completeExceptionally(cause));
        pendingAcks.clear();
        pendingBackfill.clear();
        ctx.close();
    }
}
