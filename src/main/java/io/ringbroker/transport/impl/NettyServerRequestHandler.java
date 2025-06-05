package io.ringbroker.transport.impl;

import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.offset.OffsetStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Handles incoming client requests from the Netty pipeline, dispatching them
 * to the appropriate {@link ClusteredIngress} or {@link OffsetStore} methods.
 */
@Slf4j
@RequiredArgsConstructor
public class NettyServerRequestHandler
        extends SimpleChannelInboundHandler<BrokerApi.Envelope> {

    private final ClusteredIngress ingress;
    private final OffsetStore offsetStore;

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final BrokerApi.Envelope env) {
        final long corrId = env.getCorrelationId();

        try {
            switch (env.getKindCase()) {
                case PUBLISH -> {
                    final var m = env.getPublish();
                    /** Pass correlationId to ClusteredIngress.publish */
                    ingress.publish(corrId, m.getTopic(), m.getKey().toByteArray(), m.getRetries(), m.getPayload().toByteArray());

                    ctx.write(
                            BrokerApi.Envelope.newBuilder()
                                    .setCorrelationId(corrId)
                                    .setPublishReply(
                                            BrokerApi.PublishReply.newBuilder().setSuccess(true)
                                    )
                                    .build()
                    );
                }

                case BATCH -> {
                    for (final var m : env.getBatch().getMessagesList()) {
                        /** Pass correlationId for each message in the batch. */
                        ingress.publish(corrId, m.getTopic(), m.getKey().toByteArray(), m.getRetries(), m.getPayload().toByteArray());
                    }
                    ctx.write(
                            BrokerApi.Envelope.newBuilder()
                                    .setCorrelationId(corrId)
                                    .setPublishReply( // Single reply for the batch operation
                                            BrokerApi.PublishReply.newBuilder().setSuccess(true)
                                    )
                                    .build()
                    );
                }

                case COMMIT -> {
                    final var req = env.getCommit();
                    offsetStore.commit(req.getTopic(), req.getGroup(), req.getPartition(), req.getOffset());
                    ctx.write(
                            BrokerApi.Envelope.newBuilder()
                                    .setCorrelationId(corrId)
                                    .setCommitAck(
                                            BrokerApi.CommitAck.newBuilder().setSuccess(true)
                                    )
                                    .build()
                    );
                }

                case COMMITTED -> {
                    final var r = env.getCommitted();
                    final long off = offsetStore.fetch(r.getTopic(), r.getGroup(), r.getPartition());
                    ctx.write(
                            BrokerApi.Envelope.newBuilder()
                                    .setCorrelationId(corrId)
                                    .setCommittedReply(
                                            BrokerApi.CommittedReply.newBuilder().setOffset(off)
                                    )
                                    .build()
                    );
                }

                case FETCH -> {
                    final var f = env.getFetch();
                    final var delivery = ingress.getDeliveryMap().get(f.getPartition());

                    if (delivery == null || delivery.getRing() == null) {
                        log.warn("Fetch request for partition {} (corrId: {}) failed: No delivery or ring buffer found.",
                                f.getPartition(), corrId);
                        final BrokerApi.FetchReply.Builder errorReply = BrokerApi.FetchReply.newBuilder(); // Empty
                        // TODO: Consider adding an error field to FetchReply in proto for richer error reporting.
                        ctx.write(
                                BrokerApi.Envelope.newBuilder()
                                        .setCorrelationId(corrId)
                                        .setFetchReply(errorReply)
                                        .build()
                        );
                        break;
                    }

                    final var ring = delivery.getRing();
                    final BrokerApi.FetchReply.Builder fr = BrokerApi.FetchReply.newBuilder();
                    long offset = f.getOffset();
                    final long maxAvailableOffset = ring.getCursor(); // Highest available sequence in the ring

                    for (int i = 0; i < f.getMaxMessages() && offset <= maxAvailableOffset; i++, offset++) {
                        final byte[] payload = ring.get(offset);
                        if (payload == null) { // Defensive check
                            log.warn("Payload at offset {} for partition {} (corrId: {}) was null. Skipping fetch for this offset.",
                                    offset, f.getPartition(), corrId);
                            continue;
                        }
                        fr.addMessages(
                                BrokerApi.MessageEvent.newBuilder()
                                        .setTopic(f.getTopic())
                                        .setOffset(offset)
                                        // Key is not currently stored in RingBuffer<byte[]>; if needed, RingBuffer structure must change.
                                        .setKey(ByteString.EMPTY)
                                        .setPayload(ByteString.copyFrom(payload))
                        );
                    }
                    ctx.write(
                            BrokerApi.Envelope.newBuilder()
                                    .setCorrelationId(corrId)
                                    .setFetchReply(fr)
                                    .build()
                    );
                }

                case SUBSCRIBE -> {
                    final var s = env.getSubscribe();
                    ingress.subscribeTopic(
                            s.getTopic(),
                            s.getGroup(),
                            (seq, msg) -> { // BiConsumer callback for subscribed messages
                                if (ctx.channel().isActive()) {
                                    ctx.writeAndFlush( // Use writeAndFlush for streaming messages
                                            BrokerApi.Envelope.newBuilder()
                                                    // No correlationId for pushed MessageEvents is standard.
                                                    .setMessageEvent(
                                                            BrokerApi.MessageEvent.newBuilder()
                                                                    .setTopic(s.getTopic())
                                                                    .setOffset(seq)
                                                                    .setKey(ByteString.EMPTY) // Key not available from RingBuffer<byte[]>
                                                                    .setPayload(ByteString.copyFrom(msg))
                                                    )
                                                    .build()
                                    );
                                } else {
                                    log.warn("Subscription channel for topic '{}' group '{}' became inactive. Cannot push message event for offset {}.",
                                            s.getTopic(), s.getGroup(), seq);
                                    // TODO: Implement unsubscription logic if channel dies.
                                }
                            }
                    );
                    // No immediate reply for SUBSCRIBE is typical; success is implied by subsequent MessageEvents.
                    // If an explicit "Subscribe OK" ack is needed, send it here.
                }

                default -> {
                    log.warn("Unhandled envelope kind: {} with correlationId: {}. This may indicate a client sending an unexpected message type or a new unhandled type.",
                            env.getKindCase(), corrId);
                    // Optionally, send a generic error reply to the client.
                }
            }

            /** Flush any buffered writes (e.g., for non-streaming replies) */
            if (env.getKindCase() != BrokerApi.Envelope.KindCase.SUBSCRIBE) {
                ctx.flush();
            }

        } catch (final Exception ex) {
            log.error("Error handling envelope kind {} (corrId: {}): {}", env.getKindCase(), corrId, ex.getMessage(), ex);
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        final String remoteAddress = ctx.channel().remoteAddress() != null ? ctx.channel().remoteAddress().toString() : "unknown remote";
        log.error("NettyServerRequestHandler connection error for remote address {}: {}",
                remoteAddress,
                cause.getMessage(), cause);
        ctx.close();
    }
}