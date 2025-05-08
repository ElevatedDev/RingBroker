package io.ringbroker.transport.impl;

import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.offset.OffsetStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
                    ingress.publish(m.getTopic(), m.getKey().toByteArray(), m.getRetries(), m.getPayload().toByteArray());

                    // buffer the reply
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
                        ingress.publish(m.getTopic(), m.getKey().toByteArray(), m.getRetries(), m.getPayload().toByteArray());
                    }
                    ctx.write(
                            BrokerApi.Envelope.newBuilder()
                                    .setCorrelationId(corrId)
                                    .setPublishReply(
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
                    final var ring = delivery.getRing();

                    final BrokerApi.FetchReply.Builder fr = BrokerApi.FetchReply.newBuilder();

                    long offset = f.getOffset();
                    final long max = ring.getCursor();

                    for (int i = 0; i < f.getMaxMessages() && offset <= max; i++, offset++) {
                        final byte[] payload = ring.get(offset);
                        fr.addMessages(
                                BrokerApi.MessageEvent.newBuilder()
                                        .setTopic(f.getTopic())
                                        .setOffset(offset)
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
                    // install your callback to push streamed events:
                    ingress.subscribeTopic(
                            s.getTopic(),
                            s.getGroup(),
                            (seq, msg) -> {
                                ctx.write(
                                        BrokerApi.Envelope.newBuilder()
                                                // no correlationId needed on push events (client treats them specially)
                                                .setMessageEvent(
                                                        BrokerApi.MessageEvent.newBuilder()
                                                                .setTopic(s.getTopic())
                                                                .setOffset(seq)
                                                                .setKey(ByteString.EMPTY)
                                                                .setPayload(ByteString.copyFrom(msg))
                                                )
                                                .build()
                                );
                                ctx.flush();
                            }
                    );
                }

                default -> { /* ignore */ }
            }

            // flush all buffered replies for this request
            ctx.flush();

        } catch (final Exception ex) {
            log.error("Error handling envelope {}", env.getKindCase(), ex);
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        log.error("RawTcp connection error", cause);
        ctx.close();
    }
}
