package io.ringbroker.transport.impl;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.broker.ingress.Ingress;
import io.ringbroker.offset.OffsetStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public class NettyServerRequestHandler extends SimpleChannelInboundHandler<BrokerApi.Envelope> {

    private final ClusteredIngress ingress;
    private final OffsetStore offsetStore;

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final BrokerApi.Envelope env) {
        final long corrId = env.getCorrelationId();

        try {
            switch (env.getKindCase()) {
                case PUBLISH -> {
                    final var m = env.getPublish();
                    /* Wait for Quorum (Local + Replicas) before replying */
                    ingress.publish(corrId, m.getTopic(), m.getKey().toByteArray(), m.getRetries(), m.getPayload().toByteArray())
                            .whenComplete((v, ex) -> {
                                if (ex != null) {
                                    log.error("Publish failed (corrId: {}): {}", corrId, ex.getMessage());
                                    ctx.close(); // Or send error reply
                                } else {
                                    writeReply(ctx, corrId, BrokerApi.PublishReply.newBuilder().setSuccess(true).build());
                                }
                            });
                }

                case BATCH -> {
                    final var list = env.getBatch().getMessagesList();
                    final List<CompletableFuture<Void>> futures = new ArrayList<>(list.size());

                    for (final var m : list) {
                        futures.add(ingress.publish(corrId, m.getTopic(), m.getKey().toByteArray(), m.getRetries(), m.getPayload().toByteArray()));
                    }

                    /* Wait for ALL messages in batch to be safe */
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                            .whenComplete((v, ex) -> {
                                if (ex != null) {
                                    log.error("Batch publish failed (corrId: {}): {}", corrId, ex.getMessage());
                                    ctx.close();
                                } else {
                                    writeReply(ctx, corrId, BrokerApi.PublishReply.newBuilder().setSuccess(true).build());
                                }
                            });
                }

                case COMMIT -> {
                    final var req = env.getCommit();
                    offsetStore.commit(req.getTopic(), req.getGroup(), req.getPartition(), req.getOffset());
                    writeReply(ctx, corrId, BrokerApi.CommitAck.newBuilder().setSuccess(true).build());
                }

                case COMMITTED -> {
                    final var r = env.getCommitted();
                    final long off = offsetStore.fetch(r.getTopic(), r.getGroup(), r.getPartition());
                    writeReply(ctx, corrId, BrokerApi.CommittedReply.newBuilder().setOffset(off).build());
                }

                case FETCH -> {
                    final var f = env.getFetch();

                    final Ingress part = ingress.getIngressMap().get(f.getPartition());
                    if (part == null) {
                        writeReply(ctx, corrId, BrokerApi.FetchReply.newBuilder().build());
                        break;
                    }

                    final BrokerApi.FetchReply.Builder fr = BrokerApi.FetchReply.newBuilder();
                    final String topic = f.getTopic();
                    final int max = f.getMaxMessages();
                    final long startOffset = f.getOffset();

                    part.fetch(startOffset, max, (off, segBuf, payloadPos, payloadLen) -> {
                        // Create an isolated ByteBuffer view over the mmap region (zero-copy).
                        final ByteBuffer bb = segBuf.duplicate();
                        bb.position(payloadPos);
                        bb.limit(payloadPos + payloadLen);

                        fr.addMessages(BrokerApi.MessageEvent.newBuilder()
                                .setTopic(topic)
                                .setOffset(off)
                                .setKey(ByteString.EMPTY)
                                .setPayload(UnsafeByteOperations.unsafeWrap(bb)));
                    });

                    writeReply(ctx, corrId, fr.build());
                }

                case SUBSCRIBE -> {
                    final var s = env.getSubscribe();
                    ingress.subscribeTopic(s.getTopic(), s.getGroup(), (seq, msg) -> {
                        if (ctx.channel().isActive()) {
                            ctx.writeAndFlush(
                                    BrokerApi.Envelope.newBuilder()
                                            .setMessageEvent(BrokerApi.MessageEvent.newBuilder()
                                                    .setTopic(s.getTopic())
                                                    .setOffset(seq)
                                                    .setKey(ByteString.EMPTY)
                                                    .setPayload(ByteString.copyFrom(msg)))
                                            .build()
                            );
                        }
                    });
                }

                default -> log.warn("Unknown envelope kind: {}", env.getKindCase());
            }

        } catch (final Exception ex) {
            log.error("Handler error", ex);
            ctx.close();
        }
    }

    private void writeReply(ChannelHandlerContext ctx, long corrId, com.google.protobuf.GeneratedMessageV3 reply) {
        final BrokerApi.Envelope.Builder b = BrokerApi.Envelope.newBuilder().setCorrelationId(corrId);

        if (reply instanceof BrokerApi.PublishReply r) {
            b.setPublishReply(r);
        } else if (reply instanceof BrokerApi.CommitAck r) {
            b.setCommitAck(r);
        } else if (reply instanceof BrokerApi.CommittedReply r) {
            b.setCommittedReply(r);
        } else if (reply instanceof BrokerApi.FetchReply r) {
            b.setFetchReply(r);
        }

        /* Using writeAndFlush inside async callbacks ensures data goes out immediately */
        if (ctx.channel().isActive()) {
            ctx.writeAndFlush(b.build());
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        log.error("Transport error: {}", cause.getMessage());
        ctx.close();
    }
}
