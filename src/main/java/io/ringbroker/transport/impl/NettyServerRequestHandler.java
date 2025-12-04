package io.ringbroker.transport.impl;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.broker.ingress.Ingress;
import io.ringbroker.core.lsn.Lsn;
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
                    ingress.publish(corrId, m.getTopic(), m.getKey().toByteArray(), m.getRetries(), m.getPayload().toByteArray())
                            .whenComplete((v, ex) -> {
                                if (ex != null) {
                                    log.error("Publish failed (corrId: {}): {}", corrId, ex.getMessage());
                                    writeReply(ctx, corrId, BrokerApi.PublishReply.newBuilder()
                                            .setSuccess(false)
                                            .setError(String.valueOf(ex.getMessage()))
                                            .build());
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

                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                            .whenComplete((v, ex) -> {
                                if (ex != null) {
                                    log.error("Batch publish failed (corrId: {}): {}", corrId, ex.getMessage());
                                    writeReply(ctx, corrId, BrokerApi.PublishReply.newBuilder()
                                            .setSuccess(false)
                                            .setError(String.valueOf(ex.getMessage()))
                                            .build());
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

                    final long startLsn = f.getOffset();
                    final long epoch = Lsn.epoch(startLsn);
                    final long startSeq = Lsn.seq(startLsn);

                    final int partitionId = f.getPartition();
                    final Ingress part = ingress.getIngressMap().get(partitionId);
                    final var placementOpt = ingress.placementForEpoch(partitionId, epoch);

                    final BrokerApi.FetchReply.Builder fr = BrokerApi.FetchReply.newBuilder();

                    if (placementOpt.isPresent() && !placementOpt.get().contains(ingress.getMyNodeId())) {
                        fr.setStatus(BrokerApi.FetchReply.Status.NOT_IN_PLACEMENT);
                        fr.addAllRedirectNodes(placementOpt.get());
                        writeReply(ctx, corrId, fr.build());
                        break;
                    }

                    if (part == null) {
                        fr.setStatus(BrokerApi.FetchReply.Status.EPOCH_MISSING);
                        fr.addAllRedirectNodes(placementOpt.orElseGet(List::of));
                        writeReply(ctx, corrId, fr.build());
                        break;
                    }

                    if (!part.getVirtualLog().hasEpoch(epoch)) {
                        fr.setStatus(BrokerApi.FetchReply.Status.EPOCH_MISSING);
                        fr.addAllRedirectNodes(placementOpt.orElseGet(List::of));
                        writeReply(ctx, corrId, fr.build());
                        break;
                    }

                    final String topic = f.getTopic();
                    final int max = f.getMaxMessages();

                    final int visited = part.fetchEpoch(epoch, startSeq, max, (off, segBuf, payloadPos, payloadLen) -> {
                        final ByteBuffer bb = segBuf.duplicate();
                        bb.position(payloadPos);
                        bb.limit(payloadPos + payloadLen);

                        fr.addMessages(BrokerApi.MessageEvent.newBuilder()
                                .setTopic(topic)
                                .setOffset(Lsn.encode(epoch, off))
                                .setKey(ByteString.EMPTY)
                                .setPayload(UnsafeByteOperations.unsafeWrap(bb)));
                    });

                    if (visited == 0 && placementOpt.isPresent() && !placementOpt.get().isEmpty()) {
                        fr.setStatus(BrokerApi.FetchReply.Status.EPOCH_MISSING);
                        fr.addAllRedirectNodes(placementOpt.get());
                    } else {
                        fr.setStatus(BrokerApi.FetchReply.Status.OK);
                    }

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
                                                    .setPayload(UnsafeByteOperations.unsafeWrap(msg)))
                                            .build()
                            );
                        }
                    });
                }

                case APPEND -> {
                    ingress.handleAppendAsync(env.getAppend())
                            .whenComplete((ack, ex) -> {
                                if (ex != null) {
                                    writeReply(ctx, corrId, BrokerApi.ReplicationAck.newBuilder()
                                            .setStatus(BrokerApi.ReplicationAck.Status.ERROR_UNKNOWN)
                                            .setErrorMessage(String.valueOf(ex.getMessage()))
                                            .build());
                                } else {
                                    writeReply(ctx, corrId, ack);
                                }
                            });
                }

                case APPEND_BATCH -> {
                    ingress.handleAppendBatchAsync(env.getAppendBatch())
                            .whenComplete((ack, ex) -> {
                                if (ex != null) {
                                    writeReply(ctx, corrId, BrokerApi.ReplicationAck.newBuilder()
                                            .setStatus(BrokerApi.ReplicationAck.Status.ERROR_UNKNOWN)
                                            .setErrorMessage(String.valueOf(ex.getMessage()))
                                            .build());
                                } else {
                                    writeReply(ctx, corrId, ack);
                                }
                            });
                }

                case EPOCH_STATUS -> {
                    ingress.handleEpochStatusAsync(env.getEpochStatus())
                            .whenComplete((ack, ex) -> {
                                if (ex != null) {
                                    writeReply(ctx, corrId, BrokerApi.ReplicationAck.newBuilder()
                                            .setStatus(BrokerApi.ReplicationAck.Status.ERROR_UNKNOWN)
                                            .setErrorMessage(String.valueOf(ex.getMessage()))
                                            .build());
                                } else {
                                    writeReply(ctx, corrId, ack);
                                }
                            });
                }

                case SEAL -> {
                    ingress.handleSealAndRollAsync(env.getSeal())
                            .whenComplete((ack, ex) -> {
                                if (ex != null) {
                                    writeReply(ctx, corrId, BrokerApi.ReplicationAck.newBuilder()
                                            .setStatus(BrokerApi.ReplicationAck.Status.ERROR_UNKNOWN)
                                            .setErrorMessage(String.valueOf(ex.getMessage()))
                                            .build());
                                } else {
                                    writeReply(ctx, corrId, ack);
                                }
                            });
                }

                case BACKFILL -> {
                    ingress.handleBackfillAsync(env.getBackfill())
                            .whenComplete((bf, ex) -> {
                                if (ex != null) {
                                    writeReply(ctx, corrId, BrokerApi.BackfillReply.newBuilder()
                                            .addRedirectNodes(-1)
                                            .build());
                                } else {
                                    writeReply(ctx, corrId, bf);
                                }
                            });
                }

                case METADATA_UPDATE -> {
                    final BrokerApi.ReplicationAck ack = ingress.handleMetadataUpdate(env.getMetadataUpdate());
                    writeReply(ctx, corrId, ack);
                }

                default -> log.warn("Unknown envelope kind: {}", env.getKindCase());
            }

        } catch (final Exception ex) {
            log.error("Handler error", ex);
            ctx.close();
        }
    }

    private void writeReply(final ChannelHandlerContext ctx,
                            final long corrId,
                            final com.google.protobuf.GeneratedMessageV3 reply) {
        final BrokerApi.Envelope.Builder b = BrokerApi.Envelope.newBuilder().setCorrelationId(corrId);

        if (reply instanceof final BrokerApi.PublishReply r) b.setPublishReply(r);
        else if (reply instanceof final BrokerApi.CommitAck r) b.setCommitAck(r);
        else if (reply instanceof final BrokerApi.CommittedReply r) b.setCommittedReply(r);
        else if (reply instanceof final BrokerApi.FetchReply r) b.setFetchReply(r);
        else if (reply instanceof final BrokerApi.ReplicationAck r) b.setReplicationAck(r);
        else if (reply instanceof final BrokerApi.BackfillReply r) b.setBackfillReply(r);
        else if (reply instanceof final BrokerApi.MessageEvent r) b.setMessageEvent(r);
        else {
            log.warn("Unknown reply type: {}", reply.getClass().getName());
            return;
        }

        ctx.writeAndFlush(b.build());
    }
}
