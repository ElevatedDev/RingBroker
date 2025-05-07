package io.ringbroker.transport;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.api.BrokerGrpc;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.offset.OffsetStore;
import io.ringbroker.registry.TopicRegistry;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@RequiredArgsConstructor
public final class GrpcTransport {
    private final int port;
    private final ClusteredIngress ingress;
    private final OffsetStore offsets;
    private final TopicRegistry registry;
    private Server server;

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .compressorRegistry(CompressorRegistry.newEmptyInstance())
                .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
                .executor(Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory()))
                .addService(new RemoteBrokerServiceGrpc.RemoteBrokerServiceImplBase() {
                    @Override
                    public void forwardMessage(final RemoteBrokerServiceProto.ForwardRequest req, final StreamObserver<RemoteBrokerServiceProto.ForwardResponse> resp) {
                        try {
                            ingress.publish(req.getTopic(),
                                    req.getKey().toByteArray(),
                                    req.getPayload().toByteArray());
                            resp.onNext(RemoteBrokerServiceProto.ForwardResponse.newBuilder().setSuccess(true).build());
                        } catch (final Exception e) {
                            resp.onNext(RemoteBrokerServiceProto.ForwardResponse.newBuilder().setSuccess(false).build());
                        }
                        resp.onCompleted();
                    }
                })
                .addService(new BrokerGrpc.BrokerImplBase() {

                    @Override
                    public void publish(final BrokerApi.Message req,
                                        final StreamObserver<BrokerApi.PublishReply> resp) {
                        try {
                            ingress.publish(
                                    req.getTopic(),
                                    req.getKey().toByteArray(),
                                    req.getRetries(),
                                    req.getPayload().toByteArray()
                            );
                            resp.onNext(BrokerApi.PublishReply.newBuilder()
                                    .setSuccess(true)
                                    .build());
                        } catch (final Exception e) {
                            resp.onNext(BrokerApi.PublishReply.newBuilder()
                                    .setSuccess(false)
                                    .setError(e.getMessage())
                                    .build());
                        } finally {
                            resp.onCompleted();
                        }
                    }

                    @Override
                    public void commitOffset(final BrokerApi.CommitRequest req,
                                             final StreamObserver<BrokerApi.CommitAck> resp) {
                        boolean ok = true;
                        try {
                            offsets.commit(req.getTopic(), req.getGroup(), req.getPartition(), req.getOffset());
                        } catch (final Exception e) {
                            ok = false;
                        }
                        resp.onNext(BrokerApi.CommitAck.newBuilder().setSuccess(ok).build());
                        resp.onCompleted();
                    }

                    @Override
                    public void fetchCommitted(final BrokerApi.CommittedRequest req,
                                               final StreamObserver<BrokerApi.CommittedReply> resp) {
                        final long offset = offsets.fetch(req.getTopic(), req.getGroup(), req.getPartition());
                        resp.onNext(BrokerApi.CommittedReply.newBuilder().setOffset(offset).build());
                        resp.onCompleted();
                    }

                    @Override
                    public void createTopic(final BrokerApi.TopicRequest req,
                                            final StreamObserver<BrokerApi.TopicReply> resp) {
                        try {
                            registry.addTopic(req.getTopic(), null);
                            resp.onNext(BrokerApi.TopicReply.newBuilder().setSuccess(true).build());
                        } catch (final Exception e) {
                            resp.onNext(BrokerApi.TopicReply.newBuilder()
                                    .setSuccess(false)
                                    .setError(e.getMessage())
                                    .build());
                        } finally {
                            resp.onCompleted();
                        }
                    }

                    @Override
                    public void deleteTopic(final BrokerApi.TopicRequest req,
                                            final StreamObserver<BrokerApi.TopicReply> resp) {
                        try {
                            registry.removeTopic(req.getTopic());
                            resp.onNext(BrokerApi.TopicReply.newBuilder().setSuccess(true).build());
                        } catch (final Exception e) {
                            resp.onNext(BrokerApi.TopicReply.newBuilder()
                                    .setSuccess(false)
                                    .setError(e.getMessage())
                                    .build());
                        } finally {
                            resp.onCompleted();
                        }
                    }

                    @Override
                    public void listTopics(final BrokerApi.Empty req,
                                           final StreamObserver<BrokerApi.TopicListReply> resp) {
                        final var reply = BrokerApi.TopicListReply.newBuilder()
                                .addAllTopics(registry.listTopics())
                                .build();
                        resp.onNext(reply);
                        resp.onCompleted();
                    }

                    @Override
                    public void describeTopic(final BrokerApi.TopicRequest req,
                                              final StreamObserver<BrokerApi.TopicDescriptionReply> resp) {
                        if (!registry.contains(req.getTopic())) {
                            resp.onNext(BrokerApi.TopicDescriptionReply.newBuilder()
                                    .setError("Topic not found")
                                    .build());
                        } else {
                            resp.onNext(BrokerApi.TopicDescriptionReply.newBuilder()
                                    .setTopic(req.getTopic())
                                    .setPartitions(ingress.getTotalPartitions())
                                    .build());
                        }
                        resp.onCompleted();
                    }

                    @Override
                    public void subscribeTopic(final BrokerApi.SubscribeRequest req,
                                               final StreamObserver<BrokerApi.MessageEvent> responseObserver) {

                        final Context.CancellableContext ctx = Context.current().withCancellation();
                        final ServerCallStreamObserver<BrokerApi.MessageEvent> serverObserver =
                                (ServerCallStreamObserver<BrokerApi.MessageEvent>) responseObserver;

                        final Queue<BrokerApi.MessageEvent> buffer = new ConcurrentLinkedQueue<>();
                        final AtomicBoolean flushing = new AtomicBoolean(false);

                        serverObserver.setOnCancelHandler(() -> ctx.cancel(null));

                        // Define flush logic (only one active flush at a time)
                        final Runnable flush = () -> {
                            if (!flushing.compareAndSet(false, true)) return;

                            try {
                                while (serverObserver.isReady()) {
                                    final BrokerApi.MessageEvent ev = buffer.poll();
                                    if (ev == null) break;
                                    serverObserver.onNext(ev);
                                }
                            } catch (final Exception e) {
                                ctx.cancel(new RuntimeException("Flush failed", e));
                            } finally {
                                flushing.set(false);
                            }
                        };

                        // Only set the flush handler ONCE (gRPC only allows this once)
                        serverObserver.setOnReadyHandler(flush);

                        // Register your subscription logic
                        ctx.run(() -> {
                            try {
                                ingress.subscribeTopic(req.getTopic(), req.getGroup(), (offset, payload) -> {
                                    if (ctx.isCancelled() || serverObserver.isCancelled()) return;

                                    final BrokerApi.MessageEvent ev = BrokerApi.MessageEvent.newBuilder()
                                            .setTopic(req.getTopic())
                                            .setOffset(offset)
                                            .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                                            .setKey(com.google.protobuf.ByteString.EMPTY)
                                            .build();

                                    buffer.offer(ev);
                                    if (serverObserver.isReady()) flush.run();
                                });
                            } catch (final Exception e) {
                                if (!ctx.isCancelled()) {
                                    serverObserver.onError(Status.INTERNAL
                                            .withDescription(e.getMessage())
                                            .asRuntimeException());
                                }
                            }
                        });
                    }

                    @Override
                    public void fetch(final BrokerApi.FetchRequest req,
                                      final StreamObserver<BrokerApi.FetchReply> resp) {
                        try {
                            final var delivery = ingress.getDeliveryMap().get(req.getPartition());
                            if (delivery == null) {
                                resp.onError(Status.NOT_FOUND
                                        .withDescription("Partition not found: " + req.getPartition())
                                        .asRuntimeException());
                                return;
                            }

                            final var ring = delivery.getRing();
                            final var builder = BrokerApi.FetchReply.newBuilder();
                            long offset = req.getOffset();
                            final long maxOffset = ring.getCursor();  // latest published

                            for (int i = 0; i < req.getMaxMessages(); i++) {
                                if (offset > maxOffset) break; // nothing more published

                                final byte[] payload = ring.get(offset); // this blocks until seq is visible

                                // Defensive null check (shouldn’t happen)
                                if (payload == null) break;

                                builder.addMessages(BrokerApi.MessageEvent.newBuilder()
                                        .setTopic(req.getTopic())
                                        .setOffset(offset)
                                        .setKey(ByteString.EMPTY)
                                        .setPayload(ByteString.copyFrom(payload))
                                        .build());

                                offset++;
                            }

                            resp.onNext(builder.build());
                            resp.onCompleted();
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            resp.onError(Status.CANCELLED.withDescription("Interrupted").asRuntimeException());
                        } catch (final Exception e) {
                            resp.onError(Status.INTERNAL
                                    .withDescription("Fetch error: " + e.getMessage())
                                    .asRuntimeException());
                        }
                    }
                })
                .build()
                .start();
    }

    public void stop() {
        if (server != null) server.shutdown();
    }
}
