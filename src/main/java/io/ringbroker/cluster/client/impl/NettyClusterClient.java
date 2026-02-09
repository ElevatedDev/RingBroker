package io.ringbroker.cluster.client.impl;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.membership.channel.ClientReplicationHandler;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public final class NettyClusterClient implements RemoteBrokerClient {

    private static final Object SHARED_GROUP_LOCK = new Object();
    private static final AtomicInteger SHARED_GROUP_REFS = new AtomicInteger(0);
    private static final int SHARED_GROUP_THREADS = Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors()));
    private static volatile EventLoopGroup sharedGroup;

    private final Channel channel;
    private final EventLoopGroup group;

    private final ConcurrentMap<Long, CompletableFuture<BrokerApi.ReplicationAck>> pendingAcks =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, CompletableFuture<BrokerApi.BackfillReply>> pendingBackfill =
            new ConcurrentHashMap<>();

    private final long requestTimeoutMillis;
    private final AtomicLong corrSeq = new AtomicLong(1L);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public NettyClusterClient(final String host, final int port) throws InterruptedException {
        this(host, port, Long.getLong("ringbroker.cluster.requestTimeoutMillis", 5_000L));
    }

    public NettyClusterClient(final String host,
                              final int port,
                              final long requestTimeoutMillis) throws InterruptedException {
        this.group = acquireSharedGroup();
        this.requestTimeoutMillis = Math.max(1L, requestTimeoutMillis);

        final Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new ProtobufVarint32FrameDecoder())
                                .addLast(new ProtobufDecoder(BrokerApi.Envelope.getDefaultInstance()))
                                .addLast(new ClientReplicationHandler(pendingAcks, pendingBackfill))
                                .addLast(new ProtobufVarint32LengthFieldPrepender())
                                .addLast(new ProtobufEncoder());
                    }
                });

        try {
            this.channel = bootstrap.connect(new InetSocketAddress(host, port))
                    .sync()
                    .channel();
        } catch (final InterruptedException ie) {
            releaseSharedGroup();
            throw ie;
        } catch (final RuntimeException re) {
            releaseSharedGroup();
            throw re;
        } catch (final Error err) {
            releaseSharedGroup();
            throw err;
        }

        log.info("NettyClusterClient connected to {}:{}", host, port);
    }

    @Override
    public void sendMessage(final String topic, final byte[] key, final byte[] payload) {
        if (closed.get()) throw new IllegalStateException("NettyClusterClient is closed");

        final BrokerApi.Message msg = BrokerApi.Message.newBuilder()
                .setTopic(topic)
                .setRetries(0)
                .setKey(key == null ? ByteString.EMPTY : UnsafeByteOperations.unsafeWrap(key))
                .setPayload(UnsafeByteOperations.unsafeWrap(payload))
                .build();

        final BrokerApi.Envelope env = BrokerApi.Envelope.newBuilder()
                .setPublish(msg)
                .build();

        channel.writeAndFlush(env).addListener(f -> {
            if (!f.isSuccess()) {
                log.error("sendMessage(...) failed: {}", f.cause().getMessage(), f.cause());
            }
        });
    }

    @Override
    public void sendEnvelope(final BrokerApi.Envelope envelope) {
        if (closed.get()) throw new IllegalStateException("NettyClusterClient is closed");

        channel.writeAndFlush(envelope).addListener(f -> {
            if (!f.isSuccess()) {
                log.error("sendEnvelope(...) failed: {}", f.cause().getMessage(), f.cause());
            }
        });
    }

    @Override
    public CompletableFuture<BrokerApi.ReplicationAck> sendEnvelopeWithAck(final BrokerApi.Envelope envelope) {
        if (closed.get()) return CompletableFuture.failedFuture(new ClosedChannelException());

        final long corrId = corrSeq.getAndIncrement();
        final BrokerApi.Envelope toSend = BrokerApi.Envelope.newBuilder(envelope)
                .setCorrelationId(corrId)
                .build();

        final CompletableFuture<BrokerApi.ReplicationAck> future = new CompletableFuture<>();
        pendingAcks.put(corrId, future);
        final ScheduledFuture<?> timeoutTask = channel.eventLoop().schedule(() -> {
            future.completeExceptionally(new TimeoutException("Replication ack timeout corrId=" + corrId));
        }, requestTimeoutMillis, TimeUnit.MILLISECONDS);
        future.whenComplete((res, ex) -> {
            pendingAcks.remove(corrId);
            timeoutTask.cancel(false);
        });

        channel.writeAndFlush(toSend).addListener(f -> {
            if (!f.isSuccess()) {
                future.completeExceptionally(f.cause());
                log.error("Failed to send Envelope corrId {}: {}", corrId, f.cause().getMessage());
            }
        });

        return future;
    }

    @Override
    public CompletableFuture<BrokerApi.BackfillReply> sendBackfill(final BrokerApi.Envelope envelope) {
        if (closed.get()) return CompletableFuture.failedFuture(new ClosedChannelException());

        final long corrId = corrSeq.getAndIncrement();
        final BrokerApi.Envelope toSend = BrokerApi.Envelope.newBuilder(envelope)
                .setCorrelationId(corrId)
                .build();

        final CompletableFuture<BrokerApi.BackfillReply> future = new CompletableFuture<>();
        pendingBackfill.put(corrId, future);
        final ScheduledFuture<?> timeoutTask = channel.eventLoop().schedule(() -> {
            future.completeExceptionally(new TimeoutException("Backfill timeout corrId=" + corrId));
        }, requestTimeoutMillis, TimeUnit.MILLISECONDS);
        future.whenComplete((res, ex) -> {
            pendingBackfill.remove(corrId);
            timeoutTask.cancel(false);
        });

        channel.writeAndFlush(toSend).addListener(f -> {
            if (!f.isSuccess()) {
                future.completeExceptionally(f.cause());
                log.error("Failed to send Backfill corrId {}: {}", corrId, f.cause().getMessage());
            }
        });

        return future;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;

        final ClosedChannelException ex = new ClosedChannelException();
        pendingAcks.forEach((id, f) -> f.completeExceptionally(ex));
        pendingBackfill.forEach((id, f) -> f.completeExceptionally(ex));
        pendingAcks.clear();
        pendingBackfill.clear();

        try {
            if (channel != null) channel.close().syncUninterruptibly();
        } finally {
            releaseSharedGroup();
        }

        log.info("NettyClusterClient closed.");
    }

    private static EventLoopGroup acquireSharedGroup() {
        synchronized (SHARED_GROUP_LOCK) {
            EventLoopGroup g = sharedGroup;
            if (g == null || g.isShuttingDown() || g.isShutdown() || g.isTerminated()) {
                g = new MultiThreadIoEventLoopGroup(SHARED_GROUP_THREADS, NioIoHandler.newFactory());
                sharedGroup = g;
            }
            SHARED_GROUP_REFS.incrementAndGet();
            return g;
        }
    }

    private static void releaseSharedGroup() {
        EventLoopGroup g = null;
        synchronized (SHARED_GROUP_LOCK) {
            final int refs = SHARED_GROUP_REFS.decrementAndGet();
            if (refs <= 0) {
                SHARED_GROUP_REFS.set(0);
                g = sharedGroup;
                sharedGroup = null;
            }
        }
        if (g != null) {
            g.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }
}
