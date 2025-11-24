package io.ringbroker.cluster.client.impl;

import com.google.protobuf.ByteString;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A Netty‐backed RemoteBrokerClient that:
 * • sends Publish Envelopes to a remote Persistence node, and
 * • waits (via CompletableFuture) for the corresponding ReplicationAck.
 * <p>
 * The pipeline uses Protobuf varint32 framing to match the server side.
 */
@Slf4j
public final class NettyClusterClient implements RemoteBrokerClient {
    private final Channel channel;
    private final EventLoopGroup group;

    /**
     * Tracks all in-flight replication requests:
     * correlationId → CompletableFuture<ReplicationAck>.
     * <p>
     * Futures are self-cleaning: they remove themselves from this map
     * upon completion, cancellation, or exception.
     */
    private final ConcurrentMap<Long, CompletableFuture<BrokerApi.ReplicationAck>> pendingAcks = new ConcurrentHashMap<>();

    public NettyClusterClient(final String host, final int port) throws InterruptedException {
        final IoHandlerFactory factory = NioIoHandler.newFactory();

        this.group = new MultiThreadIoEventLoopGroup(1, factory);

        final Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel ch) {
                        ch.pipeline()
                                // 1) Decode varint32‐length‐prefixed frames
                                .addLast(new ProtobufVarint32FrameDecoder())
                                // 2) Decode each frame into BrokerApi.Envelope
                                .addLast(new ProtobufDecoder(BrokerApi.Envelope.getDefaultInstance()))
                                // 3) Our custom handler to catch and complete ReplicationAck futures
                                .addLast(new ClientReplicationHandler(pendingAcks))
                                // 4) Outbound: prepend varint32 length
                                .addLast(new ProtobufVarint32LengthFieldPrepender())
                                // 5) Outbound: serialize BrokerApi.Envelope → bytes
                                .addLast(new ProtobufEncoder());
                    }
                });

        this.channel = bootstrap.connect(new InetSocketAddress(host, port))
                .sync()
                .channel();
        log.info("NettyClusterClient connected to {}:{}", host, port);
    }

    /**
     * Legacy send‐message path. Builds a BrokerApi.Message + Envelope and sends it.
     */
    @Override
    public void sendMessage(final String topic, final byte[] key, final byte[] payload) {
        final BrokerApi.Message msg = BrokerApi.Message.newBuilder()
                .setTopic(topic)
                .setRetries(0)
                .setKey(key == null ? ByteString.EMPTY : ByteString.copyFrom(key))
                .setPayload(ByteString.copyFrom(payload))
                .build();

        final BrokerApi.Envelope env = BrokerApi.Envelope.newBuilder()
                .setPublish(msg)
                .build();

        channel.writeAndFlush(env).addListener(f -> {
            if (!f.isSuccess()) {
                log.error("sendMessage(...) failed to write to channel: {}", f.cause().getMessage(), f.cause());
            }
        });
    }

    /**
     * Zero‐copy path for replication: simply write the pre‐built Envelope.
     * Does not wait for any ack.
     */
    @Override
    public void sendEnvelope(final BrokerApi.Envelope envelope) {
        channel.writeAndFlush(envelope).addListener(f -> {
            if (!f.isSuccess()) {
                log.error("sendEnvelope(...) failed to write: {}", f.cause().getMessage(), f.cause());
            }
        });
    }

    /**
     * Zero‐copy path for replication **with** ack: write the given Envelope,
     * register a CompletableFuture under its correlationId, and return that future.
     * The future completes when a matching ReplicationAck arrives (or exceptionally on error).
     *
     * @param envelope must include a valid correlationId and Publish message.
     * @return CompletableFuture that completes with the ReplicationAck from the server.
     */
    public CompletableFuture<BrokerApi.ReplicationAck> sendEnvelopeWithAck(final BrokerApi.Envelope envelope) {
        final long corrId = envelope.getCorrelationId();
        final CompletableFuture<BrokerApi.ReplicationAck> future = new CompletableFuture<>();

        pendingAcks.put(corrId, future);

        // LEAK FIX: Ensure removal from map on ANY completion (Success, Failure, or Cancellation)
        future.whenComplete((res, ex) -> pendingAcks.remove(corrId));

        // Write-and-flush the Envelope. On write failure, complete the future exceptionally:
        channel.writeAndFlush(envelope).addListener(f -> {
            if (!f.isSuccess()) {
                // This triggers the whenComplete hook above, ensuring cleanup
                future.completeExceptionally(f.cause());
                log.error("Failed to send Envelope for correlationId {}: {}", corrId, f.cause().getMessage());
            }
        });

        return future;
    }

    /**
     * Close the underlying channel and event loop.
     */
    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close().sync();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while closing NettyClusterClient channel.", e);
        } finally {
            group.shutdownGracefully();
            log.info("NettyClusterClient event loop group shut down.");
        }
    }
}