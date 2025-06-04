package io.ringbroker.cluster.client.impl;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.cluster.client.RemoteBrokerClient;

import java.net.InetSocketAddress;

/**
 * {@code NettyClusterClient} is a concrete implementation of {@link RemoteBrokerClient} that uses Netty
 * to forward messages to another broker node in a distributed cluster. It establishes a persistent
 * TCP connection to a remote broker using Netty's asynchronous networking framework and encodes messages
 * using Protocol Buffers.
 * <p>
 * This client is responsible for:
 * <ul>
 *   <li>Establishing and managing a Netty channel to the remote broker.</li>
 *   <li>Encoding messages with a length field and Protocol Buffers for transmission.</li>
 *   <li>Forwarding messages to the remote broker using the {@code sendMessage} method.</li>
 *   <li>Gracefully closing the connection and releasing resources when no longer needed.</li>
 * </ul>
 * <p>
 * Usage example:
 * <pre>
 *   NettyClusterClient client = new NettyClusterClient("host", port);
 *   client.sendMessage("topic", keyBytes, payloadBytes);
 *   client.close();
 * </pre>
 */
public final class NettyClusterClient implements RemoteBrokerClient {

    private static final IoHandlerFactory SHARED_FACTORY = NioIoHandler.newFactory();

    private final Channel channel;
    private final EventLoopGroup group;

    public NettyClusterClient(final String host, final int port) throws InterruptedException {
        group = new MultiThreadIoEventLoopGroup(SHARED_FACTORY);

        final Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(final Channel ch) {
                        ch.pipeline()
                                .addLast(new LengthFieldPrepender(4))
                                .addLast(new ProtobufEncoder());
                    }
                });

        channel = bootstrap.connect(new InetSocketAddress(host, port)).sync().channel();
    }

    @Override
    public void sendMessage(final String topic, final byte[] key, final byte[] payload) {
        final var message = BrokerApi.Message.newBuilder()
                .setTopic(topic)
                .setRetries(0)
                .setKey(com.google.protobuf.ByteString.copyFrom(key))
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .build();

        channel.writeAndFlush(BrokerApi.Envelope.newBuilder().setPublish(message).build());
    }

    @Override
    public void sendHeartbeat(final int fromNode) {
        // Heartbeats reuse the Envelope frame with no payload
        channel.writeAndFlush(BrokerApi.Envelope.newBuilder().build());
    }

    public void close() {
        channel.close();
        group.shutdownGracefully();
    }
}
