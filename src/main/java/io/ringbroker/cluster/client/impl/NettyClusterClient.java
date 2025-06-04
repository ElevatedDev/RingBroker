// ── src/main/java/io/ringbroker/cluster/client/impl/NettyClusterClient.java
package io.ringbroker.cluster.client.impl;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.cluster.client.RemoteBrokerClient;

import java.net.InetSocketAddress;

public final class NettyClusterClient implements RemoteBrokerClient {
    private final Channel channel;
    private final EventLoopGroup   group;

    public NettyClusterClient(final String host, final int port) throws InterruptedException {
        this.group = new NioEventLoopGroup(1);

        final Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<>() {
                    @Override protected void initChannel(final Channel ch) {
                        ch.pipeline()
                                .addLast(new LengthFieldPrepender(4))
                                .addLast(new ProtobufEncoder());
                    }
                });

        this.channel = bootstrap.connect(new InetSocketAddress(host, port))
                .sync().channel();
    }

    @Override
    public void sendMessage(final String topic,
                            final byte[] key,
                            final byte[] payload) {

        var msg = BrokerApi.Message.newBuilder()
                .setTopic(topic)
                .setRetries(0)
                .setKey(key == null ? com.google.protobuf.ByteString.EMPTY
                        : com.google.protobuf.ByteString.copyFrom(key))
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .build();

        channel.writeAndFlush(
                BrokerApi.Envelope.newBuilder().setPublish(msg).build());
    }

    /** Zero-copy replication path. */
    @Override
    public void sendEnvelope(final BrokerApi.Envelope envelope) {
        channel.writeAndFlush(envelope);
    }

    public void close() {
        channel.close();
        group.shutdownGracefully();
    }
}
