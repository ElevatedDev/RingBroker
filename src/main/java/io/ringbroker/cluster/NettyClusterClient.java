package io.ringbroker.cluster;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.cluster.type.RemoteBrokerClient;

import java.net.InetSocketAddress;

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
        final var m = BrokerApi.Message.newBuilder()
                .setTopic(topic)
                .setRetries(0)
                .setKey(com.google.protobuf.ByteString.copyFrom(key))
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .build();

        channel.writeAndFlush(BrokerApi.Envelope.newBuilder().setPublish(m).build());
    }

    public void close() {
        channel.close();
        group.shutdownGracefully();
    }
}
