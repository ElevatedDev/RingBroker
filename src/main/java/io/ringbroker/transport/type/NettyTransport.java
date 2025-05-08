package io.ringbroker.transport.type;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.offset.OffsetStore;
import io.ringbroker.registry.TopicRegistry;
import io.ringbroker.transport.impl.NettyServerRequestHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class NettyTransport {
    private final int port;
    private final ClusteredIngress ingress;
    private final OffsetStore offsetStore;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        final ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new FlushConsolidationHandler(256, true));
                        // inbound: split the byte stream into frames
                        p.addLast(new ProtobufVarint32FrameDecoder());
                        // inbound: parse each frame as an Envelope
                        p.addLast(new ProtobufDecoder(BrokerApi.Envelope.getDefaultInstance()));
                        // outbound: prefix each Envelope with its varint length
                        p.addLast(new ProtobufVarint32LengthFieldPrepender());
                        // outbound: serialize Envelopes to bytes
                        p.addLast(new ProtobufEncoder());
                        // your business logic
                        p.addLast(new NettyServerRequestHandler(ingress, offsetStore));
                    }
                })
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        final ChannelFuture f = b.bind(port).sync();
        log.info("RawTcpTransport listening on {}", port);
        f.channel().closeFuture().addListener(cf -> stop());
    }

    public void stop() {
        if (bossGroup != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
    }
}
