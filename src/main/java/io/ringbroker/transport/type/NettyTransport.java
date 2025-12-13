package io.ringbroker.transport.type;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
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
import io.ringbroker.transport.impl.NettyServerRequestHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;

@Slf4j
public class NettyTransport {
    @Getter private int port;
    private final ClusteredIngress ingress;
    private final OffsetStore offsetStore;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public NettyTransport(final int port, final ClusteredIngress ingress, final OffsetStore offsetStore) {
        this.port = port;
        this.ingress = ingress;
        this.offsetStore = offsetStore;
    }

    public void start() throws InterruptedException {
        final IoHandlerFactory factory = NioIoHandler.newFactory();

        /*
         * Netty Threading Model:
         * 1 Boss thread for accepting connections.
         * 0 (Default) Worker threads for IO processing (defaults to NettyRuntime.availableProcessors() * 2).
         */
        bossGroup = new MultiThreadIoEventLoopGroup(1, factory);
        workerGroup = new MultiThreadIoEventLoopGroup(0, factory);

        final ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel ch) {
                        final ChannelPipeline p = ch.pipeline();

                        /*
                         * Consolidate flushes to reduce syscalls under heavy load.
                         * 256 pending writes or explicit flush triggers actual write.
                         */
                        p.addLast(new FlushConsolidationHandler(256, true));

                        /* Protocol Buffers Framing (Varint32 Length Prefix) */
                        p.addLast(new ProtobufVarint32FrameDecoder());
                        p.addLast(new ProtobufDecoder(BrokerApi.Envelope.getDefaultInstance()));
                        p.addLast(new ProtobufVarint32LengthFieldPrepender());
                        p.addLast(new ProtobufEncoder());

                        /* Business Logic Handler */
                        p.addLast(new NettyServerRequestHandler(ingress, offsetStore));
                    }
                })

                /*
                 * TCP Tuning:
                 * TCP_NODELAY: Disable Nagle's algorithm. Essential for low-latency messaging.
                 * SO_KEEPALIVE: Detect dead peers at TCP level.
                 */
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        final ChannelFuture f = b.bind(port).sync();
        port = ((InetSocketAddress) f.channel().localAddress()).getPort();
        log.info("Netty Transport started on port {}", port);

        f.channel().closeFuture().addListener(cf -> stop());
    }

    public void stop() {
        if (bossGroup != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
    }
}
