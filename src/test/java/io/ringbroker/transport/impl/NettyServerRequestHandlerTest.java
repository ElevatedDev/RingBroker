package io.ringbroker.transport.impl;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.ringbroker.api.BrokerApi;
import io.ringbroker.broker.ingress.ClusteredIngress;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import io.ringbroker.cluster.membership.replicator.AdaptiveReplicator;
import io.ringbroker.cluster.membership.resolver.ReplicaSetResolver;
import io.ringbroker.cluster.metadata.JournaledLogMetadataStore;
import io.ringbroker.core.wait.Blocking;
import io.ringbroker.offset.InMemoryOffsetStore;
import io.ringbroker.registry.TopicRegistry;
import io.ringbroker.transport.type.NettyTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Lightweight Netty roundtrip (no containers) to validate request/response wiring.
 */
final class NettyServerRequestHandlerTest {

    @TempDir
    Path dir;

    private NettyTransport transport;
    private ClusteredIngress ingress;
    private InMemoryOffsetStore offsets;

    @BeforeEach
    void setUp() throws Exception {
        final TopicRegistry registry = TopicRegistry.builder()
                .topic("t", BrokerApi.Message.getDescriptor())
                .build();

        offsets = new InMemoryOffsetStore(dir.resolve("offsets"));
        final var metadata = new JournaledLogMetadataStore(dir.resolve("metadata"));
        ingress = ClusteredIngress.create(
                registry,
                (key, total) -> 0,
                1,
                0,
                1,
                Map.of(),
                dir.resolve("data"),
                16,
                new Blocking(),
                1024,
                4,
                false,
                offsets,
                BrokerRole.PERSISTENCE,
                new ReplicaSetResolver(1, () -> java.util.List.of(
                        new io.ringbroker.cluster.membership.member.Member(
                                0, BrokerRole.PERSISTENCE, new java.net.InetSocketAddress("localhost", 0), System.currentTimeMillis(), 1)
                )),
                new AdaptiveReplicator(1, Map.of(), 500),
                metadata
        );

        transport = new NettyTransport(0, ingress, offsets);
        transport.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (transport != null) transport.stop();
        if (ingress != null) ingress.shutdown();
        if (offsets != null) offsets.close();
    }

    @Test
    void publishAndFetchRoundTrip() throws Exception {
        final int port = transport.getPort();
        final NioEventLoopGroup group = new NioEventLoopGroup(1);

        try (final TestClient client = new TestClient(group, "localhost", port)) {
            final BrokerApi.Envelope pubEnv = BrokerApi.Envelope.newBuilder()
                    .setCorrelationId(1)
                    .setPublish(BrokerApi.Message.newBuilder()
                            .setTopic("t")
                            .setPartitionId(0)
                            .setPayload(com.google.protobuf.ByteString.copyFromUtf8("hello"))
                            .build())
                    .build();

            client.send(pubEnv).get(5, TimeUnit.SECONDS);

            final BrokerApi.Envelope fetchEnv = BrokerApi.Envelope.newBuilder()
                    .setCorrelationId(2)
                    .setFetch(BrokerApi.FetchRequest.newBuilder()
                            .setTopic("t")
                            .setGroup("g")
                            .setPartition(0)
                            .setOffset(0)
                            .setMaxMessages(10)
                            .build())
                    .build();

            final BrokerApi.Envelope reply = client.send(fetchEnv).get(5, TimeUnit.SECONDS);
            assertEquals(BrokerApi.FetchReply.Status.OK, reply.getFetchReply().getStatus());
            assertEquals(1, reply.getFetchReply().getMessagesCount());
            assertEquals("hello", reply.getFetchReply().getMessages(0).getPayload().toStringUtf8());
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }

    private static final class TestClient implements AutoCloseable {
        private final EventLoopGroup group;
        private final Channel ch;

        TestClient(final EventLoopGroup group, final String host, final int port) throws InterruptedException {
            this.group = group;
            final Bootstrap b = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(final SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new ProtobufVarint32FrameDecoder())
                                    .addLast(new ProtobufDecoder(BrokerApi.Envelope.getDefaultInstance()))
                                    .addLast(new ProtobufVarint32LengthFieldPrepender())
                                    .addLast(new ProtobufEncoder());
                        }
                    });

            this.ch = b.connect(new InetSocketAddress(host, port)).sync().channel();
        }

        CompletableFuture<BrokerApi.Envelope> send(final BrokerApi.Envelope env) {
            final CompletableFuture<BrokerApi.Envelope> fut = new CompletableFuture<>();
            ch.pipeline().addLast(new SimpleChannelInboundHandler<BrokerApi.Envelope>() {
                @Override
                protected void channelRead0(final ChannelHandlerContext ctx, final BrokerApi.Envelope msg) {
                    fut.complete(msg);
                    ctx.pipeline().remove(this);
                }
            });
            ch.writeAndFlush(env).addListener(f -> {
                if (!f.isSuccess()) fut.completeExceptionally(f.cause());
            });
            return fut;
        }

        @Override
        public void close() {
            if (ch != null) ch.close().syncUninterruptibly();
        }
    }
}
