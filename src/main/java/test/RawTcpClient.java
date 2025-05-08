package test;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.ringbroker.api.BrokerApi;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;

/**
 * A raw-TCP client using Protobuf varint32 framing to communicate with the RingBroker server.
 */
public class RawTcpClient implements AutoCloseable {
    private static final int FLUSH_BATCH_SIZE = 1;

    private final Channel channel;
    private final EventLoopGroup group;
    private final LongAdder nextCorr = new LongAdder();
    private final ConcurrentMap<Long, CompletableFuture<BrokerApi.Envelope>> inflight = new ConcurrentHashMap<>();
    private final ClientHandler handler = new ClientHandler(inflight);
    private int writeCounter = 0;

    public RawTcpClient(String host, int port) throws InterruptedException {
        group = new NioEventLoopGroup(1);
        Bootstrap b = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ChannelPipeline p = ch.pipeline();
                        // Inbound: split by varint32 length prefix
                        p.addLast(new ProtobufVarint32FrameDecoder());
                        // Inbound: decode bytes into Envelope messages
                        p.addLast(new ProtobufDecoder(BrokerApi.Envelope.getDefaultInstance()));

                        // Outbound: prepend varint32 length prefix
                        p.addLast(new ProtobufVarint32LengthFieldPrepender());
                        // Outbound: serialize Envelope to bytes
                        p.addLast(new ProtobufEncoder());

                        // Business logic handler
                        p.addLast(handler);
                    }
                });

        channel = b.connect(new InetSocketAddress(host, port)).sync().channel();
    }

    private void maybeFlush() {
        if (++writeCounter >= FLUSH_BATCH_SIZE) {
            channel.flush();
            writeCounter = 0;
        }
    }

    private CompletableFuture<BrokerApi.Envelope> sendEnv(BrokerApi.Envelope env) {
        long id = nextCorr.longValue();
        nextCorr.increment();
        env = env.toBuilder().setCorrelationId(id).build();

        CompletableFuture<BrokerApi.Envelope> fut = new CompletableFuture<>();
        inflight.put(id, fut);

        channel.write(env);
        maybeFlush();
        return fut;
    }

    /**
     * 1) Publish one message
     */
    public CompletableFuture<Void> publishAsync(BrokerApi.Message msg) {
        BrokerApi.Envelope env = BrokerApi.Envelope.newBuilder()
                .setPublish(msg)
                .build();
        return sendEnv(env).thenCompose(reply -> {
            var ack = reply.getPublishReply();
            if (ack.getSuccess()) return CompletableFuture.completedFuture(null);
            else return CompletableFuture.failedFuture(
                    new RuntimeException("publish failed: " + ack.getError())
            );
        });
    }

    /**
     * 2) Publish a batch of messages
     */
    public CompletableFuture<Void> publishBatchAsync(List<BrokerApi.Message> msgs) {
        BrokerApi.Envelope env = BrokerApi.Envelope.newBuilder()
                .setBatch(BrokerApi.BatchMessage.newBuilder().addAllMessages(msgs))
                .build();
        return sendEnv(env).thenCompose(reply -> {
            var ack = reply.getPublishReply();
            if (ack.getSuccess()) return CompletableFuture.completedFuture(null);
            else return CompletableFuture.failedFuture(
                    new RuntimeException("batch failed: " + ack.getError())
            );
        });
    }

    /**
     * 3) Fetch up to maxMsgs from (topic,partition,offset)
     */
    public CompletableFuture<List<BrokerApi.MessageEvent>> fetchAsync(
            String topic, int partition, long offset, int maxMsgs
    ) {
        BrokerApi.Envelope env = BrokerApi.Envelope.newBuilder()
                .setFetch(BrokerApi.FetchRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .setOffset(offset)
                        .setMaxMessages(maxMsgs)
                ).build();
        return sendEnv(env)
                .thenApply(r -> r.getFetchReply().getMessagesList());
    }

    /**
     * 4) Commit an offset
     */
    public CompletableFuture<Void> commitAsync(
            String topic, String group, int partition, long offset
    ) {
        BrokerApi.Envelope env = BrokerApi.Envelope.newBuilder()
                .setCommit(BrokerApi.CommitRequest.newBuilder()
                        .setTopic(topic)
                        .setGroup(group)
                        .setPartition(partition)
                        .setOffset(offset)
                ).build();
        return sendEnv(env).thenApply(r -> null);
    }

    /**
     * 5) Get committed offset
     */
    public CompletableFuture<Long> fetchCommittedAsync(
            String topic, String group, int partition
    ) {
        BrokerApi.Envelope env = BrokerApi.Envelope.newBuilder()
                .setCommitted(BrokerApi.CommittedRequest.newBuilder()
                        .setTopic(topic)
                        .setGroup(group)
                        .setPartition(partition)
                ).build();
        return sendEnv(env)
                .thenApply(r -> r.getCommittedReply().getOffset());
    }

    /**
     * 6) Subscribe: set callback, then send subscribe request
     */
    public void subscribe(
            String topic, String group,
            BiConsumer<Long, byte[]> messageHandler
    ) {
        handler.setSubscribeHandler(messageHandler);
        BrokerApi.Envelope env = BrokerApi.Envelope.newBuilder()
                .setSubscribe(BrokerApi.SubscribeRequest.newBuilder()
                        .setTopic(topic)
                        .setGroup(group)
                ).build();
        channel.writeAndFlush(env);
    }

    /**
     * Force any buffered writes out
     */
    public void finishAndFlush() {
        if (writeCounter > 0) {
            channel.flush();
            writeCounter = 0;
        }
    }

    @Override
    public void close() {
        finishAndFlush();
        channel.close();
        group.shutdownGracefully();
    }

    // ---------------------------------
    // Internal handler for inbound Envelopes
    // ---------------------------------
    @ChannelHandler.Sharable
    private static class ClientHandler
            extends SimpleChannelInboundHandler<BrokerApi.Envelope> {

        private final ConcurrentMap<Long, CompletableFuture<BrokerApi.Envelope>> inflight;
        private volatile BiConsumer<Long, byte[]> subscribeHandler = (seq, b) -> {
        };

        ClientHandler(ConcurrentMap<Long, CompletableFuture<BrokerApi.Envelope>> map) {
            this.inflight = map;
        }

        void setSubscribeHandler(BiConsumer<Long, byte[]> h) {
            this.subscribeHandler = h;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, BrokerApi.Envelope env) {
            if (env.hasMessageEvent()) {
                var ev = env.getMessageEvent();
                subscribeHandler.accept(ev.getOffset(), ev.getPayload().toByteArray());
            }

            long id = env.getCorrelationId();
            final CompletableFuture<BrokerApi.Envelope> f = inflight.remove(id);
            if (f != null) {
                f.complete(env);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
}
