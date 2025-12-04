package io.ringbroker.cluster.membership.gossip.impl;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.membership.gossip.type.GossipService;
import io.ringbroker.cluster.membership.member.Member;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Lightweight SWIM-style gossip.
 */
@Slf4j
public final class SwimGossipService implements GossipService {

    private static final BrokerRole[] ROLES = BrokerRole.values();
    private static final long MEMBER_TIMEOUT_MS = 3_000;

    private final ConcurrentMap<Integer, Member> view = new ConcurrentHashMap<>();

    private final int selfId;
    private final Member selfMember;
    private final EventLoopGroup group;
    private final InetSocketAddress bind;
    private final List<InetSocketAddress> seeds;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        final Thread t = new Thread(r, "gossip-flusher");
        t.setDaemon(true);
        return t;
    });

    private volatile Channel channel;

    public SwimGossipService(final int selfId,
                             final BrokerRole role,
                             final InetSocketAddress bind,
                             final List<InetSocketAddress> seeds) {
        this.selfId = selfId;
        this.selfMember = new Member(selfId, role, bind, System.currentTimeMillis(), 16);
        this.bind = bind;
        this.seeds = seeds;
        this.group = new NioEventLoopGroup(1);
    }

    private static byte[] encode(final Member m) {
        final ByteBuf b = Unpooled.buffer(4 + 1 + 8 + 1);
        b.writeInt(m.brokerId());
        b.writeByte(m.role().ordinal());
        b.writeLong(System.currentTimeMillis());
        b.writeByte(m.vnodes());
        return ByteBufUtil.getBytes(b);
    }

    @Override
    public Map<Integer, Member> view() {
        return Collections.unmodifiableMap(view);
    }

    @Override
    public void start() {
        try {
            channel = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_BROADCAST, false)
                    .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
                        @Override
                        protected void channelRead0(final ChannelHandlerContext ctx,
                                                    final DatagramPacket pkt) {
                            decode(pkt);
                        }
                    })
                    .bind(bind)
                    .sync()
                    .channel();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Cannot start gossip", e);
        }

        view.put(selfId, selfMember);

        scheduler.scheduleAtFixedRate(this::flush, 0, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::sweep, 3, 3, TimeUnit.SECONDS);
    }

    private void flush() {
        final ByteBuf payload = Unpooled.buffer(4 + 1 + 8 + 1);

        payload.writeInt(selfId);
        payload.writeByte(selfMember.role().ordinal());
        payload.writeLong(System.currentTimeMillis());
        payload.writeByte(selfMember.vnodes());

        for (final InetSocketAddress seed : seeds) {
            channel.writeAndFlush(new DatagramPacket(payload.retainedDuplicate(), seed));
        }

        for (final Member m : view.values()) {
            if (m.brokerId() == selfId) continue;
            channel.writeAndFlush(new DatagramPacket(payload.retainedDuplicate(), m.address()));
        }

        payload.release();
    }

    private void decode(final DatagramPacket pkt) {
        final ByteBuf b = pkt.content();
        if (b.readableBytes() < 14) return;

        final int id = b.readInt();
        final int roleOrd = b.readByte();
        final long ts = b.readLong();
        final int vnodes = b.readByte();

        if (System.currentTimeMillis() - ts > MEMBER_TIMEOUT_MS) return;

        final BrokerRole role = (roleOrd >= 0 && roleOrd < ROLES.length) ? ROLES[roleOrd] : BrokerRole.INGESTION;

        final Member m = new Member(id, role,
                new InetSocketAddress(pkt.sender().getAddress(), bind.getPort()), ts, vnodes);

        view.merge(id, m, (old, neu) -> neu.timestampMillis() > old.timestampMillis() ? neu : old);
    }

    private void sweep() {
        final long now = System.currentTimeMillis();
        view.values().removeIf(mem -> (now - mem.timestampMillis()) > MEMBER_TIMEOUT_MS);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            if (channel != null) {
                channel.close().syncUninterruptibly();
            }
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }
}
