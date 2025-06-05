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
 * Lightweight SWIM‑style gossip. Each broker sends a 48‑byte UDP ping once per second containing
 * its {@code brokerId}, {@code role} and a monotonic heartbeat counter. Peers reply with an ACK.
 * Any member missing 3 consecutive pings is marked dead.
 */
@Slf4j
public final class SwimGossipService implements GossipService {

    /**
     * Immutable view keyed by brokerId.
     */
    private final ConcurrentMap<Integer, Member> view = new ConcurrentHashMap<>();

    private final int selfId;
    private final Member selfMember;
    private final EventLoopGroup group;
    private final InetSocketAddress bind;
    private final List<InetSocketAddress> seeds;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r ->
            new Thread(r, "gossip-flusher"));
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
        // bootstrap UDP channel
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

        // local member is always alive
        view.put(selfId, selfMember);

        scheduler.scheduleAtFixedRate(this::flush, 0, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::sweep, 3, 3, TimeUnit.SECONDS);
    }

    private void flush() {
        // Build one heartbeat buffer
        final ByteBuf payload = Unpooled.buffer(4 + 1 + 8 + 1);

        payload.writeInt(selfId);                               // brokerId
        payload.writeByte(selfMember.role().ordinal());         // role
        payload.writeLong(System.currentTimeMillis());          // timestamp
        payload.writeByte(selfMember.vnodes());                 // weight / vnodes

        // 1) seeds (static discovery)
        for (final InetSocketAddress seed : seeds) {
            channel.writeAndFlush(new DatagramPacket(payload.retainedDuplicate(), seed));
        }

        // 2) live peers discovered so far
        for (final Member m : view.values()) {
            if (m.brokerId() == selfId) continue;

            // skip myself
            channel.writeAndFlush(
                    new DatagramPacket(payload.retainedDuplicate(), m.address()));
        }

        payload.release();
    }

    private void decode(final DatagramPacket pkt) {
        final ByteBuf b = pkt.content();
        final int id = b.readInt();
        final BrokerRole role = BrokerRole.values()[b.readByte()];
        final long ts = b.readLong();
        final int vnodes = b.readByte();
        final Member m = new Member(id, role,
                new InetSocketAddress(pkt.sender().getAddress(), bind.getPort()), ts, vnodes);
        view.merge(id, m, (old, neu) -> neu.timestampMillis() > old.timestampMillis() ? neu : old);
    }

    private void sweep() {
        final long now = System.currentTimeMillis();
        view.values().removeIf(mem -> (now - mem.timestampMillis()) > 3_000);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        if (channel != null) channel.close();
        group.shutdownGracefully();
    }
}