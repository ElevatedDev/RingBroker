package io.ringbroker.cluster.membership.gossip.type;

import io.ringbroker.cluster.membership.member.Member;

import java.util.Map;

/**
 * Contract for a background membership service (SWIM, etc.).
 */
public interface GossipService extends AutoCloseable {

    /**
     * Live immutable view keyed by {@code brokerId}.
     */
    Map<Integer, Member> view();

    /**
     * Starts network I/O and schedulers.
     */
    void start();

    @Override
    void close();
}
