package io.ringbroker.cluster.membership.member;

import io.ringbroker.broker.role.BrokerRole;

import java.net.InetSocketAddress;

/**
 * Immutable membership entry, updated on each valid heartbeat.
 */
public record Member(int brokerId,
                     BrokerRole role,
                     InetSocketAddress address,
                     long timestampMillis,
                     int vnodes) {
}