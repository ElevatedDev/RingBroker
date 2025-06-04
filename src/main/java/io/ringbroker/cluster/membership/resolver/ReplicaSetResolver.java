package io.ringbroker.cluster.membership.resolver;

import io.ringbroker.cluster.membership.hash.HashingProvider;
import io.ringbroker.cluster.membership.member.Member;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/** Determines which persistence brokers form the replica set for a partition. */
public final class ReplicaSetResolver {
    private final int replicationFactor;
    private final Supplier<Collection<Member>> members;

    public ReplicaSetResolver(final int replicationFactor,
                              final Supplier<Collection<Member>> members) {
        this.replicationFactor = replicationFactor;
        this.members = members;
    }

    public List<Integer> replicas(final int partitionId) {
        return HashingProvider.topN(partitionId, replicationFactor, members.get());
    }
}