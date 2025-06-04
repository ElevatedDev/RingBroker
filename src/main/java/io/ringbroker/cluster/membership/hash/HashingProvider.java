package io.ringbroker.cluster.membership.hash;

import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.membership.member.Member;
import lombok.experimental.UtilityClass;

import java.util.*;
import java.util.stream.Collectors;

/** Highest‑Random‑Weight hashing, stable under membership churn. */
@UtilityClass
public final class HashingProvider {

    private long score(final int key, final int brokerId) {
        long h = key * 0x9E3779B97F4A7C15L ^ brokerId;
        h ^= (h >>> 33);
        h *= 0xff51afd7ed558ccdL;
        h ^= (h >>> 33);
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= (h >>> 33);
        return h;
    }

    /** Returns the brokerId with the highest weight for this key among INGESTION brokers. */
    public int primary(final int key, final Collection<Member> members) {
        long best = Long.MIN_VALUE;
        int bestId = -1;
        for (Member m : members) {
            if (m.role() != BrokerRole.INGESTION) continue;
            long s = score(key, m.brokerId());
            if (s > best) {
                best = s;
                bestId = m.brokerId();
            }
        }
        return bestId;
    }

    /** Top‑N persistence replicas for the given key. */
    public List<Integer> topN(final int key,
                                     final int n,
                                     final Collection<Member> members) {
        return members.stream()
                .filter(m -> m.role() == BrokerRole.PERSISTENCE)
                .sorted(Comparator.comparingLong(m -> -score(key, m.brokerId())))
                .limit(n)
                .map(Member::brokerId)
                .collect(Collectors.toList());
    }
}