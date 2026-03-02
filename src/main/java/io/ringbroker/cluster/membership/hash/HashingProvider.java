package io.ringbroker.cluster.membership.hash;

import io.ringbroker.broker.role.BrokerRole;
import io.ringbroker.cluster.membership.member.Member;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Highest‑Random‑Weight hashing, stable under membership churn.
 */
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

    /**
     * Returns the brokerId with the highest weight for this key among INGESTION brokers.
     */
    public int primary(final int key, final Collection<Member> members) {
        long best = Long.MIN_VALUE;
        int bestId = -1;
        for (final Member m : members) {
            if (m.role() != BrokerRole.INGESTION) continue;
            final long s = score(key, m.brokerId());
            if (s > best) {
                best = s;
                bestId = m.brokerId();
            }
        }
        return bestId;
    }

    /**
     * Top‑N persistence replicas for the given key.
     */
    public List<Integer> topN(final int key,
                              final int n,
                              final Collection<Member> members) {
        if (n <= 0 || members.isEmpty()) {
            return List.of();
        }

        final Member[] candidateBuf = new Member[members.size()];
        int candidateCount = 0;

        for (final Member m : members) {
            if (m.role() == BrokerRole.PERSISTENCE) {
                candidateBuf[candidateCount++] = m;
            }
        }

        if (candidateCount == 0) {
            for (final Member m : members) {
                candidateBuf[candidateCount++] = m;
            }
        }

        final int limit = Math.min(n, candidateCount);
        final int[] bestIds = new int[limit];
        final long[] bestScores = new long[limit];
        int bestCount = 0;

        for (int i = 0; i < candidateCount; i++) {
            final int brokerId = candidateBuf[i].brokerId();
            final long s = score(key, brokerId);

            int insertAt = bestCount;
            while (insertAt > 0) {
                final int prev = insertAt - 1;
                final long prevScore = bestScores[prev];
                final int prevId = bestIds[prev];

                final boolean shouldShift =
                        s > prevScore || (s == prevScore && brokerId < prevId);
                if (!shouldShift) break;
                insertAt--;
            }

            if (insertAt >= limit) {
                continue;
            }

            final int upper = Math.min(bestCount, limit - 1);
            for (int j = upper; j > insertAt; j--) {
                bestScores[j] = bestScores[j - 1];
                bestIds[j] = bestIds[j - 1];
            }

            bestScores[insertAt] = s;
            bestIds[insertAt] = brokerId;
            if (bestCount < limit) {
                bestCount++;
            }
        }

        final ArrayList<Integer> out = new ArrayList<>(bestCount);
        for (int i = 0; i < bestCount; i++) {
            out.add(bestIds[i]);
        }
        return out;
    }
}
