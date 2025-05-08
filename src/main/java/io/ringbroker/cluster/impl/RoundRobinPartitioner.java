package io.ringbroker.cluster.impl;

import io.ringbroker.cluster.type.Partitioner;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cycles through partitions in round-robin fashion to evenly spread load.
 */
public final class RoundRobinPartitioner implements Partitioner {
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int selectPartition(final byte[] key, final int totalPartitions) {
        final int idx = counter.getAndIncrement();

        // floorMod handles wrap and negative values safely
        return Math.floorMod(idx, totalPartitions);
    }
}
