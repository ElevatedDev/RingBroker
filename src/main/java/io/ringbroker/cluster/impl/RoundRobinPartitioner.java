package io.ringbroker.cluster.impl;

import io.ringbroker.cluster.type.Partitioner;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@code RoundRobinPartitioner} is an implementation of the {@link Partitioner} interface that assigns
 * messages to partitions in a round-robin manner. This approach evenly distributes messages across all
 * available partitions, regardless of the message key, to balance load.
 * <p>
 * The partition index is incremented atomically for each message, and {@link Math#floorMod(int, int)}
 * is used to ensure the index wraps around safely and remains within the valid partition range.
 * </p>
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
