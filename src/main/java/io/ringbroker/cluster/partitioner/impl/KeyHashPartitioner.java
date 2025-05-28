package io.ringbroker.cluster.partitioner.impl;

import io.ringbroker.cluster.partitioner.Partitioner;

import java.util.Arrays;

/**
 * {@code KeyHashPartitioner} is an implementation of the {@link Partitioner} interface that assigns
 * messages to partitions based on the hash of their key. This ensures that all messages with the same key
 * are consistently routed to the same partition, providing key-based partitioning semantics.
 * <p>
 * If the key is {@code null} or empty, partition 0 is selected by default.
 * The partition is determined by computing the hash code of the key (using {@link Arrays#hashCode(byte[])}),
 * and then applying {@link Math#floorMod(int, int)} to ensure a non-negative partition index within the range
 * of available partitions.
 * </p>
 */
public final class KeyHashPartitioner implements Partitioner {

    @Override
    public int selectPartition(final byte[] key, final int totalPartitions) {
        // if no key provided, default to partition 0
        final int hash = (key != null && key.length > 0)
                ? Arrays.hashCode(key)
                : 0;

        return Math.floorMod(hash, totalPartitions);
    }
}
