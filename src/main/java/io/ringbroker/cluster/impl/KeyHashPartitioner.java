package io.ringbroker.cluster.impl;

import io.ringbroker.cluster.type.Partitioner;

import java.util.Arrays;

/**
 * Hashes the message key to pick a partition,
 * so that all messages with the same key land in the same partition.
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
