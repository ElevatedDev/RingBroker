package io.ringbroker.cluster.type;

/**
 * Selects which partition a message should go to.
 */
public interface Partitioner {
    /**
     * @param key             the message key (may be empty or null)
     * @param totalPartitions total number of partitions configured
     * @return the partition index [0..totalPartitions)
     */
    int selectPartition(byte[] key, int totalPartitions);
}
