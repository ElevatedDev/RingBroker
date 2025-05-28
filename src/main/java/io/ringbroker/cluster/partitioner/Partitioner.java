package io.ringbroker.cluster.partitioner;

/**
 * The {@code Partitioner} interface defines a strategy for determining which partition a message should be routed to
 * in a distributed messaging or data storage system. Implementations of this interface provide different partitioning
 * algorithms, such as key-based hashing or round-robin assignment, to distribute messages across multiple partitions.
 * <p>
 * Partitioning is essential for scalability and parallelism, ensuring that messages are evenly or logically distributed
 * according to the application's requirements.
 * </p>
 */
public interface Partitioner {
    /**
     * Selects the partition index for a message based on its key and the total number of partitions.
     *
     * @param key             the message key (may be empty or null)
     * @param totalPartitions total number of partitions configured
     * @return the partition index in the range [0..totalPartitions)
     */
    int selectPartition(byte[] key, int totalPartitions);
}
