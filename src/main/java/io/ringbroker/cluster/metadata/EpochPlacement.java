package io.ringbroker.cluster.metadata;

import lombok.Getter;
import lombok.ToString;

import java.util.List;
import java.util.Objects;

@Getter
@ToString
public final class EpochPlacement {
    private final long epoch;
    private final List<Integer> storageNodes;
    private final int[] storageNodesArray;
    private final int ackQuorum;

    public EpochPlacement(final long epoch,
                          final List<Integer> storageNodes,
                          final int ackQuorum) {
        Objects.requireNonNull(storageNodes, "storageNodes");
        if (storageNodes.isEmpty()) {
            throw new IllegalArgumentException("storageNodes must not be empty");
        }
        if (ackQuorum <= 0) {
            throw new IllegalArgumentException("ackQuorum must be > 0");
        }
        if (ackQuorum > storageNodes.size()) {
            throw new IllegalArgumentException("ackQuorum cannot exceed storage size");
        }
        this.epoch = epoch;
        this.storageNodes = List.copyOf(storageNodes);
        final int size = this.storageNodes.size();
        this.storageNodesArray = new int[size];
        for (int i = 0; i < size; i++) {
            this.storageNodesArray[i] = this.storageNodes.get(i);
        }
        this.ackQuorum = ackQuorum;
    }

    public List<Integer> getStorageNodes() {
        return storageNodes;
    }

    public int[] getStorageNodesArray() {
        return storageNodesArray;
    }

    public EpochPlacement withEpoch(final long newEpoch) {
        return new EpochPlacement(newEpoch, storageNodes, ackQuorum);
    }
}
