package io.ringbroker.cluster.metadata;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class EpochPlacementTest {

    @Test
    void throwsWhenAckQuorumExceedsStorage() {
        assertThrows(IllegalArgumentException.class,
                () -> new EpochPlacement(0L, List.of(1), 2),
                "ackQuorum larger than storage nodes should be rejected");
    }

    @Test
    void exposesDefensiveCopies() {
        final List<Integer> nodes = List.of(1, 2, 3);
        final EpochPlacement placement = new EpochPlacement(7L, nodes, 2);

        assertEquals(nodes, placement.getStorageNodes(), "storage nodes should match constructor input");
        assertArrayEquals(new int[]{1, 2, 3}, placement.getStorageNodesArray(), "array view should mirror list");

        // ensure consumers cannot mutate internal list
        assertThrows(UnsupportedOperationException.class, () -> placement.getStorageNodes().add(99));
    }
}
