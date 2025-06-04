package io.ringbroker.cluster.manager;

import io.ringbroker.cluster.client.RemoteBrokerClient;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ClusterManagerTest {
    @Test
    public void testLeaderChangesWhenNodeDown() {
        Map<Integer, RemoteBrokerClient> clients = new HashMap<>();
        ClusterManager mgr = new ClusterManager(clients, 0);

        // cluster of single node (0) only
        assertTrue(mgr.isLeader(1, 0));

        // add other nodes
        clients.put(1, null);
        clients.put(2, null);
        mgr.nodeUp(1);
        mgr.nodeUp(2);

        // leader for partition 1 should be node 0
        assertEquals(0, mgr.getLeader(1));

        // simulate node 0 down -> leader becomes 1
        mgr.nodeDown(0);
        assertEquals(1, mgr.getLeader(1));
    }
}
