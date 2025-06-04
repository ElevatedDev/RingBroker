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
        clients.put(1, null);
        clients.put(2, null);
        ClusterManager mgr = new ClusterManager(clients, 0, 2);

        // start with only node 0 active
        mgr.nodeDown(1);
        mgr.nodeDown(2);

        assertTrue(mgr.isLeader(1, 0));

        // add other nodes
        mgr.nodeUp(1);
        mgr.nodeUp(2);

        // leader for partition 1 should now be node 1
        assertEquals(1, mgr.getLeader(1));

        // simulate node 0 down -> leader becomes 1
        mgr.nodeDown(0);
        assertEquals(1, mgr.getLeader(1));
    }

    @Test
    public void testReplicasForPartition() {
        Map<Integer, RemoteBrokerClient> clients = new HashMap<>();
        clients.put(1, null);
        clients.put(2, null);
        ClusterManager mgr = new ClusterManager(clients, 0, 2);

        mgr.nodeUp(1);
        mgr.nodeUp(2);

        var replicas = mgr.replicasFor(0);
        assertEquals(2, replicas.size());
        assertEquals(0, replicas.getFirst());
        assertEquals(1, replicas.get(1));
    }

    @Test
    public void testHeartbeatTimeoutRemovesNode() {
        Map<Integer, RemoteBrokerClient> clients = new HashMap<>();
        clients.put(1, null);
        ClusterManager mgr = new ClusterManager(clients, 0, 2, 50);

        mgr.nodeUp(1);
        mgr.handleHeartbeat(1, 0);

        mgr.checkHeartbeats(100);

        assertFalse(mgr.replicasFor(0).contains(1), "node 1 should be inactive");
    }
}
