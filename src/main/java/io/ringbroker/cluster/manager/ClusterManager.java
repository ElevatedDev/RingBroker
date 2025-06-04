package io.ringbroker.cluster.manager;

import io.ringbroker.cluster.client.RemoteBrokerClient;

import java.util.*;

/**
 * Simple in-memory cluster manager that keeps track of active broker nodes and
 * determines the current leader for each partition based on a deterministic
 * ordering of node IDs. This implementation does not perform any network
 * heartbeating; nodes must be marked up/down by the application.
 */
public final class ClusterManager {
    private final Map<Integer, RemoteBrokerClient> clients;
    private final List<Integer> allNodes;
    private final Set<Integer> activeNodes = new HashSet<>();

    public ClusterManager(Map<Integer, RemoteBrokerClient> clients, int myNodeId) {
        this.clients = clients;
        this.allNodes = new ArrayList<>(clients.keySet());
        this.allNodes.add(myNodeId); // include self
        Collections.sort(this.allNodes);
        this.activeNodes.addAll(this.allNodes);
    }

    /** Mark a node as failed/inactive. */
    public synchronized void nodeDown(int nodeId) {
        activeNodes.remove(nodeId);
    }

    /** Mark a node as alive/active. */
    public synchronized void nodeUp(int nodeId) {
        if (allNodes.contains(nodeId)) {
            activeNodes.add(nodeId);
        }
    }

    /** Returns true if this node is the leader for the given partition. */
    public synchronized boolean isLeader(int partitionId, int nodeId) {
        return getLeader(partitionId) == nodeId;
    }

    /** Determine the current leader for the given partition. */
    public synchronized int getLeader(int partitionId) {
        List<Integer> ordered = activeOrdered();
        if (ordered.isEmpty()) throw new IllegalStateException("No active nodes");
        int idx = Math.floorMod(partitionId, ordered.size());
        return ordered.get(idx);
    }

    /** Lookup the client for the given node id. */
    public synchronized RemoteBrokerClient clientFor(int nodeId) {
        return clients.get(nodeId);
    }

    private List<Integer> activeOrdered() {
        List<Integer> ordered = new ArrayList<>(activeNodes);
        ordered.sort(Integer::compareTo);
        return ordered;
    }
}
