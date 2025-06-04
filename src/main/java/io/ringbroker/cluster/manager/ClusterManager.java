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
    private final Map<Integer, Long> lastHeartbeat = new HashMap<>();
    private final int replicationFactor;
    private final long heartbeatTimeoutMillis;

    public ClusterManager(Map<Integer, RemoteBrokerClient> clients,
                          int myNodeId,
                          int replicationFactor) {
        this(clients, myNodeId, replicationFactor, 10_000);
    }

    public ClusterManager(Map<Integer, RemoteBrokerClient> clients,
                          int myNodeId,
                          int replicationFactor,
                          long heartbeatTimeoutMillis) {
        this.clients = clients;
        this.allNodes = new ArrayList<>(clients.keySet());
        this.allNodes.add(myNodeId); // include self
        Collections.sort(this.allNodes);
        this.activeNodes.addAll(this.allNodes);
        this.replicationFactor = replicationFactor;
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
        long now = System.currentTimeMillis();
        for (Integer n : this.allNodes) {
            lastHeartbeat.put(n, now);
        }
    }

    /** Mark a node as failed/inactive. */
    public synchronized void nodeDown(int nodeId) {
        activeNodes.remove(nodeId);
    }

    /** Mark a node as alive/active. */
    public synchronized void nodeUp(int nodeId) {
        if (allNodes.contains(nodeId)) {
            activeNodes.add(nodeId);
            lastHeartbeat.put(nodeId, System.currentTimeMillis());
        }
    }

    /** Record a heartbeat from the given node. */
    public synchronized void handleHeartbeat(int nodeId) {
        handleHeartbeat(nodeId, System.currentTimeMillis());
    }

    synchronized void handleHeartbeat(int nodeId, long now) {
        if (allNodes.contains(nodeId)) {
            activeNodes.add(nodeId);
            lastHeartbeat.put(nodeId, now);
        }
    }

    /** Remove nodes whose heartbeat timed out. */
    public synchronized void checkHeartbeats() {
        checkHeartbeats(System.currentTimeMillis());
    }

    synchronized void checkHeartbeats(long now) {
        Iterator<Integer> it = activeNodes.iterator();
        while (it.hasNext()) {
            int n = it.next();
            long last = lastHeartbeat.getOrDefault(n, 0L);
            if (now - last > heartbeatTimeoutMillis) {
                it.remove();
            }
        }
    }

    /** Returns true if this node is the leader for the given partition. */
    public synchronized boolean isLeader(int partitionId, int nodeId) {
        return getLeader(partitionId) == nodeId;
    }

    /** Determine the current leader for the given partition. */
    public synchronized int getLeader(int partitionId) {
        checkHeartbeats();
        return replicasFor(partitionId).getFirst();
    }

    /** Return the list of replica node ids for a partition (leader first). */
    public synchronized List<Integer> replicasFor(int partitionId) {
        checkHeartbeats();
        List<Integer> replicas = new ArrayList<>();
        int rf = Math.min(replicationFactor, allNodes.size());
        int base = Math.floorMod(partitionId, allNodes.size());

        int i = 0;
        while (replicas.size() < rf && i < allNodes.size()) {
            int candidate = allNodes.get((base + i) % allNodes.size());
            if (activeNodes.contains(candidate)) {
                replicas.add(candidate);
            }
            i++;
        }

        if (replicas.isEmpty()) {
            throw new IllegalStateException("No active nodes");
        }

        return replicas;
    }

    /** Follower replicas (excludes the leader). */
    public synchronized List<Integer> followersFor(int partitionId) {
        List<Integer> replicas = replicasFor(partitionId);
        if (!replicas.isEmpty()) replicas.removeFirst();
        return replicas;
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
