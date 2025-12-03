package io.ringbroker.cluster.membership.replicator;

import io.ringbroker.api.BrokerApi;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;

/**
 * Latency-aware quorum replicator: waits on the fastest ackQuorum, updates the rest in the background.
 */
@Slf4j
public final class AdaptiveReplicator {
    private final int ackQuorum;
    private final Map<Integer, RemoteBrokerClient> clients;
    private final long timeoutMillis;

    // EWMA of each replica's latency in nanoseconds.
    private final ConcurrentMap<Integer, Double> ewmaNs = new ConcurrentHashMap<>();
    private final double alpha = 0.2;
    private final double defaultNs;

    // Executor for background replication to slow replicas.
    private final ScheduledExecutorService pool = Executors.newScheduledThreadPool(
            1, r -> {
                Thread t = new Thread(r, "adapt-quorum-bg");
                t.setDaemon(true);
                return t;
            }
    );

    public AdaptiveReplicator(final int ackQuorum,
                              final Map<Integer, RemoteBrokerClient> clients,
                              final long timeoutMillis) {
        if (ackQuorum <= 0)
            throw new IllegalArgumentException("ackQuorum must be > 0");

        this.ackQuorum = ackQuorum;
        this.clients = new HashMap<>(clients);
        this.timeoutMillis = timeoutMillis;

        this.defaultNs = TimeUnit.MILLISECONDS.toNanos(1);

        for (final Integer id : clients.keySet()) {
            ewmaNs.put(id, defaultNs);
        }
    }

    public void replicate(final BrokerApi.Envelope frame,
                          final List<Integer> replicas)
            throws InterruptedException, TimeoutException {
        replicate(frame, replicas, this.ackQuorum);
    }

    public void replicate(final BrokerApi.Envelope frame,
                          final List<Integer> replicas,
                          final int quorumOverride)
            throws InterruptedException, TimeoutException {
        if (replicas == null || replicas.isEmpty()) {
            throw new TimeoutException("No replicas provided for replication.");
        }

        final int quorum = Math.min(Math.max(1, quorumOverride), replicas.size());

        final List<Integer> sorted = new ArrayList<>(replicas);
        sorted.sort(Comparator.comparingDouble(id -> ewmaNs.getOrDefault(id, defaultNs)));

        final List<Integer> fast = sorted.subList(0, Math.min(quorum, sorted.size()));
        final List<Integer> slow = sorted.size() > quorum
                ? sorted.subList(quorum, sorted.size())
                : Collections.emptyList();

        final Map<Integer, CompletableFuture<BrokerApi.ReplicationAck>> futureMap = new HashMap<>();
        final Map<Integer, Long> startNs = new HashMap<>();

        for (final int nodeId : fast) {
            final RemoteBrokerClient client = clients.get(nodeId);
            if (client == null) {
                log.warn("No client for fast replica {}; skipping", nodeId);
                continue;
            }
            startNs.put(nodeId, System.nanoTime());
            futureMap.put(nodeId, client.sendEnvelopeWithAck(frame));
        }

        if (futureMap.size() < quorum) {
            throw new TimeoutException("Not enough valid fast replicas to meet quorum");
        }

        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        for (final Map.Entry<Integer, CompletableFuture<BrokerApi.ReplicationAck>> e : futureMap.entrySet()) {
            final int nodeId = e.getKey();
            final CompletableFuture<BrokerApi.ReplicationAck> fut = e.getValue();

            final long remainingMs = Math.max(1,
                    TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));

            final BrokerApi.ReplicationAck ack;

            try {
                ack = fut.get(remainingMs, TimeUnit.MILLISECONDS);
            } catch (final ExecutionException ex) {
                fut.cancel(true);
                throw new RuntimeException("Fast replica " + nodeId + " failed", ex.getCause());
            } catch (final TimeoutException te) {
                fut.cancel(true);
                throw new TimeoutException("Fast replica " + nodeId +
                        " did not ack within " + timeoutMillis + "ms");
            }

            if (ack.getStatus() != BrokerApi.ReplicationAck.Status.SUCCESS) {
                throw new RuntimeException("Fast replica " + nodeId +
                        " returned non-SUCCESS: " + ack.getStatus());
            }
            final long lat = System.nanoTime() - startNs.get(nodeId);

            ewmaNs.compute(nodeId, (id, prev) ->
                    prev == null ? (double) lat : (1 - alpha) * prev + alpha * lat
            );
        }

        for (final int nodeId : slow) {
            final RemoteBrokerClient client = clients.get(nodeId);
            if (client == null) continue;

            pool.submit(() -> {
                try {
                    final BrokerApi.ReplicationAck ack = client.sendEnvelopeWithAck(frame).join();
                    if (ack.getStatus() == BrokerApi.ReplicationAck.Status.SUCCESS) {
                        ewmaNs.computeIfPresent(nodeId, (id, prev) -> prev * 0.9);
                    }
                } catch (final Throwable t) {
                    log.warn("Background replication to {} failed: {}", nodeId, t.getMessage());
                }
            });
        }
    }

    public void shutdown() {
        pool.shutdownNow();
    }

    public int getAckQuorum() {
        return ackQuorum;
    }
}
