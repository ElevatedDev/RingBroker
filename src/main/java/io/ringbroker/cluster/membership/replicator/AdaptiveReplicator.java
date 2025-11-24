package io.ringbroker.cluster.membership.replicator;

import io.ringbroker.api.BrokerApi;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;

/**
 * Like FlashReplicator, but picks the fastest ackQuorum replicas based on EWMA of past latencies.
 * Slow replicas are still updated asynchronously after the quorum returns, for eventual durability.
 */
@Slf4j
public final class AdaptiveReplicator {
    private final int ackQuorum;
    private final Map<Integer, RemoteBrokerClient> clients;
    private final long timeoutMillis;

    /**
     * EWMA of each replica’s latency in nanoseconds.
     */
    private final ConcurrentMap<Integer, Double> ewmaNs = new ConcurrentHashMap<>();
    private final double alpha = 0.2;

    /**
     * Executor for background replication to “slow” replicas.
     */
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

        // initialize EWMA with a default (e.g. 1ms)
        final double defaultNs = TimeUnit.MILLISECONDS.toNanos(1);
        for (final Integer id : clients.keySet()) {
            ewmaNs.put(id, defaultNs);
        }
    }

    /**
     * Replicates the given frame to all replicas, but only **waits** on the fastest {@code ackQuorum}.
     * Other replicas are updated **asynchronously** after quorum is reached.
     *
     * @param frame    BrokerApi.Envelope (must have correlationId + Publish set)
     * @param replicas list of replica node IDs to send to
     * @throws InterruptedException if interrupted while waiting
     * @throws TimeoutException     if the fastest ackQuorum don’t all succeed within timeoutMillis
     * @throws RuntimeException     on unexpected errors
     */
    public void replicate(final BrokerApi.Envelope frame,
                          final List<Integer> replicas)
            throws InterruptedException, TimeoutException {
        if (replicas == null || replicas.isEmpty()) {
            throw new TimeoutException("No replicas provided for replication.");
        }

        // 1) Sort replicas by their EWMA latency (ascending)
        final List<Integer> sorted = new ArrayList<>(replicas);
        sorted.sort(Comparator.comparingDouble(ewmaNs::get));

        // 2) Split into the “fast quorum” and the rest
        final List<Integer> fast = sorted.subList(0, Math.min(ackQuorum, sorted.size()));
        final List<Integer> slow = sorted.size() > ackQuorum
                ? sorted.subList(ackQuorum, sorted.size())
                : Collections.emptyList();

        // 3) Fire off sends to the fast set and measure start times
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

        if (futureMap.size() < ackQuorum) {
            throw new TimeoutException("Not enough valid fast replicas to meet quorum");
        }

        // 4) Wait for all fast futures, with per-call timeout
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
                // IMPORTANT: Cancel the future to trigger map cleanup in NettyClusterClient
                fut.cancel(true);
                throw new RuntimeException("Fast replica " + nodeId + " failed", ex.getCause());
            } catch (final TimeoutException te) {
                // IMPORTANT: Cancel the future to trigger map cleanup in NettyClusterClient
                fut.cancel(true);
                throw new TimeoutException("Fast replica " + nodeId +
                        " did not ack within " + timeoutMillis + "ms");
            }

            if (ack.getStatus() != BrokerApi.ReplicationAck.Status.SUCCESS) {
                throw new RuntimeException("Fast replica " + nodeId +
                        " returned non-SUCCESS: " + ack.getStatus());
            }
            // 5) Update EWMA
            final long lat = System.nanoTime() - startNs.get(nodeId);

            ewmaNs.compute(nodeId, (id, prev) ->
                    (1 - alpha) * prev + alpha * lat
            );
        }

        // 6) Quorum reached—now fire-and-forget to the slow replicas
        for (final int nodeId : slow) {
            final RemoteBrokerClient client = clients.get(nodeId);
            if (client == null) continue;

            pool.submit(() -> {
                try {
                    // We join() here because we are in a background thread anyway
                    final BrokerApi.ReplicationAck ack = client.sendEnvelopeWithAck(frame).join();
                    if (ack.getStatus() == BrokerApi.ReplicationAck.Status.SUCCESS) {
                        // warm up their EWMA too, nudge downward
                        ewmaNs.computeIfPresent(nodeId, (id, prev) -> prev * 0.9);
                    }
                } catch (final Throwable t) {
                    log.warn("Background replication to {} failed: {}", nodeId, t.getMessage());
                }
            });
        }
    }

    /**
     * Shutdown background tasks (call on broker shutdown).
     */
    public void shutdown() {
        pool.shutdownNow();
    }
}