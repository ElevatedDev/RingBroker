package io.ringbroker.cluster.membership.replicator;

import io.ringbroker.api.BrokerApi;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Latency-aware quorum replicator.
 *
 * Optimizations:
 *  - keeps LIVE reference to clients map (supports wiring after construction)
 *  - no per-call sorting/map allocations; O(n) selection for fastest quorum
 *  - background replication for remaining replicas retained
 */
@Slf4j
public final class AdaptiveReplicator {

    private final int ackQuorum;
    private final Map<Integer, RemoteBrokerClient> clients; // LIVE reference
    private final long timeoutMillis;

    // EWMA latency (ns) per node
    private final ConcurrentMap<Integer, Double> ewmaNs = new ConcurrentHashMap<>();
    private final double alpha = 0.2;
    private final double defaultNs;

    private final ExecutorService background =
            Executors.newSingleThreadExecutor(r -> {
                final Thread t = new Thread(r, "adapt-repl-bg");
                t.setDaemon(true);
                return t;
            });

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public AdaptiveReplicator(final int ackQuorum,
                              final Map<Integer, RemoteBrokerClient> clients,
                              final long timeoutMillis) {
        if (ackQuorum <= 0) throw new IllegalArgumentException("ackQuorum must be > 0");
        this.ackQuorum = ackQuorum;
        this.clients = Objects.requireNonNull(clients, "clients");
        this.timeoutMillis = timeoutMillis;
        this.defaultNs = TimeUnit.MILLISECONDS.toNanos(1);

        // Pre-seed from whatever exists now; late-join is still handled.
        for (final Integer id : clients.keySet()) {
            ewmaNs.put(id, defaultNs);
        }
    }

    public int getAckQuorum() {
        return ackQuorum;
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
        if (replicas == null || replicas.isEmpty()) throw new TimeoutException("No replicas provided");
        final int n = replicas.size();
        final int quorum = Math.min(Math.max(1, quorumOverride), n);

        // Select fastest quorum replicas by EWMA without sorting.
        final int[] fast = new int[quorum];
        final double[] fastScore = new double[quorum];
        int fastCount = 0;

        // Remaining replicas (slow path)
        final int[] slow = new int[Math.max(0, n - quorum)];
        int slowCount = 0;

        for (int i = 0; i < n; i++) {
            final int id = replicas.get(i);
            final RemoteBrokerClient c = clients.get(id);
            if (c == null) {
                // treat as unavailable -> can only go slow list (but will be skipped later)
                if (slowCount < slow.length) slow[slowCount++] = id;
                continue;
            }

            final double score = ewmaNs.getOrDefault(id, defaultNs);

            if (fastCount < quorum) {
                fast[fastCount] = id;
                fastScore[fastCount] = score;
                fastCount++;
            } else {
                // replace worst fast if this one is better
                int worstIdx = 0;
                double worst = fastScore[0];
                for (int j = 1; j < quorum; j++) {
                    final double s = fastScore[j];
                    if (s > worst) {
                        worst = s;
                        worstIdx = j;
                    }
                }
                if (score < worst) {
                    // demote old worst to slow
                    if (slowCount < slow.length) slow[slowCount++] = fast[worstIdx];
                    fast[worstIdx] = id;
                    fastScore[worstIdx] = score;
                } else {
                    if (slowCount < slow.length) slow[slowCount++] = id;
                }
            }
        }

        if (fastCount < quorum) {
            throw new TimeoutException("Not enough available replicas to satisfy quorum=" + quorum);
        }

        // Fire fast quorum
        final CompletableFuture<BrokerApi.ReplicationAck>[] futs = new CompletableFuture[quorum];
        final long[] startNs = new long[quorum];

        for (int i = 0; i < quorum; i++) {
            final int nodeId = fast[i];
            final RemoteBrokerClient client = clients.get(nodeId);
            if (client == null) throw new TimeoutException("Fast replica missing client: " + nodeId);
            startNs[i] = System.nanoTime();
            futs[i] = client.sendEnvelopeWithAck(frame);
        }

        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);

        // Wait for all quorum acks (quorum == fastCount)
        for (int i = 0; i < quorum; i++) {
            final int nodeId = fast[i];
            final CompletableFuture<BrokerApi.ReplicationAck> fut = futs[i];

            final long remainingNs = deadline - System.nanoTime();
            if (remainingNs <= 0) {
                fut.cancel(true);
                throw new TimeoutException("Quorum timed out (node " + nodeId + ")");
            }

            final BrokerApi.ReplicationAck ack;
            try {
                ack = fut.get(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNs)), TimeUnit.MILLISECONDS);
            } catch (final ExecutionException ex) {
                fut.cancel(true);
                throw new RuntimeException("Fast replica " + nodeId + " failed", ex.getCause());
            } catch (final TimeoutException te) {
                fut.cancel(true);
                throw new TimeoutException("Fast replica " + nodeId + " timeout");
            }

            if (ack.getStatus() != BrokerApi.ReplicationAck.Status.SUCCESS) {
                throw new RuntimeException("Fast replica " + nodeId + " returned " + ack.getStatus());
            }

            final long lat = System.nanoTime() - startNs[i];
            ewmaNs.compute(nodeId, (id, prev) -> prev == null ? (double) lat : (1 - alpha) * prev + alpha * lat);
        }

        // Background replicate to remaining
        if (!closed.get() && slowCount > 0) {
            for (int i = 0; i < slowCount; i++) {
                final int nodeId = slow[i];
                final RemoteBrokerClient client = clients.get(nodeId);
                if (client == null) continue;

                background.execute(() -> {
                    try {
                        final BrokerApi.ReplicationAck ack = client.sendEnvelopeWithAck(frame).get(timeoutMillis, TimeUnit.MILLISECONDS);
                        if (ack.getStatus() == BrokerApi.ReplicationAck.Status.SUCCESS) {
                            // small reward
                            ewmaNs.computeIfPresent(nodeId, (id, prev) -> prev * 0.95);
                        }
                    } catch (final Throwable t) {
                        log.debug("Background replication to {} failed: {}", nodeId, t.toString());
                    }
                });
            }
        }
    }

    public void shutdown() {
        if (!closed.compareAndSet(false, true)) return;
        background.shutdownNow();
    }
}
