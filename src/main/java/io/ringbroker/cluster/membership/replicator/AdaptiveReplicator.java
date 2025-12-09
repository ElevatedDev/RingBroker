package io.ringbroker.cluster.membership.replicator;

import io.ringbroker.api.BrokerApi;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Latency-aware quorum replicator (failover-safe).
 *
 * Key properties:
 *  - keeps LIVE reference to clients map (supports wiring after construction)
 *  - no per-call sorting; O(n) selection for fastest candidates
 *  - waits completions in any order (no head-of-line blocking)
 *  - if a chosen replica fails/times out, automatically starts another to still reach quorum
 *
 * IMPORTANT: Hot-path overload uses primitive arrays to avoid boxing / List allocations.
 */
@Slf4j
@Getter
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

        if (replicas == null || replicas.isEmpty()) throw new TimeoutException("No replicas provided");
        final int n = replicas.size();
        final int[] arr = new int[n];
        for (int i = 0; i < n; i++) arr[i] = replicas.get(i);
        replicate(frame, arr, n, quorumOverride);
    }

    public void replicate(final BrokerApi.Envelope frame,
                          final int[] replicas,
                          final int replicaCount)
            throws InterruptedException, TimeoutException {
        replicate(frame, replicas, replicaCount, this.ackQuorum);
    }

    public void replicate(final BrokerApi.Envelope frame,
                          final int[] replicas,
                          final int replicaCount,
                          final int quorumOverride)
            throws InterruptedException, TimeoutException {

        if (replicas == null || replicaCount <= 0) throw new TimeoutException("No replicas provided");

        final int n = replicaCount;
        final int quorum = Math.min(Math.max(1, quorumOverride), n);

        final long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);

        final boolean[] attempted = new boolean[n];
        final boolean[] completed = new boolean[n];

        @SuppressWarnings("unchecked")
        final CompletableFuture<BrokerApi.ReplicationAck>[] inflight =
                new CompletableFuture[n];

        final ArrayBlockingQueue<Done> doneQ = new ArrayBlockingQueue<>(n);

        int started = 0;
        int doneCount = 0;
        int successes = 0;
        String firstFailure = null;

        // Start enough attempts to be *capable* of reaching quorum.
        while (started < quorum) {
            final int idx = pickBestAvailableIndex(replicas, n, attempted);
            if (idx < 0) {
                final long rem = deadlineNs - System.nanoTime();
                if (rem <= 0) break;
                LockSupport.parkNanos(Math.min(rem, TimeUnit.MILLISECONDS.toNanos(1)));
                continue;
            }

            attempted[idx] = true;

            final int nodeId = replicas[idx];
            final RemoteBrokerClient client = clients.get(nodeId);
            if (client == null) continue;

            final long startNs = System.nanoTime();
            final CompletableFuture<BrokerApi.ReplicationAck> f = client.sendEnvelopeWithAck(frame);
            inflight[idx] = f;
            started++;

            f.whenComplete((ack, err) -> {
                final long lat = System.nanoTime() - startNs;
                // Never block a Netty event loop thread; offer is safe for n-sized queue.
                doneQ.offer(new Done(idx, nodeId, ack, err, lat));
            });
        }

        if (started < quorum) {
            throw new TimeoutException("Not enough replicas available to start quorum=" + quorum + " (started=" + started + ")");
        }

        while (successes < quorum) {
            long remainingNs = deadlineNs - System.nanoTime();
            if (remainingNs <= 0) break;

            final Done d = doneQ.poll(remainingNs, TimeUnit.NANOSECONDS);
            if (d == null) break;

            if (!completed[d.idx]) {
                completed[d.idx] = true;
                doneCount++;
            }

            if (d.success()) {
                successes++;
                reward(d.nodeId, d.latencyNs);
            } else {
                penalize(d.nodeId);
                if (firstFailure == null) {
                    final String status = (d.ack == null)
                            ? "no-ack"
                            : d.ack.getStatus().name();
                    firstFailure = "node=" + d.nodeId + " status=" + status +
                            (d.err != null ? " err=" + d.err : "");
                }
            }

            // If impossible to reach quorum with remaining inflight, start failover attempts.
            while (successes + (started - doneCount) < quorum) {
                remainingNs = deadlineNs - System.nanoTime();
                if (remainingNs <= 0) break;

                final int idx = pickBestAvailableIndex(replicas, n, attempted);
                if (idx < 0) break;

                attempted[idx] = true;

                final int nodeId = replicas[idx];
                final RemoteBrokerClient client = clients.get(nodeId);
                if (client == null) continue;

                final long startNs = System.nanoTime();
                final CompletableFuture<BrokerApi.ReplicationAck> f = client.sendEnvelopeWithAck(frame);
                inflight[idx] = f;
                started++;

                f.whenComplete((ack, err) -> {
                    final long lat = System.nanoTime() - startNs;
                    doneQ.offer(new Done(idx, nodeId, ack, err, lat));
                });
            }

            // EARLY EXIT: all attempted completed, none left, cannot reach quorum.
            if (doneCount == started && started == n && successes < quorum) break;
        }

        if (successes < quorum) {
            for (int i = 0; i < n; i++) {
                final CompletableFuture<BrokerApi.ReplicationAck> f = inflight[i];
                if (f != null && !f.isDone()) f.cancel(true);
            }
            final String cause = (firstFailure == null) ? "no responses" : firstFailure;
            throw new TimeoutException("Quorum timed out: got " + successes + "/" + quorum + " (firstFailure=" + cause + ")");
        }

        // Background replicate remaining replicas (not attempted)
        if (!closed.get()) {
            for (int i = 0; i < n; i++) {
                if (attempted[i]) continue;

                final int nodeId = replicas[i];
                final RemoteBrokerClient client = clients.get(nodeId);
                if (client == null) continue;

                background.execute(() -> {
                    final long startNs = System.nanoTime();
                    try {
                        final BrokerApi.ReplicationAck ack =
                                client.sendEnvelopeWithAck(frame).get(timeoutMillis, TimeUnit.MILLISECONDS);
                        if (ack.getStatus() == BrokerApi.ReplicationAck.Status.SUCCESS) {
                            reward(nodeId, System.nanoTime() - startNs);
                        } else {
                            penalize(nodeId);
                        }
                    } catch (final Throwable t) {
                        penalize(nodeId);
                        log.debug("Background replication to {} failed: {}", nodeId, t.toString());
                    }
                });
            }
        }
    }

    private static final class Done {
        final int idx;
        final int nodeId;
        final BrokerApi.ReplicationAck ack;
        final Throwable err;
        final long latencyNs;

        Done(final int idx, final int nodeId, final BrokerApi.ReplicationAck ack, final Throwable err, final long latencyNs) {
            this.idx = idx;
            this.nodeId = nodeId;
            this.ack = ack;
            this.err = err;
            this.latencyNs = latencyNs;
        }

        boolean success() {
            return err == null && ack != null && ack.getStatus() == BrokerApi.ReplicationAck.Status.SUCCESS;
        }
    }

    private void reward(final int nodeId, final long latencyNs) {
        ewmaNs.compute(nodeId, (id, prev) -> {
            final double p = (prev == null) ? defaultNs : prev;
            return (1.0 - alpha) * p + alpha * (double) latencyNs;
        });
    }

    private void penalize(final int nodeId) {
        ewmaNs.compute(nodeId, (id, prev) -> {
            final double p = (prev == null) ? defaultNs : prev;
            final double bumped = Math.min(p * 2.0, (double) TimeUnit.SECONDS.toNanos(10));
            return Math.max(bumped, defaultNs);
        });
    }

    private int pickBestAvailableIndex(final int[] replicas, final int n, final boolean[] attempted) {
        int bestIdx = -1;
        double bestScore = Double.POSITIVE_INFINITY;

        for (int i = 0; i < n; i++) {
            if (attempted[i]) continue;

            final int nodeId = replicas[i];
            final RemoteBrokerClient c = clients.get(nodeId);
            if (c == null) continue;

            final double score = ewmaNs.getOrDefault(nodeId, defaultNs);
            if (score < bestScore) {
                bestScore = score;
                bestIdx = i;
            }
        }
        return bestIdx;
    }

    public void shutdown() {
        if (!closed.compareAndSet(false, true)) return;
        background.shutdownNow();
    }
}
