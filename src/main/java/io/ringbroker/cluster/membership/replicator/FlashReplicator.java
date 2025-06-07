package io.ringbroker.cluster.membership.replicator;

import io.ringbroker.api.BrokerApi;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Sends a pre-built BrokerApi.Envelope (of kind PUBLISH) to all replicas in parallel,
 * and waits until ackQuorum of them respond with Status.SUCCESS (i.e. durable fsync).
 * <p>
 * If fewer than ackQuorum succeed within timeoutMillis, a TimeoutException is thrown.
 */
@Slf4j
public final class FlashReplicator {

    private final int ackQuorum;
    private final Map<Integer, RemoteBrokerClient> clients;
    private final long timeoutMillis;

    /**
     * Corrected thread factory for a daemon thread named "flash-replicator".
     */
    private final ExecutorService pool = Executors.newCachedThreadPool(runnable -> {
        Thread t = new Thread(runnable, "flash-replicator");
        t.setDaemon(true);
        return t;
    });

    /**
     * @param ackQuorum     how many SUCCESS acks are required before returning
     * @param clients       map from nodeId → NettyClusterClient (already connected)
     * @param timeoutMillis maximum time (in ms) to wait for ackQuorum successes
     */
    public FlashReplicator(final int ackQuorum,
                           final Map<Integer, RemoteBrokerClient> clients,
                           final long timeoutMillis) {
        if (ackQuorum <= 0) throw new IllegalArgumentException("ackQuorum must be > 0");
        this.ackQuorum = ackQuorum;
        this.clients = clients;
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * Replicates the given Envelope to all specified replica node IDs in parallel,
     * and blocks until ackQuorum of them return a SUCCESS ReplicationAck before timeout.
     *
     * @param frame    BrokerApi.Envelope (must have correlationId + Publish set)
     * @param replicas list of replica node IDs to send to
     * @throws InterruptedException if the thread is interrupted while waiting
     * @throws TimeoutException     if fewer than ackQuorum succeed within timeoutMillis
     * @throws RuntimeException     if send failures or unexpected errors occur
     */
    public void replicate(final BrokerApi.Envelope frame,
                          final List<Integer> replicas) throws InterruptedException, TimeoutException {
        if (replicas == null || replicas.isEmpty()) {
            throw new TimeoutException("No replicas provided for replication.");
        }

        final Instant start = Instant.now();
        final List<CompletableFuture<BrokerApi.ReplicationAck>> futures = new ArrayList<>(replicas.size());

        // 1) Send to each replica and collect the future
        for (final int nodeId : replicas) {
            final RemoteBrokerClient client = clients.get(nodeId);
            if (client == null) {
                log.warn("Replica node {} has no client; skipping.", nodeId);
                continue;
            }
            // Each future completes when that replica sends back its ReplicationAck
            final CompletableFuture<BrokerApi.ReplicationAck> ackFuture = client.sendEnvelopeWithAck(frame);
            futures.add(ackFuture);
        }

        if (futures.isEmpty()) {
            throw new TimeoutException("No valid replica clients found for replication.");
        }

        // 2) Wait until ackQuorum successes or timeoutMillis elapses
        int successCount = 0;
        boolean quorumReached = false;

        // Keep a mutable list of pending futures
        final List<CompletableFuture<BrokerApi.ReplicationAck>> pending = new ArrayList<>(futures);

        while (true) {
            final long elapsed = Duration.between(start, Instant.now()).toMillis();
            final long remaining = timeoutMillis - elapsed;
            if (remaining <= 0) break; // timeout

            // Build an array of still-pending futures to wait on anyOf(...)
            final CompletableFuture<?>[] toWait = pending.toArray(new CompletableFuture[0]);
            if (toWait.length == 0) break; // no one left to wait on

            final CompletableFuture<Object> any = CompletableFuture.anyOf(toWait);
            final BrokerApi.ReplicationAck ack;
            try {
                ack = (BrokerApi.ReplicationAck) any.get(remaining, TimeUnit.MILLISECONDS);
            } catch (final ExecutionException ee) {
                // One future failed. Remove it and continue.
                final Throwable cause = ee.getCause();
                log.warn("One replication attempt threw an exception: {}", cause.getMessage());
                pending.removeIf(fut -> fut.isCompletedExceptionally());
                continue;
            } catch (final TimeoutException te) {
                // No future completed within 'remaining' ms → exit loop to check if quorum met
                break;
            }

            // At this point, 'any' completed normally with a ReplicationAck
            // Find and remove the matching future from pending:
            pending.removeIf(fut -> {
                if (fut.isDone() && !fut.isCompletedExceptionally()) {
                    final BrokerApi.ReplicationAck candidate = fut.getNow(null);
                    return candidate == ack;
                }
                return false;
            });

            if (ack.getStatus() == BrokerApi.ReplicationAck.Status.SUCCESS) {
                successCount++;
                log.debug("Received SUCCESS from replica {}. Count={}/{}", ack.getReplicaNodeId(), successCount, ackQuorum);
                if (successCount >= ackQuorum) {
                    quorumReached = true;
                    break;
                }
            } else {
                log.warn("Replica {} returned non-SUCCESS status: {}",
                        ack.getReplicaNodeId(), ack.getStatus());
            }
        }

        if (!quorumReached) {
            throw new TimeoutException(
                    "Failed to reach durable ACK quorum (" + ackQuorum + ") within " +
                            timeoutMillis + "ms. Only got " + successCount + " SUCCESS acks.");
        }
    }
}
