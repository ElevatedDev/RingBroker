package io.ringbroker.cluster.membership.replicator;

import io.ringbroker.api.BrokerApi;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Sends the publish frame to <b>all</b> replicas in parallel and waits until {@code ackQuorum}
 * have responded with a PMem‑durable ACK.  RemoteBrokerClient instances are assumed to be
 * long‑lived and TCP‑pooled.
 */
@Slf4j
@RequiredArgsConstructor
public final class FlashReplicator {

    private final int ackQuorum;
    private final Map<Integer, RemoteBrokerClient> clients;
    private final ExecutorService pool = Executors.newCachedThreadPool(r ->
            new Thread(r, "flash‑replicator"));

    public void replicate(final BrokerApi.Envelope frame,
                          final List<Integer> replicas) {
        final CountDownLatch latch = new CountDownLatch(ackQuorum);
        for (int target : replicas) {
            final RemoteBrokerClient cli = clients.get(target);
            if (cli == null) {
                log.warn("No client for PB {}", target);
                continue;
            }
            pool.submit(() -> {
                cli.sendEnvelope(frame); // new method that accepts pre‑built envelope
                latch.countDown();
            });
        }
        try {
            if (!latch.await(1, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("Durable ACK quorum not reached in time");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(ie);
        } catch (TimeoutException te) {
            throw new RuntimeException(te);
        }
    }
}
