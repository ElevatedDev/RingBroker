package io.ringbroker.cluster.membership.replicator;

import io.ringbroker.api.BrokerApi;
import io.ringbroker.cluster.client.RemoteBrokerClient;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Latency-aware quorum replicator (failover-safe) optimized for high-throughput / low-latency.
 *
 * Fixes vs earlier perf implementation:
 *  - Treats CompletableFuture timeouts (TimeoutException) as retriable within the same replicate() call,
 *    like ERROR_REPLICA_NOT_READY, to avoid segment-creation warmup stalls wedging quorum.
 *  - Sizes completion queue to tolerate retries/bursts (avoids producer spinning if queue fills).
 */
@Slf4j
@Getter
public final class AdaptiveReplicator {

    private static final int ACK_NONE = -1;
    private static final int SUCCESS_ORDINAL = BrokerApi.ReplicationAck.Status.SUCCESS.ordinal();
    private static final int NOT_READY_ORDINAL = BrokerApi.ReplicationAck.Status.ERROR_REPLICA_NOT_READY.ordinal();

    // Retry backoff for transient states within a single replicate() call.
    private static final long RETRY_INITIAL_NS = TimeUnit.MICROSECONDS.toNanos(50);
    private static final long RETRY_TIMEOUT_INITIAL_NS = TimeUnit.MICROSECONDS.toNanos(200);
    private static final long RETRY_MAX_NS = TimeUnit.MILLISECONDS.toNanos(5);

    // If no inflight and no scheduled retries, don't fail fast; briefly park and re-check until deadline.
    private static final long NO_PROGRESS_PARK_NS = TimeUnit.MICROSECONDS.toNanos(200);

    private final int ackQuorum;
    private final Map<Integer, RemoteBrokerClient> clients; // LIVE reference
    private final long timeoutMillis;

    // EWMA latency in ns per node (integer EWMA).
    private final ConcurrentMap<Integer, AtomicLong> ewmaNs = new ConcurrentHashMap<>();
    // new = prev + ((sample - prev) >> ewmaShift); ewmaShift=3 => alpha=1/8
    private final int ewmaShift = 3;

    private final long defaultNs;
    private final long maxPenaltyNs;

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
        this.maxPenaltyNs = TimeUnit.SECONDS.toNanos(10);

        for (final Integer id : clients.keySet()) {
            ewmaNs.put(id, new AtomicLong(defaultNs));
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

    private static int clampQuorum(final int quorumOverride, final int n) {
        final int q = Math.max(1, quorumOverride);
        return Math.min(q, n);
    }

    private static int nextPow2AtLeast(final int x) {
        int v = (x <= 2) ? 2 : x;
        v--;
        v |= v >>> 1;
        v |= v >>> 2;
        v |= v >>> 4;
        v |= v >>> 8;
        v |= v >>> 16;
        v++;
        return v;
    }

    public void replicate(final BrokerApi.Envelope frame,
                          final int[] replicas,
                          final int replicaCount,
                          final int quorumOverride)
            throws InterruptedException, TimeoutException {

        if (frame == null) throw new NullPointerException("frame");
        if (replicas == null || replicaCount <= 0) throw new TimeoutException("No replicas provided");
        if (replicaCount > replicas.length) throw new IllegalArgumentException("replicaCount > replicas.length");

        final int n = replicaCount;
        final int quorum = clampQuorum(quorumOverride, n);

        // Per-call client cache (refresh from LIVE map when null).
        final RemoteBrokerClient[] clientCache = new RemoteBrokerClient[n];
        int availableAtStart = 0;
        for (int i = 0; i < n; i++) {
            final RemoteBrokerClient c = clients.get(replicas[i]);
            clientCache[i] = c;
            if (c != null) availableAtStart++;
        }
        if (availableAtStart < quorum) {
            throw new TimeoutException("Not enough replicas available to start quorum=" + quorum +
                    " (availableAtStart=" + availableAtStart + ")");
        }

        final long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);

        // attempted[i] means "do not start right now" (either in-flight OR permanently exhausted by non-retriable failure).
        final boolean[] attempted = new boolean[n];
        final boolean[] completed = new boolean[n];

        @SuppressWarnings("unchecked")
        final CompletableFuture<BrokerApi.ReplicationAck>[] inflight = new CompletableFuture[n];

        // Written by completers before publishing idx into doneQ.
        final int[] ackStatus = new int[n];
        final Throwable[] errs = new Throwable[n];
        final long[] latencyNs = new long[n];
        for (int i = 0; i < n; i++) ackStatus[i] = ACK_NONE;

        // Transient retry scheduling per idx.
        final long[] retryAfterNs = new long[n];     // 0 means "eligible now"
        final long[] retryBackoffNs = new long[n];
        for (int i = 0; i < n; i++) retryBackoffNs[i] = RETRY_INITIAL_NS;

        // IMPORTANT: completions can exceed n when we retry; give headroom to avoid offer() spinning on producers.
        final IntMpscQueue doneQ = new IntMpscQueue(nextPow2AtLeast(Math.max(2, n << 2)));

        int started = 0;
        int doneCount = 0;
        int successes = 0;

        int firstFailNode = Integer.MIN_VALUE;
        int firstFailStatus = ACK_NONE;
        Throwable firstFailErr = null;

        while (successes < quorum) {
            final long nowNs = System.nanoTime();
            if (nowNs >= deadlineNs) break;

            // Top-up: ensure successes + inflight >= quorum (no HOL blocking).
            while (successes + (started - doneCount) < quorum) {
                final long now2 = System.nanoTime();
                if (now2 >= deadlineNs) break;

                final int idx = pickBestEligibleIndex(replicas, clientCache, attempted, retryAfterNs, n, now2);
                if (idx < 0) break;

                final int nodeId = replicas[idx];
                final RemoteBrokerClient client = getClient(idx, nodeId, clientCache);
                if (client == null) {
                    // Don't mark attempted; allow late wiring within the same call.
                    retryAfterNs[idx] = 0L;
                    continue;
                }

                // startAttempt(idx)
                attempted[idx] = true;
                completed[idx] = false;
                started++;

                final long startNs = System.nanoTime();
                try {
                    final CompletableFuture<BrokerApi.ReplicationAck> f = client.sendEnvelopeWithAck(frame);
                    inflight[idx] = f;

                    f.whenComplete((ack, err) -> {
                        ackStatus[idx] = (ack == null) ? ACK_NONE : ack.getStatus().ordinal();
                        errs[idx] = err;
                        latencyNs[idx] = System.nanoTime() - startNs;
                        doneQ.offer(idx);
                    });
                } catch (final Throwable t) {
                    ackStatus[idx] = ACK_NONE;
                    errs[idx] = t;
                    latencyNs[idx] = System.nanoTime() - startNs;
                    doneQ.offer(idx);
                }
            }

            if (successes >= quorum) break;

            // If nothing is inflight, sleep until the next scheduled retry (or a short park) or deadline.
            long wakeNs = deadlineNs;
            if (started == doneCount) {
                final long nextRetry = findNextRetryNs(replicas, clientCache, attempted, retryAfterNs, n, nowNs);
                if (nextRetry == Long.MAX_VALUE) {
                    wakeNs = Math.min(wakeNs, nowNs + NO_PROGRESS_PARK_NS);
                } else {
                    wakeNs = Math.min(wakeNs, nextRetry);
                }
            }

            final int idx = doneQ.pollIntUntil(wakeNs);
            if (idx < 0) {
                // Woke up for retry/deadline; loop continues.
                continue;
            }

            if (completed[idx]) continue; // defensive
            completed[idx] = true;
            doneCount++;

            final int nodeId = replicas[idx];
            final int st = ackStatus[idx];
            final Throwable err = errs[idx];
            final Throwable rootErr = unwrap(err);

            final boolean ok = (err == null && st == SUCCESS_ORDINAL);
            if (ok) {
                successes++;
                retryBackoffNs[idx] = RETRY_INITIAL_NS;
                retryAfterNs[idx] = 0L;
                reward(nodeId, latencyNs[idx]);
                continue;
            }

            if (firstFailNode == Integer.MIN_VALUE) {
                firstFailNode = nodeId;
                firstFailStatus = st;
                firstFailErr = err;
            }

            // ERROR_REPLICA_NOT_READY is retriable within call.
            if (err == null && st == NOT_READY_ORDINAL) {
                penalizeNotReady(nodeId);

                attempted[idx] = false; // allow retry
                scheduleRetry(idx, System.nanoTime(), deadlineNs, retryAfterNs, retryBackoffNs, RETRY_INITIAL_NS);
                continue;
            }

            // TimeoutException from the per-replica CompletableFuture (often segment creation stalls) is retriable.
            if (rootErr instanceof TimeoutException) {
                penalizeNotReady(nodeId); // soft penalty; usually transient

                attempted[idx] = false; // allow retry
                final CompletableFuture<BrokerApi.ReplicationAck> f = inflight[idx];
                if (f != null) f.cancel(true);

                scheduleRetry(idx, System.nanoTime(), deadlineNs, retryAfterNs, retryBackoffNs, RETRY_TIMEOUT_INITIAL_NS);
                continue;
            }

            // Non-success, non-retriable: penalize and keep attempted[idx]=true (do not retry this idx in this call).
            penalize(nodeId);
        }

        if (successes < quorum) {
            for (int i = 0; i < n; i++) {
                final CompletableFuture<BrokerApi.ReplicationAck> f = inflight[i];
                if (f != null && !f.isDone()) f.cancel(true);
            }

            final String cause;
            if (firstFailNode == Integer.MIN_VALUE) {
                cause = "no responses";
            } else {
                final String statusStr = (firstFailStatus == ACK_NONE)
                        ? "no-ack"
                        : BrokerApi.ReplicationAck.Status.values()[firstFailStatus].name();
                cause = "node=" + firstFailNode + " status=" + statusStr +
                        (firstFailErr != null ? " err=" + firstFailErr : "");
            }

            final boolean timedOut = System.nanoTime() >= deadlineNs;
            final String prefix = timedOut ? "Quorum timed out" : "Quorum failed";
            throw new TimeoutException(prefix + ": got " + successes + "/" + quorum + " (firstFailure=" + cause + ")");
        }

        // Background replicate remaining replicas (not attempted), preserving original behavior.
        if (!closed.get()) {
            for (int i = 0; i < n; i++) {
                if (attempted[i]) continue;

                final int nodeId = replicas[i];
                final RemoteBrokerClient client = getClient(i, nodeId, clientCache);
                if (client == null) continue;

                background.execute(() -> {
                    final long startNs = System.nanoTime();
                    try {
                        final BrokerApi.ReplicationAck ack =
                                client.sendEnvelopeWithAck(frame).get(timeoutMillis, TimeUnit.MILLISECONDS);

                        if (ack != null && ack.getStatus() == BrokerApi.ReplicationAck.Status.SUCCESS) {
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

    private static void scheduleRetry(final int idx,
                                      final long nowNs,
                                      final long deadlineNs,
                                      final long[] retryAfterNs,
                                      final long[] retryBackoffNs,
                                      final long minInitialBackoffNs) {
        long b = retryBackoffNs[idx];
        if (b < minInitialBackoffNs) b = minInitialBackoffNs;

        long next = nowNs + b;
        if (next > deadlineNs) next = deadlineNs;
        retryAfterNs[idx] = next;

        final long bumped = b << 1;
        retryBackoffNs[idx] = (bumped <= 0L) ? RETRY_MAX_NS : Math.min(bumped, RETRY_MAX_NS);
    }

    private static Throwable unwrap(Throwable t) {
        while (t instanceof CompletionException || t instanceof ExecutionException) {
            final Throwable c = t.getCause();
            if (c == null) break;
            t = c;
        }
        return t;
    }

    private RemoteBrokerClient getClient(final int idx, final int nodeId, final RemoteBrokerClient[] cache) {
        RemoteBrokerClient c = cache[idx];
        if (c != null) return c;
        c = clients.get(nodeId);
        cache[idx] = c;
        return c;
    }

    private void reward(final int nodeId, final long sampleNs) {
        final AtomicLong a = ewmaNs.computeIfAbsent(nodeId, id -> new AtomicLong(defaultNs));
        long prev, next;
        do {
            prev = a.get();
            next = prev + ((sampleNs - prev) >> ewmaShift);
            if (next < defaultNs) next = defaultNs;
        } while (!a.compareAndSet(prev, next));
    }

    private void penalize(final int nodeId) {
        final AtomicLong a = ewmaNs.computeIfAbsent(nodeId, id -> new AtomicLong(defaultNs));
        long prev, next;
        do {
            prev = a.get();
            if (prev >= (maxPenaltyNs >>> 1)) next = maxPenaltyNs;
            else {
                next = prev << 1;
                if (next < defaultNs) next = defaultNs;
            }
        } while (!a.compareAndSet(prev, next));
    }

    // Softer penalty for transient states: bump EWMA by 25% to deprioritize without blacklisting.
    private void penalizeNotReady(final int nodeId) {
        final AtomicLong a = ewmaNs.computeIfAbsent(nodeId, id -> new AtomicLong(defaultNs));
        long prev, next;
        do {
            prev = a.get();
            final long bump = prev + (prev >>> 2); // *1.25
            next = Math.min(Math.max(bump, defaultNs), maxPenaltyNs);
        } while (!a.compareAndSet(prev, next));
    }

    /**
     * Pick best eligible candidate:
     * - not attempted
     * - client present (live map refresh via cache)
     * - now >= retryAfterNs[i]
     * - minimal EWMA
     */
    private int pickBestEligibleIndex(final int[] replicas,
                                      final RemoteBrokerClient[] clientCache,
                                      final boolean[] attempted,
                                      final long[] retryAfterNs,
                                      final int n,
                                      final long nowNs) {
        int bestIdx = -1;
        long bestScore = Long.MAX_VALUE;

        for (int i = 0; i < n; i++) {
            if (attempted[i]) continue;
            final long ra = retryAfterNs[i];
            if (ra != 0L && nowNs < ra) continue;

            final int nodeId = replicas[i];
            RemoteBrokerClient c = clientCache[i];
            if (c == null) {
                c = clients.get(nodeId);
                clientCache[i] = c;
            }
            if (c == null) continue;

            final AtomicLong a = ewmaNs.get(nodeId);
            final long score = (a == null) ? defaultNs : a.get();

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

    /**
     * Find earliest retryAfterNs among candidates that are not attempted and have a client.
     * Returns Long.MAX_VALUE if no scheduled retries exist.
     */
    private long findNextRetryNs(final int[] replicas,
                                 final RemoteBrokerClient[] clientCache,
                                 final boolean[] attempted,
                                 final long[] retryAfterNs,
                                 final int n,
                                 final long nowNs) {
        long best = Long.MAX_VALUE;
        for (int i = 0; i < n; i++) {
            if (attempted[i]) continue;

            final long ra = retryAfterNs[i];
            if (ra == 0L || ra <= nowNs) continue;

            final int nodeId = replicas[i];
            RemoteBrokerClient c = clientCache[i];
            if (c == null) {
                c = clients.get(nodeId);
                clientCache[i] = c;
            }
            if (c == null) continue;

            if (ra < best) best = ra;
        }
        return best;
    }

    /**
     * Multi-producer / single-consumer bounded ring queue (Vyukov MPSC) for indices.
     * sequence[] must be long (monotonic tickets). buffer[] is int for cache density.
     */
    private static final class IntMpscQueue {
        private static final VarHandle SEQ, BUF;

        static {
            SEQ = MethodHandles.arrayElementVarHandle(long[].class);
            BUF = MethodHandles.arrayElementVarHandle(int[].class);
        }

        private final int mask;
        private final int capacity;
        private final long[] sequence;
        private final int[] buffer;

        private final PaddedCounter tail = new PaddedCounter(0L);
        private final PaddedCounter head = new PaddedCounter(0L);

        private volatile Thread waiter;

        IntMpscQueue(final int capacityPow2) {
            if (Integer.bitCount(capacityPow2) != 1) throw new IllegalArgumentException("capacity must be pow2");
            this.capacity = capacityPow2;
            this.mask = capacityPow2 - 1;
            this.sequence = new long[capacityPow2];
            this.buffer = new int[capacityPow2];
            for (int i = 0; i < capacityPow2; i++) sequence[i] = i;
        }

        void offer(final int item) {
            long t;
            while (true) {
                t = tail.get();
                final int idx = (int) (t & mask);
                final long sv = (long) SEQ.getVolatile(sequence, idx);
                final long dif = sv - t;
                if (dif == 0) {
                    if (tail.compareAndSet(t, t + 1)) break;
                } else {
                    Thread.onSpinWait();
                }
            }

            final int idx = (int) (t & mask);
            BUF.setRelease(buffer, idx, item);
            SEQ.setRelease(sequence, idx, t + 1);

            final Thread w = waiter;
            if (w != null) LockSupport.unpark(w);
        }

        private int pollRaw() {
            long h;
            while (true) {
                h = head.get();
                final int idx = (int) (h & mask);
                final long sv = (long) SEQ.getVolatile(sequence, idx);
                final long dif = sv - (h + 1);
                if (dif == 0) {
                    if (head.compareAndSet(h, h + 1)) {
                        final int item = (int) BUF.getAcquire(buffer, idx);
                        BUF.setRelease(buffer, idx, 0);
                        SEQ.setRelease(sequence, idx, h + capacity);
                        return item;
                    }
                } else if (dif < 0) {
                    return Integer.MIN_VALUE; // empty
                } else {
                    Thread.onSpinWait();
                }
            }
        }

        /**
         * Poll until deadlineNs, parking when empty.
         *
         * @return index [0..] or -1 on timeout
         */
        int pollIntUntil(final long deadlineNs) throws InterruptedException {
            for (int i = 0; i < 128; i++) {
                final int v = pollRaw();
                if (v != Integer.MIN_VALUE) return v;
                if (System.nanoTime() >= deadlineNs) return -1;
                Thread.onSpinWait();
            }

            final Thread me = Thread.currentThread();
            waiter = me;
            try {
                while (true) {
                    final int v = pollRaw();
                    if (v != Integer.MIN_VALUE) return v;

                    final long remaining = deadlineNs - System.nanoTime();
                    if (remaining <= 0) return -1;

                    if (Thread.interrupted()) throw new InterruptedException();
                    LockSupport.parkNanos(this, remaining);
                }
            } finally {
                waiter = null;
            }
        }

        private static final class PaddedCounter {
            private static final VarHandle VALUE;

            static {
                try {
                    VALUE = MethodHandles.lookup().findVarHandle(PaddedCounter.class, "value", long.class);
                } catch (final ReflectiveOperationException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }

            @SuppressWarnings("unused")
            private long p1, p2, p3, p4, p5, p6, p7;
            private volatile long value;
            @SuppressWarnings("unused")
            private long q1, q2, q3, q4, q5, q6, q7;

            PaddedCounter(final long initial) {
                VALUE.setRelease(this, initial);
            }

            long get() {
                return (long) VALUE.getVolatile(this);
            }

            boolean compareAndSet(final long expect, final long update) {
                return VALUE.compareAndSet(this, expect, update);
            }
        }
    }
}
