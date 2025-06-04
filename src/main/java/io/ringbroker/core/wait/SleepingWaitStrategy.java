// ── src/main/java/io/ringbroker/core/wait/SleepingWaitStrategy.java
package io.ringbroker.core.wait;

import io.ringbroker.core.barrier.Barrier;
import io.ringbroker.core.sequence.Sequence;

import java.util.concurrent.locks.LockSupport;

/**
 * Low-CPU wait strategy: spin briefly, then park for 1 µs.
 * Suitable for background flushers that are not latency-critical.
 */
public final class SleepingWaitStrategy implements WaitStrategy {

    private static final int SPINS = 64;

    /**
     * Blocks until {@code cursor} has advanced to at least {@code seq}, or
     * the thread is interrupted.
     *
     * @return the value of {@code cursor} when it satisfies the predicate.
     */
    @Override
    public long await(final long seq,
                      final Sequence cursor,
                      final Barrier barrier) throws InterruptedException {

        int spins = 0;
        long available;

        while ((available = cursor.getValue()) < seq) {
            if (++spins < SPINS) {
                Thread.onSpinWait();
            } else {
                LockSupport.parkNanos(1_000); // 1 µs
                spins = 0;
            }
            if (Thread.interrupted()) throw new InterruptedException();
        }
        return available;
    }

    /** Nothing to signal – consumer side polls. */
    @Override
    public void signalAll() { /* no-op */ }
}
