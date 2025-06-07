package io.ringbroker.core.wait;

import io.ringbroker.core.barrier.Barrier;
import io.ringbroker.core.sequence.Sequence;

/**
 * Balanced wait strategy that blocks the consumer thread when idle to reduce CPU usage.
 */
public final class Blocking implements WaitStrategy {

    @Override
    public long await(final long seq, final Sequence cursor, final Barrier barrier) throws InterruptedException {
        long available;
        // First, quick check without blocking:
        if ((available = cursor.getValue()) >= seq) {
            return available;
        }
        // Otherwise, wait until notified of new data
        while ((available = cursor.getValue()) < seq) {
            if (barrier.isAlerted()) {
                throw new RuntimeException("Consumer alerted");
            }
            barrier.block(); // wait on the barrier's condition
        }
        return available;
    }

    @Override
    public void signalAll() {
        // No-op: barrier.signal() is used to wake up the consumer
    }
}
