package io.ringbroker.core.wait;

import io.ringbroker.core.barrier.Barrier;
import io.ringbroker.core.sequence.Sequence;

/**
 * Spins briefly, then blocks on the barrier condition to reduce CPU burn.
 */
public final class AdaptiveSpin implements WaitStrategy {
    private static final int SPIN_LIMIT = 1000;

    @Override
    public long await(final long seq, final Sequence cursor, final Barrier barrier)
            throws InterruptedException {
        int counter = 0;
        long available;
        while ((available = cursor.getValue()) < seq) {
            if (barrier.isAlerted()) {
                throw new RuntimeException("Consumer alerted");
            }
            if (counter < SPIN_LIMIT) {
                Thread.onSpinWait();
            } else {
                barrier.block(seq);
                counter = 0;
            }
            counter++;
        }
        return available;
    }

    @Override
    public void signalAll() {
        // no-op: Barrier.signal() handles waking parked threads
    }
}
