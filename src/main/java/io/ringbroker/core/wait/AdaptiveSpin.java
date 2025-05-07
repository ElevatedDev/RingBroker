package io.ringbroker.core.wait;

import io.ringbroker.core.barrier.Barrier;
import io.ringbroker.core.sequence.Sequence;

/**
 * Spins briefly, then blocks on the Barrier’s condition to reduce CPU burn.
 */
public final class AdaptiveSpin implements WaitStrategy {
    private static final int SPIN_LIMIT = 100;

    @Override
    public long await(final long seq, final Sequence cursor, final Barrier barrier)
            throws InterruptedException {
        int counter = 0;
        long available;
        while ((available = cursor.getValue()) < seq) {
            if (counter < SPIN_LIMIT) {
                Thread.onSpinWait();
            } else {
                barrier.block();    // park on barrier’s condition
                counter = 0;        // reset spinning after wake
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
