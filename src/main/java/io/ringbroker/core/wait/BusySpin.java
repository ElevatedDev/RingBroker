package io.ringbroker.core.wait;

import io.ringbroker.core.barrier.Barrier;
import io.ringbroker.core.sequence.Sequence;

/**
 * Ultra‑low latency (CPU hungry)
 */
public final class BusySpin implements WaitStrategy {
    public long await(final long s, final Sequence c, final Barrier b) {
        for (; ; ) {
            if (b.isAlerted()) throw new RuntimeException("alerted");

            final long cur = c.getValue();

            if (cur >= s) return cur;

            Thread.onSpinWait();
        }
    }

    public void signalAll() {
    }
}
