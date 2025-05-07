package io.ringbroker.core.wait;

import io.ringbroker.core.barrier.Barrier;
import io.ringbroker.core.sequence.Sequence;

/** Wait-strategy abstraction. */
public sealed interface WaitStrategy
        permits BusySpin, Blocking, AdaptiveSpin {

    /**
     * Wait until cursor.get() ≥ seq, then return the available cursor value.
     */
    long await(long seq, Sequence cursor, Barrier barrier)
            throws InterruptedException;

    /** Wake up any threads blocked in await(). */
    void signalAll();
}
