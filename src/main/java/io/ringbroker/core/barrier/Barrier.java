package io.ringbroker.core.barrier;

import io.ringbroker.core.sequence.Sequence;
import io.ringbroker.core.wait.WaitStrategy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Coordinates blocking and wake-ups for a WaitStrategy.
 */
@RequiredArgsConstructor
public final class Barrier {
    private final Sequence cursor;
    private final WaitStrategy waitStrategy;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private volatile int waiters;
    /**
     * -- GETTER --
     * Check if an alert has been raised.
     */
    @Getter
    private volatile boolean alerted;

    /**
     * Called by the RingBuffer consumer to wait for a sequence.
     */
    public long waitFor(final long seq) throws InterruptedException {
        return waitStrategy.await(seq, cursor, this);
    }

    /**
     * Called by producers to wake up any blocked consumer.
     */
    public void signal() {
        if (waiters == 0) {
            return;
        }
        lock.lock();
        try {
            if (waiters > 0) {
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
        // No need to call waitStrategy.signalAll(); wait strategies either don't block or use this Barrier.
    }

    /**
     * Mark as alerted and wake up waiters.
     */
    public void alert() {
        alerted = true;
        signal();
    }

    /**
     * Called by wait strategies to block the consumer thread until {@code seq} is published.
     * Re-checks cursor under lock to avoid missed wakeups.
     */
    public void block(final long seq) throws InterruptedException {
        lock.lock();
        try {
            if (alerted) return;
            if (cursor.getValue() >= seq) return;

            waiters++;
            try {
                while (!alerted && cursor.getValue() < seq) {
                    condition.await();
                }
            } finally {
                waiters--;
            }
        } finally {
            lock.unlock();
        }
    }
}
