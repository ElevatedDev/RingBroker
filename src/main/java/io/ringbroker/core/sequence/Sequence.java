package io.ringbroker.core.sequence;

import lombok.Getter;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Padded, CAS-capable cursor to avoid false-sharing between threads.
 */
public final class Sequence {
    private static final VarHandle VH;

    static {
        try {
            VH = MethodHandles.lookup().findVarHandle(Sequence.class, "value", long.class);
        } catch (final ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // padding to avoid false sharing
    @SuppressWarnings("unused")
    private long p1, p2, p3, p4, p5, p6, p7;

    @Getter
    private volatile long value;

    @SuppressWarnings("unused")
    private long p8, p9, p10, p11, p12, p13, p14;

    public Sequence(final long initial) {
        // use a release write to initialize
        VH.setRelease(this, initial);
    }

    /**
     * CAS from expect → update. Returns true if successful.
     */
    public boolean cas(final long expect, final long update) {
        return VH.compareAndSet(this, expect, update);
    }

    /**
     * A *release‐ordered* write to set this sequence to the given value.
     * Used by a single “owner” (i.e. the publisher whose turn it is) to bump
     * the cursor forward once all prior slots are filled.
     */
    public void setValue(final long newValue) {
        VH.setRelease(this, newValue);
    }
}
