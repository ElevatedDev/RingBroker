package io.ringbroker.core.lsn;

public final class Lsn {
    /**
     * LSN layout: [epoch:24 bits][seq:40 bits]
     * - epoch supports ~16 million transitions.
     * - seq supports ~1.09e12 messages per epoch.
     */
    private static final long SEQ_MASK = (1L << 40) - 1;
    private static final long EPOCH_MASK = (1L << 24) - 1;

    private Lsn() {}

    public static long encode(final long epoch, final long seq) {
        if ((epoch & EPOCH_MASK) != epoch) {
            throw new IllegalArgumentException("epoch out of range (must fit 24 bits): " + epoch);
        }
        if ((seq & SEQ_MASK) != seq) {
            throw new IllegalArgumentException("seq out of range (must fit 40 bits): " + seq);
        }
        return (epoch << 40) | (seq & SEQ_MASK);
    }

    public static long epoch(final long lsn) {
        return (lsn >>> 40) & EPOCH_MASK;
    }

    public static long seq(final long lsn) {
        return lsn & SEQ_MASK;
    }
}
