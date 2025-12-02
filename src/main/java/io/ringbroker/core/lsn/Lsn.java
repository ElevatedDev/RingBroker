package io.ringbroker.core.lsn;

public final class Lsn {
    private static final long SEQ_MASK = 0x0000_FFFF_FFFF_FFFFL;

    private Lsn() {}

    public static long encode(final long epoch, final long seq) {
        if ((epoch & 0xFFFFL) != epoch) {
            throw new IllegalArgumentException("epoch out of range (must fit 16 bits): " + epoch);
        }
        if ((seq & SEQ_MASK) != seq) {
            throw new IllegalArgumentException("seq out of range (must fit 48 bits): " + seq);
        }
        return (epoch << 48) | (seq & SEQ_MASK);
    }

    public static long epoch(final long lsn) {
        return (lsn >>> 48) & 0xFFFFL;
    }

    public static long seq(final long lsn) {
        return lsn & SEQ_MASK;
    }
}
