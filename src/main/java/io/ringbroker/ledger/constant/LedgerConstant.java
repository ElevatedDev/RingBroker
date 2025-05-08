package io.ringbroker.ledger.constant;

public final class LedgerConstant {
    public static final String SEGMENT_EXT = ".seg";
    public static final int MAGIC = 0x5242_4B52;   // "RBKR"
    public static final short VERSION = 1;

    private LedgerConstant() {
    }
}