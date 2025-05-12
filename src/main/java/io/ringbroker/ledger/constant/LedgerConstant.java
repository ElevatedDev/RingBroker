package io.ringbroker.ledger.constant;

/**
 * Shared constants for ledger segment files.
 */
public final class LedgerConstant {
    /**
     * File extension for segment files.
     */
    public static final String SEGMENT_EXT = ".seg";

    /**
     * 0x52424B52 == 'R' 'B' 'K' 'R'
     */
    public static final int MAGIC = 0x5242_4B52;
    public static final short VERSION = 1;

    private LedgerConstant() {
        // Prevent instantiation
    }
}
