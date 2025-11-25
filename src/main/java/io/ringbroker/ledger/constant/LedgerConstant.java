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
     * File extension for dense offset index files.
     */
    public static final String INDEX_EXT = ".idx";

    /**
     * 0x52424B52 == 'R' 'B' 'K' 'R'
     */
    public static final int MAGIC = 0x5242_4B52;
    public static final short VERSION = 1;

    /**
     * 0x52424958 == 'R' 'B' 'I' 'X'
     */
    public static final int INDEX_MAGIC = 0x5242_4958;
    public static final short INDEX_VERSION = 1;

    private LedgerConstant() {
        // Prevent instantiation
    }
}
