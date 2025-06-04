package io.ringbroker.ledger.append;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface Appendable extends AutoCloseable {
    void append(final ByteBuffer src) throws IOException;
}