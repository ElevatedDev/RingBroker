package io.ringbroker.broker.ingress;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Persistent fencing state per partition/epoch.
 */
final class PartitionEpochState {
    final AtomicBoolean sealed = new AtomicBoolean(false);
    final AtomicLong lastSeq = new AtomicLong(-1L);
    volatile long sealedEndSeq = -1L;
}
