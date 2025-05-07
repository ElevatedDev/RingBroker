package io.ringbroker.offset;

/** Simple in-memory committed-offset store for consumer groups. */
public interface OffsetStore {
    void commit(String topic, String group, int partition, long offset);
    long fetch(String topic, String group, int partition);
}

