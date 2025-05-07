package io.ringbroker.offset;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class InMemoryOffsetStore implements OffsetStore {
    private final ConcurrentMap<String, Long> store = new ConcurrentHashMap<>();

    @Override
    public void commit(final String topic, final String group, final int partition, final long offset) {
        final String key = topic + ":" + group + ":" + partition;
        store.put(key, offset);
    }

    @Override
    public long fetch(final String topic, final String group, final int partition) {
        final String key = topic + ":" + group + ":" + partition;
        return store.getOrDefault(key, 0L);
    }
}
