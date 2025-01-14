package com.jackwaudby.ldbcimplementations;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
final public class BulkLogger {
    private static final int LOG_THRESHOLD = 1000;

    private final Map<String, Integer> elementsToAdd = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> elementsAdded = new ConcurrentHashMap<>();

    public void registerItemsToAdd(@NonNull String key, int itemsToAdd) {
        elementsToAdd.put(key, itemsToAdd);
    }

    public void registerItem(@NonNull String key) {
        final AtomicInteger added = elementsAdded.compute(key, (k, v) -> {
            if (v == null) {
                return new AtomicInteger(1);
            }

            v.incrementAndGet();
            return v;
        });

        log(key, added);
    }

    public void log() {
        elementsAdded.forEach(this::log);
    }

    private void log(@NonNull String key, @NonNull AtomicInteger added) {
        if (added.get() % LOG_THRESHOLD == 0) {
            log.info("Added for {}: {}/{}", key, added, elementsToAdd.get(key));
        }
    }
}
