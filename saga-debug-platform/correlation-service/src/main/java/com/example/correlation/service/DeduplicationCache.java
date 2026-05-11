package com.example.correlation.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * In-memory deduplication cache using Caffeine.
 * TTL: 1 hour. Uses eventId as key.
 */
@Component
public class DeduplicationCache {

    private static final Logger log = LoggerFactory.getLogger(DeduplicationCache.class);

    private final Cache<String, Boolean> cache;

    public DeduplicationCache() {
        this.cache = Caffeine.newBuilder()
                .maximumSize(100_000)
                .expireAfterWrite(Duration.ofHours(1))
                .build();
        log.info("DeduplicationCache initialized: maxSize=100000, TTL=1h");
    }

    /**
     * Check if eventId has been seen before.
     * If not seen, mark as seen and return false.
     * If already seen, return true (duplicate).
     * Uses Caffeine's atomic get() to prevent race conditions between
     * concurrent Kafka consumer threads.
     */
    public boolean isDuplicate(String eventId) {
        if (eventId == null) return false;
        boolean[] wasAbsent = {false};
        cache.get(eventId, k -> {
            wasAbsent[0] = true;
            return Boolean.TRUE;
        });
        return !wasAbsent[0];
    }

    /**
     * Get approximate count of cached entries.
     */
    public long size() {
        return cache.estimatedSize();
    }
}
