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
     */
    public boolean isDuplicate(String eventId) {
        if (eventId == null) return false;
        Boolean existing = cache.getIfPresent(eventId);
        if (existing != null) {
            return true;
        }
        cache.put(eventId, Boolean.TRUE);
        return false;
    }

    /**
     * Get approximate count of cached entries.
     */
    public long size() {
        return cache.estimatedSize();
    }
}
