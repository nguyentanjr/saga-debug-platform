package com.example.correlation.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Remembers event ids that have already been processed, so a redelivery can be
 * skipped without a database round trip.
 *
 * <p>Checking and marking are separate on purpose. The previous single
 * {@code isDuplicate} method recorded the id as a side effect of asking about it, so
 * an event was marked seen <em>before</em> it was stored. If storing then failed, the
 * consumer swallowed the error, the offset advanced, and any redelivery was rejected
 * by this cache — the event was lost twice over, and the duplicate detector never saw
 * the real duplicates either. Marking is now the caller's explicit final step, taken
 * only once the event is durably persisted.
 *
 * <p>Best-effort by nature: entries expire, the cache is per instance, and it is not
 * a correctness boundary. The unique constraint on {@code saga_events.event_id} is.
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

    /** Whether this event was already processed successfully. Records nothing. */
    public boolean seen(String eventId) {
        return eventId != null && cache.getIfPresent(eventId) != null;
    }

    /** Records the event as processed. Call only after it is durably stored. */
    public void markProcessed(String eventId) {
        if (eventId != null) {
            cache.put(eventId, Boolean.TRUE);
        }
    }

    /** Drops a recorded id, so the event can be processed again on redelivery. */
    public void forget(String eventId) {
        if (eventId != null) {
            cache.invalidate(eventId);
        }
    }

    public long size() {
        return cache.estimatedSize();
    }
}
