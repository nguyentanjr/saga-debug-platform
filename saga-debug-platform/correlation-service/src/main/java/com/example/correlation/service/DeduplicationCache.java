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
 * <p>Best-effort by nature: entries expire, the cache is per instance — replicas do not
 * share it — and it is not a correctness boundary. The boundary is the stored-event lookup
 * by {@code (source_name, event_id)} in CorrelationService and, for two replicas racing on
 * one event, the unique constraint on that pair (V13; it was {@code event_id} alone before).
 *
 * <h2>What is remembered, and why it is not just a flag</h2>
 * Each entry records <em>which record</em> carried the event, not merely that one did.
 * The two things a duplicate can mean are told apart by exactly that: an observation
 * re-read after a restart or an offset reset arrives on the same record, while a source
 * that published an event twice produced two. When this held a boolean, everything the
 * cache caught was skipped in silence — including genuine duplicates arriving inside one
 * run — and everything it missed was reported as the observed system's fault.
 */
@Component
public class DeduplicationCache {

    private static final Logger log = LoggerFactory.getLogger(DeduplicationCache.class);

    /**
     * A transport reference is optional, and a cache cannot hold a null value, so an
     * event processed without one is recorded under this instead. It never equals a real
     * reference, so it can only ever mean "seen, on a record this cache cannot name".
     */
    private static final String NO_TRANSPORT_REF = "";

    /** Event id to the transport reference of the record that carried it. */
    private final Cache<String, String> cache;

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

    /**
     * Whether this event was already processed, and demonstrably from a different record.
     *
     * <p>True only when both references are real and they differ — which is a source that
     * published one event twice, and the one case the fast path must not swallow. An
     * entry recorded without a reference, or a lookup made without one, answers false:
     * not knowing that two records differ is not the same as knowing they do, and the
     * caller keeps its previous behaviour rather than guessing.
     */
    public boolean seenOnDifferentRecord(String eventId, String transportRef) {
        if (eventId == null || transportRef == null || transportRef.isBlank()) {
            return false;
        }
        String recorded = cache.getIfPresent(eventId);
        return recorded != null && !NO_TRANSPORT_REF.equals(recorded) && !recorded.equals(transportRef);
    }

    /** Records the event as processed. Call only after it is durably stored. */
    public void markProcessed(String eventId) {
        markProcessed(eventId, null);
    }

    /**
     * Records the event as processed, and the record it arrived on.
     *
     * <p>Call only once the event is durably stored; see the note on separating checking
     * from marking above.
     */
    public void markProcessed(String eventId, String transportRef) {
        if (eventId != null) {
            cache.put(eventId, transportRef != null && !transportRef.isBlank()
                    ? transportRef
                    : NO_TRANSPORT_REF);
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
