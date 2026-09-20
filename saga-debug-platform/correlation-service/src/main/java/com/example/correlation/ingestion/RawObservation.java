package com.example.correlation.ingestion;

import java.time.Instant;
import java.util.Map;

/**
 * One observation as it arrived from somewhere, before anything has been read out of it.
 *
 * <p>Deliberately says nothing about how it got here. The ingestion logic used to live
 * inside the Kafka listener and take a {@code ConsumerRecord}, so observing a system that
 * publishes over anything else — AMQP, an HTTP callback, a log shipper — meant duplicating
 * mapping, validation, deduplication and error handling in a second place, or not
 * supporting it at all.
 *
 * <h2>Why the transport envelope is carried, not just the body</h2>
 * An earlier version passed only the payload plus a single {@code correlationHint}, on the
 * unstated assumption that everything worth reading lives inside the document. Real
 * systems break that assumption routinely. A producer using the transactional outbox
 * pattern with Debezium's {@code EventRouter} publishes the business payload as the
 * message body and puts the identity, the event type and the aggregate id in the record
 * <em>key</em> and in <em>headers</em> — so the body may be as thin as
 * {@code {"status":"BOOKED"}} while every field this platform needs sits outside it.
 *
 * <p>Carrying the key and the headers lets a source mapping say where a field lives
 * instead of the platform assuming. It also replaced a hard-coded fallback that read one
 * particular header named {@code saga-id}: that rescue is now something a mapping
 * declares, which works for any header name rather than the one this project happened to
 * choose.
 *
 * @param sourceRef      what the source mappings match on: a topic name, a queue name, or
 *                       the source id an HTTP caller posts under
 * @param payload        the raw document, untouched
 * @param transportTime  when the transport says it arrived; a fallback for event time
 * @param fallbackEventId identity to use when neither the payload nor a mapped header
 *                       carries one — a Kafka partition/offset, say. Null when the
 *                       transport offers nothing, in which case an observation without an
 *                       id cannot be deduplicated
 * @param key            the transport's own key for this record, or null. Conventionally
 *                       the correlation key, which is why a mapping can name it directly
 * @param headers        the transport's headers, never null. Empty for a transport that
 *                       has none
 * @param origin         which declared transport this arrived on -- a Kafka cluster name,
 *                       say -- or null when the deployment reads only one. A topic name is
 *                       unique within a cluster and nowhere else: two clusters routinely
 *                       both carry {@code order-created}, and reading them both while
 *                       matching on the topic alone would file two systems' runs under one
 *                       identity, which is exactly what invariant I4 forbids. Carrying it
 *                       lets a source mapping say which cluster it speaks for
 */
public record RawObservation(
        String sourceRef,
        String payload,
        Instant transportTime,
        String fallbackEventId,
        String key,
        Map<String, String> headers,
        String origin) {

    public RawObservation {
        headers = headers != null ? Map.copyOf(headers) : Map.of();
    }

    /**
     * An observation from a deployment that reads one transport, where nothing needs to be
     * said about which. Every existing caller means this.
     */
    public RawObservation(String sourceRef, String payload, Instant transportTime,
                          String fallbackEventId, String key, Map<String, String> headers) {
        this(sourceRef, payload, transportTime, fallbackEventId, key, headers, null);
    }

    public static RawObservation of(String sourceRef, String payload, Instant transportTime) {
        return new RawObservation(sourceRef, payload, transportTime, null, null, Map.of(), null);
    }

    /**
     * The value of one header, or null when the transport did not carry it.
     *
     * <p>Exact match first, then case-insensitive. Kafka header names are case-sensitive
     * and HTTP's are not, so a mapping written as {@code header:eventType} has to keep
     * meaning the same thing whichever transport delivered the observation — otherwise
     * the mapping file becomes transport-specific, which is the one thing
     * {@link RawObservation} exists to prevent.
     */
    public String header(String name) {
        if (name == null) {
            return null;
        }
        String exact = headers.get(name);
        if (exact != null) {
            return exact;
        }
        for (Map.Entry<String, String> header : headers.entrySet()) {
            if (header.getKey().equalsIgnoreCase(name)) {
                return header.getValue();
            }
        }
        return null;
    }
}
