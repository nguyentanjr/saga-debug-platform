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
 */
public record RawObservation(
        String sourceRef,
        String payload,
        Instant transportTime,
        String fallbackEventId,
        String key,
        Map<String, String> headers) {

    public RawObservation {
        headers = headers != null ? Map.copyOf(headers) : Map.of();
    }

    public static RawObservation of(String sourceRef, String payload, Instant transportTime) {
        return new RawObservation(sourceRef, payload, transportTime, null, null, Map.of());
    }

    /** The value of one header, or null when the transport did not carry it. */
    public String header(String name) {
        return name != null ? headers.get(name) : null;
    }
}
