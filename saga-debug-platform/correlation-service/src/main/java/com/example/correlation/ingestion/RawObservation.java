package com.example.correlation.ingestion;

import java.time.Instant;

/**
 * One observation as it arrived from somewhere, before anything has been read out of it.
 *
 * <p>Deliberately says nothing about how it got here. The ingestion logic used to live
 * inside the Kafka listener and take a {@code ConsumerRecord}, so observing a system that
 * publishes over anything else — AMQP, an HTTP callback, a log shipper — meant duplicating
 * mapping, validation, deduplication and error handling in a second place, or not
 * supporting it at all.
 *
 * @param sourceRef      what the source mappings match on: a topic name, a queue name, or
 *                       the source id an HTTP caller posts under
 * @param payload        the raw document, untouched
 * @param transportTime  when the transport says it arrived; a fallback for event time
 * @param fallbackEventId identity to use when the payload carries none — a Kafka
 *                       partition/offset, say. Null when the transport offers nothing,
 *                       in which case a payload without an id cannot be deduplicated
 * @param correlationHint a correlation key carried outside the payload, such as a Kafka
 *                        header. Consulted only when the mapped field is absent
 */
public record RawObservation(
        String sourceRef,
        String payload,
        Instant transportTime,
        String fallbackEventId,
        String correlationHint) {

    public static RawObservation of(String sourceRef, String payload, Instant transportTime) {
        return new RawObservation(sourceRef, payload, transportTime, null, null);
    }
}
