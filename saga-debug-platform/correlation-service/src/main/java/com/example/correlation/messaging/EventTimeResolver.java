package com.example.correlation.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Resolves the time an event actually happened, as opposed to the time the
 * transport delivered it.
 *
 * <p>Before this class existed the consumer used {@code record.timestamp()} —
 * the Kafka append time — as the event timestamp, discarding the {@code timestamp}
 * field that every {@code BaseEvent} carries. That made every duration the
 * detectors compute a transport measurement rather than an execution measurement,
 * and it made the Kafka path disagree with the admin-inject path, which has always
 * used the supplied timestamp.
 *
 * <p>The wire format of {@code BaseEvent.timestamp} depends on the serializer's
 * ObjectMapper configuration, so both renderings are accepted: an ISO-8601 string
 * and a numeric epoch value. Anything unusable falls back to transport time, and
 * the resolution source is reported so callers can record measurement provenance
 * instead of silently presenting an inferred value as an observed one.
 */
@Component
public class EventTimeResolver {

    private static final Logger log = LoggerFactory.getLogger(EventTimeResolver.class);

    /**
     * Epoch values below this are read as seconds, above as milliseconds.
     * 100_000_000_000 ms is year 5138; 100_000_000_000 s is far beyond any
     * plausible timestamp, so the split is unambiguous for real data.
     */
    private static final long EPOCH_MILLIS_THRESHOLD = 100_000_000_000L;

    public enum Source {
        /** Taken from the event's own {@code timestamp} field. */
        PAYLOAD,
        /** Payload carried no usable timestamp; transport time was used instead. */
        TRANSPORT
    }

    public record Resolved(Instant eventTime, Source source) {
        public boolean isObserved() {
            return source == Source.PAYLOAD;
        }
    }

    public Resolved resolve(JsonNode payload, Instant transportTime) {
        return resolve(payload, transportTime, "timestamp");
    }

    /**
     * @param path where this source keeps its event time, which is rarely a top-level
     *             field called {@code timestamp}
     */
    public Resolved resolve(JsonNode payload, Instant transportTime, String path) {
        Instant fromPayload = readTimestamp(payload, path);
        if (fromPayload != null) {
            return new Resolved(fromPayload, Source.PAYLOAD);
        }
        return new Resolved(transportTime, Source.TRANSPORT);
    }

    private Instant readTimestamp(JsonNode payload, String path) {
        if (payload == null || path == null || path.isBlank()) {
            return null;
        }
        JsonNode node = payload;
        for (String segment : path.split("\\.")) {
            if (node == null || !node.has(segment)) {
                return null;
            }
            node = node.get(segment);
        }
        if (node == null || node.isNull()) {
            return null;
        }

        if (node.isFloatingPointNumber()) {
            // Jackson's WRITE_DATES_AS_TIMESTAMPS renders Instant as decimal epoch
            // seconds. Reading it as a long would silently drop sub-second precision,
            // which is exactly the resolution step timings need.
            return Instant.ofEpochMilli(Math.round(node.asDouble() * 1000d));
        }

        if (node.isNumber()) {
            return fromEpoch(node.asLong());
        }

        if (node.isTextual()) {
            String text = node.asText().trim();
            if (text.isEmpty()) {
                return null;
            }
            try {
                return Instant.parse(text);
            } catch (DateTimeParseException e) {
                // Some serializers render Instant as a decimal epoch ("1716500000.123").
                try {
                    double epochSeconds = Double.parseDouble(text);
                    return Instant.ofEpochMilli(Math.round(epochSeconds * 1000d));
                } catch (NumberFormatException ignored) {
                    log.warn("Unparseable event timestamp '{}'; falling back to transport time", text);
                    return null;
                }
            }
        }

        return null;
    }

    private Instant fromEpoch(long value) {
        return value < EPOCH_MILLIS_THRESHOLD
                ? Instant.ofEpochSecond(value)
                : Instant.ofEpochMilli(value);
    }
}
