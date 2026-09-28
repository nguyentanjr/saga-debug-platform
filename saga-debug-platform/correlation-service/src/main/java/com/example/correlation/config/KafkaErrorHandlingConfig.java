package com.example.correlation.config;

import com.example.correlation.ingestion.SourceMappingRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * What the container does when the listener throws.
 *
 * <p>The listener now propagates processing failures instead of swallowing them, so
 * this decides whether that propagation is useful. The default handler retries ten
 * times with no delay and then gives up — fast enough that a database restart or a
 * brief connection-pool exhaustion burns every attempt in well under a second and the
 * event is dropped anyway.
 *
 * <p>Two kinds of failure get two policies (L1):
 * <ul>
 *   <li><b>The observer's own store is unreachable</b> ({@link #storeUnavailable}). Nothing
 *   about the record is wrong, and every other record on the partition would fail the
 *   same way, so skipping it buys no progress and loses the event. Those are retried
 *   without an end, backoff capped at 20s: the offset is not committed, Kafka keeps the
 *   record, and ingestion resumes by itself when the database returns. This used to give
 *   up after a minute, which turned any outage longer than that into silent data loss.</li>
 *   <li><b>Anything else</b> is a fault in this record's processing and may recur forever.
 *   It keeps the bounded backoff (~1 min), then is logged in full at ERROR with topic,
 *   partition and offset so it can be replayed by hand, and the offset moves on so one
 *   record cannot stall every other saga on its partition.</li>
 * </ul>
 *
 * <p>No dead-letter topic: the only Kafka the observer reads is the observed system's,
 * and creating topics or producing there is a write to their infrastructure (ADR 0001).
 * A dead-letter table in the observer's own database fails precisely in the outage that
 * matters, which the unbounded retry above already covers.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(SourceMappingRegistry mappings) {
        DefaultErrorHandler handler = new DefaultErrorHandler((record, exception) ->
                log.error("Giving up on record after retries — topic={} partition={} offset={} "
                                + "key={} value={}. This event is NOT stored; replay it manually "
                                + "if the saga matters.",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), valueForLog(record, mappings), exception),
                bounded());

        // A record that cannot be deserialized will fail identically on every attempt.
        handler.addNotRetryableExceptions(
                com.fasterxml.jackson.core.JsonProcessingException.class,
                IllegalArgumentException.class);

        handler.setBackOffFunction((record, exception) -> {
            if (storeUnavailable(exception)) {
                log.warn("Observer store unavailable; holding topic={} partition={} offset={} "
                                + "until it returns: {}",
                        record.topic(), record.partition(), record.offset(), exception.toString());
            }
            return backOffFor(exception);
        });

        return handler;
    }

    private static final long MAX_INTERVAL_MS = 20_000L;

    /** How long a record that failed this way is retried before it is given up. */
    static ExponentialBackOff backOffFor(Exception exception) {
        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxInterval(MAX_INTERVAL_MS);
        if (!storeUnavailable(exception)) backOff.setMaxElapsedTime(60_000L);
        return backOff;
    }

    private static ExponentialBackOff bounded() {
        return backOffFor(new RuntimeException());
    }

    /**
     * Whether the failure says the observer's database could not be reached or could not
     * take the write right now, as opposed to something about this record. Walks the cause
     * chain because the container wraps what the listener threw.
     */
    static boolean storeUnavailable(Throwable exception) {
        for (Throwable t = exception; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof org.springframework.dao.TransientDataAccessException
                    || t instanceof org.springframework.dao.DataAccessResourceFailureException
                    || t instanceof org.springframework.transaction.CannotCreateTransactionException
                    || t instanceof java.sql.SQLTransientException
                    || t instanceof java.sql.SQLRecoverableException) {
                return true;
            }
        }
        return false;
    }

    /**
     * The record's value as it may be written to a log: with every field its source
     * declares sensitive already replaced (L14). The log exists so the record can be replayed
     * by hand, and replay loses nothing — the observer never stores those values either.
     */
    static String valueForLog(ConsumerRecord<?, ?> record, SourceMappingRegistry mappings) {
        if (record.value() == null) return null;
        return mappings.forTopic(record.topic()).redact(String.valueOf(record.value()));
    }
}
