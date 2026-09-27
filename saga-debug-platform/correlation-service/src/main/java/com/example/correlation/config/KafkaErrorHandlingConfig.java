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
 * <p>The backoff below spans roughly a minute, which covers the outages that actually
 * occur here. Beyond that the record is logged in full at ERROR and the offset moves
 * on: without a dead-letter topic there is nowhere else to put it, and blocking the
 * partition forever would stall every other saga on it. The log line carries topic,
 * partition and offset so the record can be replayed by hand.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(SourceMappingRegistry mappings) {
        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxInterval(20_000L);
        backOff.setMaxElapsedTime(60_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler((record, exception) ->
                log.error("Giving up on record after retries — topic={} partition={} offset={} "
                                + "key={} value={}. This event is NOT stored; replay it manually "
                                + "if the saga matters.",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), valueForLog(record, mappings), exception),
                backOff);

        // A record that cannot be deserialized will fail identically on every attempt.
        handler.addNotRetryableExceptions(
                com.fasterxml.jackson.core.JsonProcessingException.class,
                IllegalArgumentException.class);

        return handler;
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
