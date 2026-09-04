package com.example.correlation.messaging;

import com.example.correlation.ingestion.EventIngestionService;
import com.example.correlation.ingestion.IngestionResult;
import com.example.correlation.ingestion.RawObservation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * The Kafka adapter.
 *
 * <p>Holds no ingestion logic: mapping, schema checks, deduplication and the accept or
 * reject decision all live in {@link EventIngestionService}, which knows nothing about
 * Kafka. All this class does is describe a {@code ConsumerRecord} as an observation and
 * translate the answer back into what the container needs to hear.
 *
 * <p>That translation is the whole reason the adapter exists. A rejection is final, so
 * the offset should advance; a transient failure must not be reported as handled, or the
 * record is skipped and lost.
 */
@Component
public class SagaEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(SagaEventConsumer.class);

    private final EventIngestionService ingestionService;

    public SagaEventConsumer(EventIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    /**
     * Subscribes by pattern, not by an enumerated list.
     *
     * <p>The topic names used to be a Java array, so carrying a new system's telemetry
     * meant editing and redeploying this class. The pattern lives in configuration and
     * Kafka re-evaluates it as topics appear, within {@code metadata.max.age.ms}.
     *
     * <p>It is not open-ended on purpose: the platform has to be told which topics carry
     * execution telemetry, because nothing in a topic name identifies it as such.
     */
    @KafkaListener(
            topicPattern = "${correlation.kafka.topic-pattern:"
                    + "order-.*|payment-.*|inventory-.*|refund-.*}",
            groupId = "correlation-engine-group")
    public void handleEvent(ConsumerRecord<String, String> record) {
        RawObservation raw = new RawObservation(
                record.topic(),
                record.value(),
                Instant.ofEpochMilli(record.timestamp()),
                // Kafka can always identify a record even when the payload cannot.
                record.topic() + "-" + record.partition() + "-" + record.offset(),
                record.key(),
                headersOf(record));

        // A thrown exception reaches the container's error handler, which retries with
        // backoff. A returned rejection does not: nothing about the record will change.
        IngestionResult result = ingestionService.ingest(raw);

        if (result.isPermanentlyRejected()) {
            log.error("Dropping record topic={} partition={} offset={}: {} ({})",
                    record.topic(), record.partition(), record.offset(),
                    result.outcome(), result.reason());
        }
    }

    /**
      * Every header, so a mapping can name the one it needs.
      *
      * <p>This replaced a method that read one hard-coded header called {@code saga-id}.
      * That name was this project's own convention, so a system carrying its correlation
      * key in a header called anything else was unreadable no matter how its payload was
      * mapped. Which header matters is a property of the source, and therefore belongs in
      * the source's mapping file.
      *
      * <p>Last value wins on a repeated header, matching {@code Headers.lastHeader}.
      */
     private static Map<String, String> headersOf(ConsumerRecord<String, String> record) {
         Map<String, String> headers = new LinkedHashMap<>();
         for (Header header : record.headers()) {
             if (header.key() == null || header.value() == null) continue;
             headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
         }
         return headers;
     }
}
