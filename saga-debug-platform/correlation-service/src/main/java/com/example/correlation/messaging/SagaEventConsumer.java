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
     * One record from one cluster.
     *
     * <p>Which topics are subscribed to, and on how many clusters, is no longer decided
     * here: {@link KafkaClusterListeners} builds a container per declared cluster and calls
     * this with the cluster's name. The subscription is still by pattern rather than an
     * enumerated list, and still not open-ended — the platform has to be told which topics
     * carry execution telemetry, because nothing in a topic name says so.
     *
     * @param origin the declared name of the cluster this arrived on, or null in a
     *               deployment that reads only one. It travels with the observation
     *               because a topic name is unique within a cluster and nowhere else.
     */
    public void handle(ConsumerRecord<String, String> record, String origin) {
        RawObservation raw = new RawObservation(
                record.topic(),
                record.value(),
                Instant.ofEpochMilli(record.timestamp()),
                // Kafka can always identify a record even when the payload cannot.
                record.topic() + "-" + record.partition() + "-" + record.offset(),
                record.key(),
                headersOf(record),
                origin);

        // A thrown exception reaches the container's error handler, which retries with
        // backoff. A returned rejection does not: nothing about the record will change.
        IngestionResult result = ingestionService.ingest(raw);

        if (result.isPermanentlyRejected()) {
            log.error("Dropping record cluster={} topic={} partition={} offset={}: {} ({})",
                    origin != null ? origin : "default", record.topic(), record.partition(),
                    record.offset(), result.outcome(), result.reason());
        }
    }

    /** The single-cluster call, where there is nothing to say about where a record came from. */
    public void handleEvent(ConsumerRecord<String, String> record) {
        handle(record, null);
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
