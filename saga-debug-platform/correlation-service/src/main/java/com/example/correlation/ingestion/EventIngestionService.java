package com.example.correlation.ingestion;

import com.example.correlation.domain.CorrelationBasis;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;

/**
 * Turns an observation from any source into a correlated event.
 *
 * <p>The single place where mapping, identity, deduplication and the decision to accept or
 * reject live. Transports are adapters around it and hold no ingestion logic of their own,
 * so adding one is writing an adapter rather than reimplementing this.
 *
 * <p>The distinction that matters to every caller: a rejection is returned, a transient
 * failure is thrown. Returning normally on a database outage is what used to lose events —
 * the transport read it as success and moved its cursor past them.
 *
 * <h2>Where identity is decided</h2>
 * Everything downstream — every detector, every rule, the whole dashboard — reasons about
 * sequences of observations. This method decides which observations form a sequence, so it
 * is the one place a mistake becomes a fabricated execution rather than a visible error.
 * Two things follow from that, and both are enforced here rather than by convention:
 *
 * <ul>
 *   <li>Identity is namespaced by source. See {@link ExecutionKey}.</li>
 *   <li>Correlation follows a ladder of decreasing directness and stops rather than
 *       guessing. See {@link CorrelationBasis}.</li>
 * </ul>
 */
@Service
public class EventIngestionService {

    private static final Logger log = LoggerFactory.getLogger(EventIngestionService.class);

    private final CorrelationService correlationService;
    private final ObjectMapper objectMapper;
    private final SourceCompletenessCheck completeness;
    private final DeduplicationCache deduplicationCache;
    private final EventTimeResolver eventTimeResolver;
    private final SourceMappingRegistry sourceMappings;
    private final ExecutionAliasResolver aliasResolver;

    /**
     * The transaction the identity decision and the write share.
     *
     * <p>Explicit rather than {@code @Transactional} on this method for two reasons. The
     * deduplication mark has to happen strictly after the commit, which an annotation on
     * the whole method cannot express; and a self-invoked annotated method acquires no
     * proxy, so the boundary would silently not exist.
     */
    private final TransactionTemplate transactions;

    public EventIngestionService(CorrelationService correlationService,
            ObjectMapper objectMapper,
            SourceCompletenessCheck completeness,
            DeduplicationCache deduplicationCache,
            EventTimeResolver eventTimeResolver,
            SourceMappingRegistry sourceMappings,
            ExecutionAliasResolver aliasResolver,
            PlatformTransactionManager transactionManager) {
        this.correlationService = correlationService;
        this.objectMapper = objectMapper;
        this.completeness = completeness;
        this.deduplicationCache = deduplicationCache;
        this.eventTimeResolver = eventTimeResolver;
        this.sourceMappings = sourceMappings;
        this.aliasResolver = aliasResolver;
        this.transactions = new TransactionTemplate(transactionManager);
    }

    /**
     * @throws RuntimeException when storing failed for a reason that may not recur; the
     *                          observation is deliberately left unmarked so the transport
     *                          can present it again
     */
    public IngestionResult ingest(RawObservation raw) {
        SourceMapping mapping = sourceMappings.forTopic(raw.origin(), raw.sourceRef());

        // The observed SYSTEM, which is not the same thing as the mapping that read this
        // observation. One system routinely needs several mappings -- a saga publishing on
        // four topics whose payloads share a field name with different meanings cannot be
        // read by one file -- and when the namespace was taken from the mapping's own name
        // each of those mappings became its own id space, so one run was filed as several
        // half-finished executions. See SourceMapping#systemName.
        String systemName = mapping.systemName() != null
                ? mapping.systemName()
                : ExecutionKey.DEFAULT_SOURCE;

        JsonNode payload;
        try {
            payload = objectMapper.readTree(raw.payload());
        } catch (Exception e) {
            log.error("Discarding unreadable observation from {}: {}",
                    raw.sourceRef(), e.getMessage());
            return IngestionResult.unreadable(e.getMessage());
        }

        // Reported against the mapping's own name rather than the system's: several
        // mappings may read one system, and the reader has to know which file to edit.
        completeness.report(completeness.gaps(payload, mapping, raw),
                mapping.getName(), raw.sourceRef());

        String eventId = mapping.eventId(payload, raw).orElse(raw.fallbackEventId());
        if (eventId == null) {
            String reason = "no value at '"
                    + SourceMapping.describe(mapping.getFields().getEventId())
                    + "' and the transport offered no identity";
            log.error("Cannot identify observation from {}: {}", raw.sourceRef(), reason);
            return IngestionResult.notCorrelatable(reason);
        }

        // Namespaced, because an event id is unique only within the system that issued it.
        // Two observed systems both numbering their events from one used to mean the second
        // system's events were silently discarded as duplicates of the first system's.
        String dedupKey = systemName + ExecutionKey.SEPARATOR + eventId;
        // The fast path skips a redelivery of the SAME record without a database round
        // trip, as before. What it no longer skips is the same event id arriving on a
        // demonstrably different record: that is a source publishing one event twice, and
        // swallowing it here is why a genuine duplicate inside a single run was never
        // reported while the observer's own re-reads were.
        if (deduplicationCache.seen(dedupKey)
                && !deduplicationCache.seenOnDifferentRecord(dedupKey, raw.fallbackEventId())) {
            log.debug("Skipping already-processed event {} from {}", eventId, raw.sourceRef());
            return IngestionResult.duplicate(eventId);
        }

        EventTimeResolver.Resolved resolved = eventTimeResolver.resolveValue(
                mapping.eventTime(payload, raw).orElse(null), raw.transportTime());
        if (!resolved.isObserved()) {
            log.warn("Event {} from {} has no usable timestamp at '{}'; using transport time {}",
                    eventId, raw.sourceRef(),
                    SourceMapping.describe(mapping.getFields().getEventTime()),
                    raw.transportTime());
        }

        String traceId = mapping.traceId(payload, raw).orElse(null);
        Correlation correlation = correlate(mapping, systemName, payload, raw, traceId);
        if (correlation == null) {
            String reason = "no value at '"
                    + SourceMapping.describe(mapping.getFields().getExecutionId())
                    + "', and no propagated trace context to fall back on";
            log.error("Cannot correlate observation from {}: {}. Declare the correlation key "
                    + "for this source under ingestion/*.yaml, or have the system propagate "
                    + "trace context.", raw.sourceRef(), reason);
            return IngestionResult.notCorrelatable(reason);
        }

        // A second identifier this observation carries, in whichever namespace its mapping
        // says it belongs to — this source's own unless the mapping deliberately says
        // otherwise.
        ExecutionKey alias = mapping.aliasKey(payload, raw)
                .map(key -> ExecutionKey.of(mapping.aliasNamespace(), key))
                .orElse(null);

        // Deciding the identity and storing the observation that establishes it are one
        // unit of work. See the class comment: an alias row cites an event, so committing
        // the two separately can leave a citation pointing at nothing — and a fold that
        // moved observations and withdrew findings on that basis.
        //
        // Resolution has to come first within the unit, not because of the transaction but
        // because an event's saga_id IS its identity: there is no row to write until the
        // key is known.
        transactions.executeWithoutResult(status -> {
            ExecutionKey correlated = aliasResolver.canonical(
                    correlation.key(), alias, eventId, resolved.eventTime());

            CorrelationBasis basis = correlated.equals(correlation.key())
                    ? correlation.basis()
                    : CorrelationBasis.ALIAS_LINK;

            // The namespace the observation is FILED under, which is the reading system's
            // own unless a declared cross-source alias moved it. saga_events.source_name is
            // the namespace half of saga_id -- a CHECK constraint ties them together -- so
            // passing the reading system here made a legitimate fold contradict itself and
            // the event was rejected as "identity disagrees with itself" and dropped.
            correlationService.processEvent(observationOf(
                    mapping, raw, payload, correlated.source(), eventId, correlated, basis,
                    resolved, traceId, raw.fallbackEventId()));
        });

        // Marked only now: the transaction above has committed, so a redelivery before this
        // point is still processed rather than silently dropped as already seen. The
        // record it came in on is recorded with it, so the next sighting of this event id
        // can say whether it is the same record read again or a second one.
        deduplicationCache.markProcessed(dedupKey, raw.fallbackEventId());
        return IngestionResult.accepted(eventId);
    }

    /**
     * Everything the mapping says about this observation, once its identity is settled.
     *
     * @param filedUnder the namespace of {@code correlated}, which is what
     *                   {@code source_name} stores; equal to the reading system's name
     *                   except after a declared cross-source alias link
     */
    private IngestedEvent observationOf(SourceMapping mapping, RawObservation raw,
            JsonNode payload, String filedUnder, String eventId, ExecutionKey correlated,
            CorrelationBasis basis, EventTimeResolver.Resolved time, String traceId, String transportRef) {
        return new IngestedEvent(
                new IngestedEvent.Identity(filedUnder, correlated, eventId,
                        mapping.businessKey(payload, raw).orElse(null), basis),
                mapping.getWorkflow(),
                mapping.eventType(payload, raw).orElse(raw.sourceRef()),
                mapping.stepName(payload, raw).orElse("UNKNOWN"),
                mapping.serviceName(payload, raw).orElse("unknown"),
                time.eventTime(),
                raw.payload(),
                Instant.now(),
                new IngestedEvent.Causality(
                        mapping.causationId(payload, raw).orElse(null),
                        mapping.spanId(payload, raw).orElse(null),
                        mapping.parentSpanId(payload, raw).orElse(null),
                        traceId),
                transportRef,
                time.source());
    }

    /** A correlation key and the grounds it was arrived at on. */
    private record Correlation(ExecutionKey key, CorrelationBasis basis) {
    }

    /**
     * Walks the correlation ladder and stops at the first rung that applies.
     *
     * <p>Rung one is the identifier the source declared. Rung two is propagated trace
     * context: weaker, because a trace is request-scoped rather than business-scoped, but
     * still something the producer emitted rather than something invented here — and for a
     * system whose services share no business identifier it is the difference between being
     * observable and not. A system can reach that rung by enabling an auto-instrumentation
     * agent, which is a deployment flag rather than a change to its code.
     *
     * <p>There is no third rung. Joining observations because they share a business key,
     * an event type, or a nearby timestamp is not evidence, and a wrong correlation does
     * not surface as an error — it surfaces as a diagnosis that quietly makes no sense.
     *
     * @return null when nothing on the ladder applies, which is a refusal rather than a
     *         reason to guess
     */
    private Correlation correlate(SourceMapping mapping, String sourceName,
            JsonNode payload, RawObservation raw, String traceId) {
        String declared = mapping.executionId(payload, raw).orElse(null);
        if (declared != null) {
            return new Correlation(ExecutionKey.of(sourceName, declared),
                    CorrelationBasis.DECLARED_KEY);
        }
        if (traceId != null && !traceId.isBlank()) {
            log.info("Observation from {} carries no declared correlation key; correlating on "
                    + "propagated trace context {}", raw.sourceRef(), traceId);
            return new Correlation(ExecutionKey.of(sourceName, traceId),
                    CorrelationBasis.PROPAGATED_CONTEXT);
        }
        return null;
    }
}
