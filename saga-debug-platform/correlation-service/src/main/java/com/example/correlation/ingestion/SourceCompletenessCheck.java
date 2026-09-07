package com.example.correlation.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * What a source did not supply, judged against what that source declares.
 *
 * <p>Replaces a schema validator that checked every incoming document for four fields
 * named {@code eventId}, {@code eventType}, {@code sagaId} and {@code timestamp}. Those
 * are this platform's names. The mapping layer exists because no external system uses
 * them, so the validator reported four violations for every single event of every
 * observed system — while the mapping beside it was reading those same values without
 * difficulty. The log said the observed system was malformed; the observed system was
 * fine.
 *
 * <p>The question worth asking is the other one: this source says its event time is at
 * {@code occurredAt} — did this event actually have one? That is a real gap, it is
 * stated in the source's own terms, and it tells an operator something they can act on.
 *
 * <h2>Identity and correlation are not checked here</h2>
 * {@link EventIngestionService} rejects an observation outright when it cannot identify
 * or correlate it. Repeating those two as advisory findings would report as a warning
 * something that already stopped the event from being ingested.
 *
 * <h2>Reported once per source and field</h2>
 * A source that never provides {@code serviceName} would otherwise log that on every
 * event forever. The fact is a property of the source, so it is worth saying once and
 * not worth saying twice.
 */
@Component
public class SourceCompletenessCheck {

    private static final Logger log = LoggerFactory.getLogger(SourceCompletenessCheck.class);

    private final Set<String> alreadyReported = ConcurrentHashMap.newKeySet();

    /**
     * @return the fields this source declares but did not provide, each named with the
     *         path the source's own mapping gave for it; empty when the source supplied
     *         everything it promised
     */
    public List<String> gaps(JsonNode payload, SourceMapping mapping, RawObservation raw) {
        if (mapping == null) {
            return List.of();
        }

        SourceMapping.Fields fields = mapping.getFields();
        Map<String, List<String>> declared = new LinkedHashMap<>();
        declared.put("eventTime", fields.getEventTime());
        declared.put("stepName", fields.getStepName());
        declared.put("serviceName", fields.getServiceName());

        List<String> gaps = new ArrayList<>();
        declared.forEach((field, paths) -> {
            if (paths == null || paths.isEmpty()) {
                // Undeclared is a deliberate statement that this source has no such
                // field, not an omission.
                return;
            }
            if (mapping.resolve(payload, raw, paths).isEmpty()) {
                gaps.add(field + " (declared at '" + SourceMapping.describe(paths) + "')");
            }
        });
        return gaps;
    }

    /** Logs each gap the first time this source shows it, and never again. */
    public void report(List<String> gaps, String sourceName, String sourceRef) {
        for (String gap : gaps) {
            if (alreadyReported.add(sourceName + "|" + gap)) {
                log.warn("Source '{}' ({}) declares {} but this event did not carry it; "
                        + "reported once per source", sourceName, sourceRef, gap);
            }
        }
    }
}
