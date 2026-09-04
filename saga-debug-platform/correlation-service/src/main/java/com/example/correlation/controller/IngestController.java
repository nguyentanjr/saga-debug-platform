package com.example.correlation.controller;

import com.example.correlation.ingestion.EventIngestionService;
import com.example.correlation.ingestion.IngestionResult;
import com.example.correlation.ingestion.RawObservation;
import com.example.correlation.ingestion.SourceMappingInferrer;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The HTTP adapter: anything that can post JSON can be observed.
 *
 * <p>Kafka was the only way in, so a system publishing over AMQP, calling a webhook, or
 * shipping from a log agent could not be observed at all without first being put behind
 * a broker. This is the same ingestion path, reached over HTTP — the mapping named by
 * {@code source} decides how the body is read, exactly as a topic name does.
 *
 * <p>Accepts one event or an array of them. Per-event outcomes are reported individually
 * rather than failing the batch: one unreadable record among fifty should not cost the
 * caller the other forty-nine.
 *
 * <p>Unauthenticated, like every endpoint here. That is a real exposure — an event posted
 * to this endpoint becomes diagnostic evidence — and it needs an answer before this runs
 * anywhere that matters.
 */
@RestController
@RequestMapping("/api/ingest")
public class IngestController {

    private static final Logger log = LoggerFactory.getLogger(IngestController.class);

    /** Bounds the work one request can ask for. */
    private static final int MAX_BATCH = 1000;

    private final EventIngestionService ingestionService;
    private final SourceMappingInferrer inferrer;

    public IngestController(EventIngestionService ingestionService, SourceMappingInferrer inferrer) {
        this.ingestionService = ingestionService;
        this.inferrer = inferrer;
    }

    /**
     * Proposes how to read a source, from a sample of its events.
     *
     * <p>Nothing is stored or applied. Writing a mapping by hand means reading someone
     * else's payloads and working out which field is the identity, which the correlation
     * key, which the clock — most of which is visible in the data. This does that reading
     * and hands back a proposal with the evidence behind each choice.
     *
     * <p>Deliberately not automatic: a wrong guess at the correlation key merges unrelated
     * executions or splits one into several, and neither surfaces as an error. It surfaces
     * as a diagnosis that quietly makes no sense.
     */
    @PostMapping("/{source}/infer")
    public ResponseEntity<Map<String, Object>> infer(@PathVariable("source") String source,
            @RequestBody JsonNode body) {

        List<JsonNode> sample = body.isArray() ? toList(body) : List.of(body);
        if (sample.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "no events to learn from"));
        }

        SourceMappingInferrer.InferredMapping inferred = inferrer.infer(sample);

        Map<String, Object> fields = new LinkedHashMap<>();
        inferred.fields().forEach((field, proposal) -> fields.put(field, Map.of(
                "path", proposal.path(),
                "basis", proposal.basis(),
                "confidence", proposal.confidence())));

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("source", source);
        response.put("sampleSize", inferred.sampleSize());
        response.put("fields", fields);
        response.put("observedEventTypes", inferred.observedEventTypes());
        response.put("unresolved", inferred.unresolved());
        response.put("warnings", inferred.warnings());
        // Dots in a source name are regex metacharacters; escaped so the proposed
        // pattern matches the literal name it was derived from.
        response.put("proposedYaml",
                inferred.toYaml(source, source.replace(".", "\\.")));

        return ResponseEntity.ok(response);
    }

    /**
     * @param source which mapping to read the body with; matched against the same
     *               {@code topicPattern} that Kafka topics are
     */
    @PostMapping("/{source}")
    public ResponseEntity<Map<String, Object>> ingest(@PathVariable("source") String source,
            @RequestBody JsonNode body) {

        List<JsonNode> events = body.isArray()
                ? toList(body)
                : List.of(body);

        if (events.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "no events in request"));
        }
        if (events.size() > MAX_BATCH) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "batch too large",
                    "max", MAX_BATCH,
                    "received", events.size()));
        }

        Instant receivedAt = Instant.now();
        List<Map<String, Object>> results = new ArrayList<>(events.size());
        int accepted = 0;
        int duplicates = 0;
        int rejected = 0;

        for (JsonNode event : events) {
            // No fallback identity: unlike a Kafka record, an HTTP body has no offset to
            // fall back on, so an event without an id cannot be deduplicated and is
            // rejected rather than being given a synthetic one that changes per request.
            IngestionResult result = ingestionService.ingest(new RawObservation(
                    source, event.toString(), receivedAt, null, null, Map.of()));

            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("outcome", result.outcome().name());
            if (result.eventId() != null) entry.put("eventId", result.eventId());
            if (result.reason() != null) entry.put("reason", result.reason());
            results.add(entry);

            if (result.isAccepted()) accepted++;
            else if (result.outcome() == IngestionResult.Outcome.DUPLICATE) duplicates++;
            else rejected++;
        }

        log.info("Ingested {} observation(s) from source '{}': {} accepted, {} duplicate, {} rejected",
                events.size(), source, accepted, duplicates, rejected);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("source", source);
        response.put("received", events.size());
        response.put("accepted", accepted);
        response.put("duplicates", duplicates);
        response.put("rejected", rejected);
        response.put("results", results);

        // 202 when anything was stored, 400 when the caller sent nothing usable. A
        // transient failure never reaches here: it propagates so the caller retries.
        return ResponseEntity
                .status(accepted > 0 || duplicates > 0 ? HttpStatus.ACCEPTED : HttpStatus.BAD_REQUEST)
                .body(response);
    }

    private static List<JsonNode> toList(JsonNode array) {
        List<JsonNode> events = new ArrayList<>(array.size());
        array.forEach(events::add);
        return events;
    }
}
