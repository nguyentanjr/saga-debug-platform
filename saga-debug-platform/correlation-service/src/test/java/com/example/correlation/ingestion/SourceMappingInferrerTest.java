package com.example.correlation.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Works out how to read a source from a sample of its events.
 *
 * <p>The alternative is a person reading someone else's payloads and deciding which field
 * is the identity, which the correlation key, which the clock. Most of that is a property
 * of the values, not of the field names: an identity never repeats, a correlation key
 * does, a timestamp parses as one, and a causal reference names an identity already in
 * the sample.
 */
class SourceMappingInferrerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final SourceMappingInferrer inferrer = new SourceMappingInferrer();

    /** Three executions of four steps, in a shape nothing here was written around. */
    private final List<JsonNode> sample = parse("""
            [
             {"id":"e1","kind":"cart.opened",  "at":"2026-01-01T00:00:00Z",
              "ctx":{"ref":"REF-1"},"from":{"svc":"cart-api"}},
             {"id":"e2","kind":"cart.charged", "at":"2026-01-01T00:00:04Z",
              "ctx":{"ref":"REF-1","after":"e1"},"from":{"svc":"billing-api"}},
             {"id":"e3","kind":"cart.shipped", "at":"2026-01-01T00:00:09Z",
              "ctx":{"ref":"REF-1","after":"e2"},"from":{"svc":"courier-api"}},
             {"id":"e4","kind":"cart.opened",  "at":"2026-01-01T00:01:00Z",
              "ctx":{"ref":"REF-2"},"from":{"svc":"cart-api"}},
             {"id":"e5","kind":"cart.charged", "at":"2026-01-01T00:01:03Z",
              "ctx":{"ref":"REF-2","after":"e4"},"from":{"svc":"billing-api"}},
             {"id":"e6","kind":"cart.opened",  "at":"2026-01-01T00:02:00Z",
              "ctx":{"ref":"REF-3"},"from":{"svc":"cart-api"}}
            ]
            """);

    @Test
    void theIdentityIsTheFieldThatNeverRepeats() {
        var inferred = inferrer.infer(sample);

        assertThat(inferred.fields().get("eventId").path()).isEqualTo("id");
        assertThat(inferred.fields().get("eventId").basis()).contains("distinct in each");
    }

    @Test
    void theCorrelationKeyIsTheFieldThatRepeats() {
        var inferred = inferrer.infer(sample);

        // Nested, and named nothing this platform would recognise.
        assertThat(inferred.fields().get("executionId").path()).isEqualTo("ctx.ref");
        assertThat(inferred.fields().get("executionId").basis()).contains("repeats across events");
    }

    @Test
    void theClockIsTheFieldWhoseValuesParseAsOne() {
        var inferred = inferrer.infer(sample);

        assertThat(inferred.fields().get("eventTime").path()).isEqualTo("at");
        assertThat(inferred.fields().get("eventTime").basis()).contains("parses as a timestamp");
    }

    /** The one rule close to proof: a causal reference names an id already in the sample. */
    @Test
    void aCausalReferenceIsRecognisedByPointingAtAnIdentity() {
        var inferred = inferrer.infer(sample);

        assertThat(inferred.fields().get("causationId").path()).isEqualTo("ctx.after");
        assertThat(inferred.fields().get("causationId").basis())
                .contains("names an event id seen in this sample");
    }

    @Test
    void epochTimestampsAreRecognisedTooButOrdinaryNumbersAreNot() {
        var inferred = inferrer.infer(parse("""
                [
                 {"id":"a","kind":"x","at":1767225600,"ref":"R1","qty":3},
                 {"id":"b","kind":"y","at":1767225604,"ref":"R1","qty":7},
                 {"id":"c","kind":"x","at":1767225700,"ref":"R2","qty":1}
                ]
                """));

        assertThat(inferred.fields().get("eventTime").path()).isEqualTo("at");
        // A quantity is a number, not a clock.
        assertThat(inferred.fields().values())
                .noneMatch(proposal -> proposal.path().equals("qty"));
    }

    @Test
    void aNameBasedChoiceSaysThatItWasNameBased() {
        var inferred = inferrer.infer(sample);

        // Event type and service name are both short repeated strings; the values cannot
        // separate them, so the field name does — and the proposal admits it.
        assertThat(inferred.fields().get("eventType").path()).isEqualTo("kind");
        assertThat(inferred.fields().get("eventType").basis())
                .contains("Chosen on the field name, not on the data");
        assertThat(inferred.fields().get("serviceName").path()).isEqualTo("from.svc");
    }

    @Test
    void theSourcesOwnVocabularyIsReportedSoASpecCanBeWrittenInIt() {
        var inferred = inferrer.infer(sample);

        // Nothing has to be renamed: a workflow specification can be written using these
        // names directly, which is why the eventTypes translation is optional.
        assertThat(inferred.observedEventTypes())
                .containsExactlyInAnyOrder("cart.opened", "cart.charged", "cart.shipped");
    }

    @Test
    void aSourceWithNoRepeatingFieldIsReportedAsUncorrelatable() {
        var inferred = inferrer.infer(parse("""
                [
                 {"id":"a","kind":"x","at":"2026-01-01T00:00:00Z"},
                 {"id":"b","kind":"y","at":"2026-01-01T00:00:01Z"},
                 {"id":"c","kind":"z","at":"2026-01-01T00:00:02Z"}
                ]
                """));

        assertThat(inferred.unresolved()).contains("executionId");
        assertThat(inferred.warnings())
                .anyMatch(w -> w.contains("nothing looks like a correlation key"));
    }

    @Test
    void tooSmallASampleIsCalledOutRatherThanGuessedAt() {
        var inferred = inferrer.infer(parse("""
                [{"id":"a","kind":"x","at":"2026-01-01T00:00:00Z","ref":"R1"}]
                """));

        assertThat(inferred.warnings())
                .anyMatch(w -> w.contains("cannot be told from an identity"));
    }

    @Test
    void theProposalIsWrittenOutWithItsEvidenceForReview() {
        String yaml = inferrer.infer(sample).toYaml("cart-system", "cart\\..*");

        assertThat(yaml)
                .contains("executionId: ctx.ref")
                .contains("causationId: ctx.after")
                .contains("Review before use");
    }

    @Test
    void anEmptySampleProposesNothing() {
        var inferred = inferrer.infer(List.of());

        assertThat(inferred.fields()).isEmpty();
        assertThat(inferred.warnings()).contains("no events supplied");
    }

    private List<JsonNode> parse(String json) {
        try {
            List<JsonNode> nodes = new ArrayList<>();
            mapper.readTree(json).forEach(nodes::add);
            return nodes;
        } catch (Exception e) {
            throw new IllegalArgumentException("test sample is not JSON", e);
        }
    }
}
