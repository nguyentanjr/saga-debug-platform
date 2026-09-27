package com.example.correlation.service.rca;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * How one observed system words its failure messages, and where it puts them.
 *
 * <p>Declared per source, for the reason on {@link FailureSignal}: which field carries the
 * reason and what the reason says when a breaker refused a call are facts about the system
 * being watched, not about this platform. They belong in a file next to that system's
 * source mapping and workflow, so that recognising a new system's failures is adding a
 * declaration rather than editing a rule.
 *
 * <p><strong>A source with no declaration gets no signal read.</strong> Not a weaker
 * reading — none. The platform cannot tell whether an undeclared system's failure text
 * reports breaker state or merely mentions the word, and guessing is the defect this type
 * was introduced to remove (I8: do not infer what the declaration does not say).
 */
public class FailureSignalDeclaration {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** The source name these signals belong to, as {@code SagaEvent.sourceName} spells it. */
    private String source;

    /**
     * Candidate JSON field names holding the failure reason, tried in order.
     *
     * <p>A list rather than one name because a system may move the field between versions
     * or between topics, exactly as {@code SourceMapping.Fields} allows. The first field
     * present and non-blank wins; nothing is concatenated, because matching a pattern
     * against a joined blob would reintroduce whole-payload scanning by another route.
     */
    private List<String> reasonFields = List.of("reason");

    /**
     * Wording that signals each category, as case-insensitive substrings.
     *
     * <p>Substrings rather than regular expressions deliberately: these are declarations an
     * operator writes by copying a message their system emits, and a mis-typed regex fails
     * in ways that are hard to see in a log. If a system genuinely needs one, that is a
     * decision to take then, with the failure mode understood.
     */
    private Map<FailureSignal, List<String>> signals = new LinkedHashMap<>();

    /** The declared reason text of an event, if this declaration can find one. */
    public Optional<String> readReason(String payload) {
        if (payload == null || payload.isBlank()) return Optional.empty();
        JsonNode root;
        try {
            root = JSON.readTree(payload);
        } catch (Exception e) {
            // Not JSON, or not JSON this declaration can read. Reporting nothing is the
            // honest answer; the alternative is to fall back to scanning the raw text,
            // which is the behaviour being removed.
            return Optional.empty();
        }
        for (String field : reasonFields) {
            JsonNode node = root.get(field);
            if (node != null && node.isTextual() && !node.asText().isBlank()) {
                return Optional.of(node.asText());
            }
        }
        return Optional.empty();
    }

    /**
     * The pattern that matched, if this reason signals {@code signal}.
     *
     * <p>Returns the pattern rather than a boolean so the finding can cite the declared
     * wording it matched on. A candidate that cannot say which declaration produced it is
     * as opaque as the substring match it replaces.
     */
    public Optional<String> matchedPattern(FailureSignal signal, String reason) {
        if (reason == null) return Optional.empty();
        String haystack = reason.toLowerCase(Locale.ROOT);
        return signals.getOrDefault(signal, List.of()).stream()
                .filter(pattern -> pattern != null && !pattern.isBlank())
                .filter(pattern -> haystack.contains(pattern.toLowerCase(Locale.ROOT)))
                .findFirst();
    }

    /** Whether this declaration says anything at all about {@code signal}. */
    public boolean declares(FailureSignal signal) {
        List<String> patterns = signals.get(signal);
        return patterns != null && !patterns.isEmpty();
    }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public List<String> getReasonFields() { return reasonFields; }
    public void setReasonFields(List<String> reasonFields) {
        if (reasonFields != null && !reasonFields.isEmpty()) this.reasonFields = reasonFields;
    }

    public Map<FailureSignal, List<String>> getSignals() { return signals; }
    public void setSignals(Map<FailureSignal, List<String>> signals) {
        this.signals = signals == null ? new LinkedHashMap<>() : signals;
    }
}
