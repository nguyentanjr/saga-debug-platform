package com.example.correlation.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Proposes a source mapping by looking at a sample of that source's events.
 *
 * <p>Writing a mapping by hand means reading someone else's payloads and deciding which
 * field is the identity, which is the correlation key, which is the clock. Most of that
 * is visible in the data: an identity is unique in every event, a correlation key repeats
 * across the events of one execution, a timestamp parses as one.
 *
 * <p>What it produces is a <em>proposal</em>, never an applied configuration. A wrong
 * guess about the correlation key silently merges unrelated executions or splits one into
 * many, and neither shows up as an error — it shows up as a diagnosis that quietly makes
 * no sense. Every field comes with the evidence behind it so the choice can be checked
 * before it is trusted.
 *
 * <p>Where the data cannot decide — telling an event type from a service name, both being
 * short repeated strings — the field name is used as a tiebreak and the proposal says so.
 */
@Service
public class SourceMappingInferrer {

    /** Below this many events the distinctions this relies on are not visible. */
    private static final int MIN_SAMPLE = 3;

    /**
     * How consecutive a value's events must be before it is read as a correlation key.
     *
     * <p>Below this the field behaves like a label recurring across the sample rather
     * than a key grouping one execution. Not 1.0: real streams interleave executions, so
     * a key's events are usually clustered rather than strictly adjacent.
     */
    private static final double CONTIGUITY_THRESHOLD = 0.6;

    private static final Set<String> TYPE_HINTS = Set.of("type", "eventtype", "name", "kind", "action");
    // Abbreviations included deliberately: a field called svc is a service name, and
    // requiring the word spelled out would leave it unproposed for no good reason.
    private static final Set<String> SERVICE_HINTS = Set.of("service", "servicename", "svc",
            "source", "src", "producer", "emitter", "origin", "component", "app");
    private static final Set<String> STEP_HINTS = Set.of("step", "stepname", "stage", "phase", "task");

    /** One proposed field, with the reason it was proposed. */
    public record FieldProposal(String path, String basis, double confidence) {
        static FieldProposal of(String path, String basis, double confidence) {
            return new FieldProposal(path, basis, confidence);
        }
    }

    public record InferredMapping(
            int sampleSize,
            Map<String, FieldProposal> fields,
            List<String> observedEventTypes,
            List<String> unresolved,
            List<String> warnings) {

        /** The mapping as it would be written, for review before it is adopted. */
        public String toYaml(String sourceName, String topicPattern) {
            StringBuilder yaml = new StringBuilder();
            yaml.append("# Proposed from ").append(sampleSize)
                    .append(" sampled event(s). Review before use — see the evidence for each\n")
                    .append("# field in the accompanying report.\n")
                    .append("source:\n")
                    .append("  name: ").append(sourceName).append('\n')
                    .append("  topicPattern: '").append(topicPattern).append("'\n")
                    .append("  fields:\n");
            fields.forEach((field, proposal) ->
                    yaml.append("    ").append(field).append(": ").append(proposal.path())
                            .append("        # ").append(proposal.basis()).append('\n'));
            if (!unresolved.isEmpty()) {
                yaml.append("  # Not determined from the sample: ")
                        .append(String.join(", ", unresolved)).append('\n');
            }
            return yaml.toString();
        }
    }

    public InferredMapping infer(List<JsonNode> sample) {
        List<String> warnings = new ArrayList<>();
        if (sample == null || sample.isEmpty()) {
            return new InferredMapping(0, Map.of(), List.of(),
                    List.of("everything"), List.of("no events supplied"));
        }
        if (sample.size() < MIN_SAMPLE) {
            warnings.add("Only " + sample.size() + " event(s): a correlation key cannot be told "
                    + "from an identity without seeing a value repeat.");
        }

        Map<String, PathStats> stats = collect(sample);
        Map<String, FieldProposal> fields = new LinkedHashMap<>();
        Set<String> taken = new LinkedHashSet<>();

        inferEventTime(stats, taken).ifPresent(p -> { fields.put("eventTime", p); taken.add(p.path()); });
        inferEventId(stats, sample.size(), taken).ifPresent(p -> { fields.put("eventId", p); taken.add(p.path()); });
        inferCausationId(stats, fields, taken).ifPresent(p -> { fields.put("causationId", p); taken.add(p.path()); });
        inferExecutionId(stats, sample.size(), taken).ifPresent(p -> { fields.put("executionId", p); taken.add(p.path()); });
        inferByCardinalityAndName(stats, taken, TYPE_HINTS, "eventType")
                .ifPresent(p -> { fields.put("eventType", p); taken.add(p.path()); });
        inferByCardinalityAndName(stats, taken, SERVICE_HINTS, "serviceName")
                .ifPresent(p -> { fields.put("serviceName", p); taken.add(p.path()); });
        inferByCardinalityAndName(stats, taken, STEP_HINTS, "stepName")
                .ifPresent(p -> { fields.put("stepName", p); taken.add(p.path()); });

        List<String> unresolved = new ArrayList<>();
        for (String required : List.of("eventId", "executionId", "eventType", "eventTime")) {
            if (!fields.containsKey(required)) unresolved.add(required);
        }
        if (!fields.containsKey("executionId")) {
            warnings.add("No field repeats across events, so nothing looks like a correlation "
                    + "key. Either the sample covers one event per execution, or this source "
                    + "does not carry one — in which case its executions cannot be reconstructed.");
        }
        if (!fields.containsKey("causationId")) {
            warnings.add("No field references another event's identity, so causal links are "
                    + "not reported by this source; step boundaries will be inferred from the "
                    + "workflow specification instead.");
        }

        List<String> eventTypes = fields.containsKey("eventType")
                ? List.copyOf(stats.get(fields.get("eventType").path()).distinct())
                : List.of();

        return new InferredMapping(sample.size(), fields, eventTypes, unresolved, warnings);
    }

    // ── field rules ───────────────────────────────────────────────────────────

    /** A clock is the field whose values parse as one. Nothing else is needed. */
    private Optional<FieldProposal> inferEventTime(Map<String, PathStats> stats, Set<String> taken) {
        return stats.entrySet().stream()
                .filter(e -> !taken.contains(e.getKey()))
                .filter(e -> e.getValue().allParseAsTime())
                .max(Comparator.comparingInt(e -> e.getValue().count()))
                .map(e -> FieldProposal.of(e.getKey(),
                        "every sampled value parses as a timestamp", 0.95));
    }

    /** An identity is present in every event and never repeats. */
    private Optional<FieldProposal> inferEventId(Map<String, PathStats> stats, int sampleSize,
            Set<String> taken) {
        return stats.entrySet().stream()
                .filter(e -> !taken.contains(e.getKey()))
                .filter(e -> e.getValue().count() == sampleSize)
                .filter(e -> e.getValue().distinct().size() == sampleSize)
                .max(Comparator.comparingDouble(e -> e.getValue().averageLength()))
                .map(e -> FieldProposal.of(e.getKey(),
                        "present in every event and distinct in each", 0.9));
    }

    /**
     * The one rule that is close to proof: a causal reference points at an identity this
     * same sample already contains.
     */
    private Optional<FieldProposal> inferCausationId(Map<String, PathStats> stats,
            Map<String, FieldProposal> fields, Set<String> taken) {
        FieldProposal identity = fields.get("eventId");
        if (identity == null) return Optional.empty();

        Set<String> identities = stats.get(identity.path()).distinct();
        return stats.entrySet().stream()
                .filter(e -> !taken.contains(e.getKey()))
                .filter(e -> !e.getValue().distinct().isEmpty())
                .filter(e -> identities.containsAll(e.getValue().distinct()))
                .findFirst()
                .map(e -> FieldProposal.of(e.getKey(),
                        "every value names an event id seen in this sample", 0.95));
    }

    /**
     * A correlation key repeats — that is what makes it one — while still separating
     * executions, so among the repeating fields it is the one with the most values.
     */
    private Optional<FieldProposal> inferExecutionId(Map<String, PathStats> stats, int sampleSize,
            Set<String> taken) {
        return stats.entrySet().stream()
                .filter(e -> !taken.contains(e.getKey()))
                .filter(e -> e.getValue().count() == sampleSize)
                .filter(e -> e.getValue().distinct().size() > 1)
                .filter(e -> e.getValue().distinct().size() < sampleSize)
                // Cardinality alone cannot separate a correlation key from a step name:
                // three executions of three steps give both exactly three values. What
                // separates them is that one execution's events are consecutive while a
                // step name recurs across the whole sample.
                .filter(e -> e.getValue().temporalContiguity() >= CONTIGUITY_THRESHOLD)
                .max(Comparator.comparingDouble(e -> e.getValue().temporalContiguity()))
                .map(e -> FieldProposal.of(e.getKey(),
                        String.format("repeats across events (%d values in %d) and each value's "
                                        + "events are consecutive (contiguity %.2f), which is "
                                        + "what a correlation key looks like",
                                e.getValue().distinct().size(), sampleSize,
                                e.getValue().temporalContiguity()),
                        0.75));
    }

    /**
     * Where the data cannot separate two short repeated strings, the field name breaks
     * the tie — and the proposal records that it was a name, not the values.
     */
    private Optional<FieldProposal> inferByCardinalityAndName(Map<String, PathStats> stats,
            Set<String> taken, Set<String> hints, String field) {
        return stats.entrySet().stream()
                .filter(e -> !taken.contains(e.getKey()))
                .filter(e -> !e.getValue().distinct().isEmpty())
                .filter(e -> matchesHint(e.getKey(), hints))
                .min(Comparator.comparingInt(e -> e.getValue().distinct().size()))
                .map(e -> FieldProposal.of(e.getKey(),
                        "name resembles a " + field + "; values repeat as expected "
                                + "(" + e.getValue().distinct().size() + " distinct). "
                                + "Chosen on the field name, not on the data", 0.5));
    }

    private static boolean matchesHint(String path, Set<String> hints) {
        String leaf = path.substring(path.lastIndexOf('.') + 1).toLowerCase();
        return hints.stream().anyMatch(leaf::contains);
    }

    // ── sampling ──────────────────────────────────────────────────────────────

    private Map<String, PathStats> collect(List<JsonNode> sample) {
        Map<String, PathStats> stats = new LinkedHashMap<>();
        for (int index = 0; index < sample.size(); index++) {
            flatten(sample.get(index), "", stats, index);
        }
        return stats;
    }

    private void flatten(JsonNode node, String prefix, Map<String, PathStats> stats,
            int sampleIndex) {
        if (node == null || !node.isObject()) return;

        node.fields().forEachRemaining(entry -> {
            String path = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            JsonNode value = entry.getValue();

            if (value.isObject()) {
                flatten(value, path, stats, sampleIndex);
            } else if (value.isValueNode() && !value.isNull()) {
                String text = value.asText();
                if (!text.isBlank()) {
                    stats.computeIfAbsent(path, k -> new PathStats()).add(text, sampleIndex);
                }
            }
            // Arrays are skipped: a mapping cannot address into one, so proposing a path
            // that the reader could not follow would be worse than proposing nothing.
        });
    }

    private static final class PathStats {
        private final Set<String> distinct = new LinkedHashSet<>();
        /** Which sampled events carried each value, in sample order. */
        private final Map<String, List<Integer>> occurrences = new LinkedHashMap<>();
        private int count;
        private long totalLength;

        void add(String value, int sampleIndex) {
            distinct.add(value);
            occurrences.computeIfAbsent(value, k -> new ArrayList<>()).add(sampleIndex);
            count++;
            totalLength += value.length();
        }

        Set<String> distinct() { return distinct; }
        int count() { return count; }
        double averageLength() { return count == 0 ? 0 : (double) totalLength / count; }

        /**
         * How tightly each value's events cluster together in time, from 0 to 1.
         *
         * <p>The signal that separates a correlation key from an event type when both
         * repeat the same number of times. One execution's events are consecutive: the
         * three events of REF-1 arrive, then the two of REF-2. A step name is scattered
         * instead, appearing once per execution across the whole sample.
         *
         * <p>Measured as how much of the index range each value spans is actually made up
         * of that value. A key whose events are consecutive scores 1.
         */
        double temporalContiguity() {
            if (occurrences.isEmpty()) return 0;
            int covered = 0;
            int spanned = 0;
            for (List<Integer> indices : occurrences.values()) {
                covered += indices.size();
                spanned += indices.get(indices.size() - 1) - indices.get(0) + 1;
            }
            return spanned == 0 ? 0 : (double) covered / spanned;
        }

        boolean allParseAsTime() {
            return count > 0 && distinct.stream().allMatch(PathStats::parsesAsTime);
        }

        private static boolean parsesAsTime(String value) {
            try {
                Instant.parse(value);
                return true;
            } catch (Exception ignored) {
                // Epoch seconds or millis, bounded so an ordinary number is not mistaken
                // for a clock: 2001-09-09 through 5138.
                try {
                    long epoch = Long.parseLong(value);
                    return epoch > 1_000_000_000L && epoch < 100_000_000_000_000L;
                } catch (NumberFormatException e) {
                    return false;
                }
            }
        }
    }
}
