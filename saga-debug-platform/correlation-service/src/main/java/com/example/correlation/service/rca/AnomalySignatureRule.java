package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.rca.CandidateStatus;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A diagnosis rule whose whole condition is "one of these anomaly types was detected".
 *
 * <p>Nine of the eighteen rules are exactly this shape: look for an anomaly type, name a
 * root cause, attach the anomaly as evidence, suggest what to check. Written as Java
 * methods they were nine near-identical loops, and adding a rule for a new domain meant
 * editing and rebuilding the engine — which is what {@code workflows/*.yaml} and
 * {@code ingestion/*.yaml} already avoid for the other two things this platform has to be
 * told about.
 *
 * <h2>What deliberately stays in Java</h2>
 * The other nine rules are not expressible here, and pretending otherwise would produce a
 * configuration language that quietly grows into a programming one. They need something
 * this format has no way to say:
 * <ul>
 *   <li>reading the <em>content</em> of an event payload (R01 looks for a circuit-breaker
 *       signature inside the failure message);</li>
 *   <li>the saga's current status (R06, R14, R21 answer differently once the execution
 *       has reached a terminal state);</li>
 *   <li>arithmetic over timestamps against a threshold read from the workflow (R05);</li>
 *   <li>the absence of an event the specification says should exist (R10, R11);</li>
 *   <li>evidence computed from the anomaly set rather than quoted from one anomaly
 *       (R20 lists the types it saw).</li>
 * </ul>
 *
 * <p>The line is therefore not "simple rules here, complex rules there" but a property of
 * the rule itself: <b>a rule belongs here when a set of anomaly types is the entire
 * condition.</b>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AnomalySignatureRule {

    private String id;
    private String rootCause;
    private String severity;
    private CandidateStatus evidence = CandidateStatus.STRONG_EVIDENCE;

    /**
     * The anomaly types that trigger this rule. Matching is "any of", not "all of":
     * a rule naming several types treats them as alternative signatures of one cause —
     * R13 fires on EXACT_DUPLICATE_EVENT or on DUPLICATE_COMPENSATION, either being
     * enough to say the same thing about delivery.
     */
    private List<AnomalyType> matchesAnyAnomaly = new ArrayList<>();

    /**
     * Fixed statements added alongside the matched anomaly, for a rule that has something
     * to say beyond quoting it. Kept separate from {@code suggestions}: this is what the
     * platform observed, that is what the reader should do about it.
     */
    private List<String> evidenceNotes = new ArrayList<>();

    private List<String> suggestions = new ArrayList<>();

    /**
     * Every anomaly this rule matches, earliest business time first.
     *
     * <p>This used to return the first match in the order it was handed, meant as "the
     * first detected" so the candidate would anchor where its story starts. But the list
     * comes from {@code findStandingBySagaId}, which has no ORDER BY, and a rule matching
     * two findings — R04 on FORBIDDEN_POST_TERMINAL and STATE_REGRESSION, R12 on two
     * invalid transitions — cited whichever came back first. The same findings in another
     * order moved the anchor and changed the promoted cause (ROADMAP F11). Quoting all of
     * them lets {@code RcaService} anchor at the earliest event any of them names, which is
     * what "first" was for, with nothing left to the storage order.
     *
     * @return empty when no anomaly matches, which means the rule does not fire
     */
    public List<Anomaly> matching(List<Anomaly> anomalies) {
        if (anomalies == null || matchesAnyAnomaly.isEmpty()) {
            return List.of();
        }
        Set<AnomalyType> wanted = Set.copyOf(matchesAnyAnomaly);
        return anomalies.stream()
                .filter(anomaly -> anomaly != null && wanted.contains(anomaly.getAnomalyType()))
                .sorted(CITATION_ORDER)
                .toList();
    }

    /** Business time first; the rest only makes the order total, and claims nothing. */
    private static final Comparator<Anomaly> CITATION_ORDER = Comparator
            .comparing(Anomaly::getMeasuredAt, Comparator.nullsLast(Comparator.naturalOrder()))
            .thenComparing(anomaly -> anomaly.getAnomalyType().name())
            .thenComparing(anomaly -> String.valueOf(anomaly.getObservedValue()))
            .thenComparing(anomaly -> String.valueOf(anomaly.getEvidenceEventIds()))
            .thenComparing(anomaly -> String.valueOf(anomaly.getDescription()));

    /**
     * Why this rule could never fire, or empty when it is well formed.
     *
     * <p>Checked at load rather than at diagnosis time. A rule missing its root cause or
     * its trigger is not a rule that produces worse answers — it is one that silently
     * never runs, and a silent absence is the hardest kind of defect to notice in a
     * diagnostic tool.
     */
    public Optional<String> validationError() {
        if (id == null || id.isBlank()) return Optional.of("no id");
        if (rootCause == null || rootCause.isBlank()) return Optional.of("no rootCause");
        if (severity == null || severity.isBlank()) return Optional.of("no severity");
        if (matchesAnyAnomaly.isEmpty()) {
            return Optional.of("matchesAnyAnomaly is empty, so the rule can never fire");
        }
        if (evidence == null) return Optional.of("no evidence tier");
        return Optional.empty();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getRootCause() { return rootCause; }
    public void setRootCause(String rootCause) { this.rootCause = rootCause; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public CandidateStatus getEvidence() { return evidence; }
    public void setEvidence(CandidateStatus evidence) { this.evidence = evidence; }

    public List<AnomalyType> getMatchesAnyAnomaly() { return matchesAnyAnomaly; }
    public void setMatchesAnyAnomaly(List<AnomalyType> matchesAnyAnomaly) {
        this.matchesAnyAnomaly = matchesAnyAnomaly != null
                ? new ArrayList<>(new LinkedHashSet<>(matchesAnyAnomaly))
                : new ArrayList<>();
    }

    public List<String> getEvidenceNotes() { return evidenceNotes; }
    public void setEvidenceNotes(List<String> evidenceNotes) {
        this.evidenceNotes = evidenceNotes != null ? evidenceNotes : new ArrayList<>();
    }

    public List<String> getSuggestions() { return suggestions; }
    public void setSuggestions(List<String> suggestions) {
        this.suggestions = suggestions != null ? suggestions : new ArrayList<>();
    }
}
