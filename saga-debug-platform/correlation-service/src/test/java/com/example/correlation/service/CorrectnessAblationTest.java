package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.*;
import com.example.correlation.service.alert.AlertSink;
import com.example.correlation.service.rca.FailureSignalRegistry;
import com.example.correlation.service.rca.RcaSelector;
import com.example.correlation.service.rca.RuleRegistry;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * E2 (ROADMAP): ablation of the correctness mechanisms that the recorded evaluation set
 * never exercises -- it is one source, delivered in order, so none of them ever fires.
 *
 * <p>Each experiment is a paired input through the real correlation, inline detectors,
 * RCA rules and selector (in-memory repositories, as in {@link Sc06ReplayTest}). The
 * mechanism is removed at the INPUT or at the repository boundary, never by a switch in
 * production code:
 * <ul>
 *   <li>identity namespace: two sources' runs are relabelled as one source, which is what
 *       keying executions by raw id alone amounts to;</li>
 *   <li>arrival-order independence: every delivery order of recorded real runs, with the
 *       subject guard of {@code RcaSelector.promote} ablated on the same outputs;</li>
 *   <li>withdrawal: {@code retractClaims} made a no-op, which is the platform before
 *       absence claims could be withdrawn.</li>
 * </ul>
 * Printed rows are the ablation table; assertions pin the control behaviour and the
 * observed difference, so a later change that erases the difference is visible.
 */
class CorrectnessAblationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    // ══ A. identity namespace ═════════════════════════════════════════════════

    @Test
    void namespaceKeepsTwoSourcesRunsApart() {
        Pipeline control = new Pipeline();
        control.deliver(healthyRun("shop-a", "ORD-1", "a", 0));
        control.deliver(healthyRun("shop-b", "ORD-1", "b", 1));

        // Ablated: the same observations with the source dimension erased.
        Pipeline ablated = new Pipeline();
        ablated.deliver(healthyRun("shop", "ORD-1", "a", 0));
        ablated.deliver(healthyRun("shop", "ORD-1", "b", 1));

        Outcome a = control.outcome(ExecutionKey.of("shop-a", "ORD-1").canonical());
        Outcome b = control.outcome(ExecutionKey.of("shop-b", "ORD-1").canonical());
        Outcome merged = ablated.outcome(ExecutionKey.of("shop", "ORD-1").canonical());

        report("A namespace", "control shop-a", a);
        report("A namespace", "control shop-b", b);
        report("A namespace", "ablated merged", merged);

        assertThat(control.instances).hasSize(2);
        assertThat(a.anomalies).isEmpty();
        assertThat(b.anomalies).isEmpty();
        assertThat(a.rcaStatus).isEqualTo("NO_DIAGNOSIS_NEEDED");
        assertThat(b.rcaStatus).isEqualTo("NO_DIAGNOSIS_NEEDED");

        assertThat(ablated.instances).hasSize(1);
        assertThat(merged.anomalies).as("two healthy runs merged into one raise findings neither run has")
                .isNotEmpty();
    }

    private List<IngestedEvent> healthyRun(String source, String rawId, String prefix, int offsetSeconds) {
        Instant t = Instant.parse("2026-09-01T10:00:00Z").plusSeconds(offsetSeconds);
        String[][] steps = {
                {"ORDER_CREATED", "CREATE_ORDER", "order-service"},
                {"PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service"},
                {"INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service"}};
        List<IngestedEvent> run = new ArrayList<>();
        for (int i = 0; i < steps.length; i++) {
            Instant at = t.plusSeconds(2L * i);
            run.add(event(source, ExecutionKey.of(source, rawId), prefix + "-e" + i,
                    steps[i][0], steps[i][1], steps[i][2], at, at, "{}", null));
        }
        return run;
    }

    // ══ B. arrival-order independence ═════════════════════════════════════════

    @Test
    void conclusionDoesNotDependOnDeliveryOrder() throws Exception {
        int totalOrders = 0, divergent = 0, statusDivergent = 0, r14Raised = 0, guardAblatedWrong = 0;
        for (String scenario : List.of("SC01_HEALTHY", "SC02_COMPENSATION_HAPPY_PATH",
                "SC06_CIRCUIT_BREAKER_REAL", "SC27_PAYMENT_PAUSE_RECOVERY")) {
            JsonNode run = fixture(scenario).path("raw_runs").get(0);
            List<JsonNode> recorded = new ArrayList<>();
            run.path("raw_events").forEach(recorded::add);
            recorded.sort(Comparator.comparing(e -> Instant.parse(e.path("receivedAt").asText())));

            Set<String> conclusions = new LinkedHashSet<>();
            Set<String> statuses = new LinkedHashSet<>();
            int orders = 0, withR14 = 0, wrongWithoutGuard = 0;
            for (List<JsonNode> order : permutations(recorded)) {
                Pipeline p = new Pipeline();
                Instant base = Instant.parse(recorded.get(0).path("receivedAt").asText());
                String id = null;
                for (int i = 0; i < order.size(); i++) {
                    // Delivery order is the permutation; each record keeps its own source stamp.
                    IngestedEvent e = fromRecorded(order.get(i), base.plusSeconds(i));
                    id = e.identity().executionKey().canonical();
                    p.deliver(List.of(e));
                }
                Outcome o = p.outcome(id);
                // The diagnosis: execution outcome and the published cause. rcaStatus is tracked
                // apart so a regression of F19 is reported on its own line.
                String conclusion = o.sagaStatus + " / " + o.primary;
                statuses.add(o.rcaStatus);
                conclusions.add(conclusion);
                orders++;

                boolean r14 = o.candidates.stream().anyMatch(c -> "R14".equals(c.getRuleId())
                        && "STRONG_EVIDENCE".equals(String.valueOf(c.getStatus())));
                if (r14) {
                    withR14++;
                    // Subject guard ablated on the same output: first STRONG candidate, R14 included.
                    String unguarded = o.candidates.stream()
                            .filter(c -> "STRONG_EVIDENCE".equals(String.valueOf(c.getStatus())))
                            .map(c -> c.getRootCause()).findFirst().orElse(null);
                    if (!Objects.equals(unguarded, o.primary)) wrongWithoutGuard++;
                }
            }
            System.out.printf("[E2 B order] %-30s orders=%3d distinct_diagnoses=%d rca_statuses=%s R14_raised=%3d"
                            + " primary_changed_without_subject_guard=%3d  diagnoses=%s%n",
                    scenario, orders, conclusions.size(), statuses, withR14, wrongWithoutGuard, conclusions);
            statusDivergent += statuses.size() > 1 ? 1 : 0;
            totalOrders += orders;
            divergent += conclusions.size() > 1 ? 1 : 0;
            r14Raised += withR14;
            guardAblatedWrong += wrongWithoutGuard;
        }
        System.out.printf("[E2 B order] TOTAL orders=%d scenarios_with_divergent_diagnosis=%d"
                + " scenarios_with_divergent_rca_status=%d R14_raised=%d primary_changed_without_subject_guard=%d%n",
                totalOrders, divergent, statusDivergent, r14Raised, guardAblatedWrong);

        assertThat(divergent).as("scenarios whose diagnosis depends on delivery order").isZero();
        assertThat(statusDivergent).as("F19: scenarios whose rcaStatus depends on delivery order").isZero();
        assertThat(r14Raised).as("out-of-order deliveries are reported, as observation-process findings")
                .isPositive();
        assertThat(guardAblatedWrong).as("without the subject guard, reordering alone changes the diagnosis")
                .isPositive();
    }

    private IngestedEvent fromRecorded(JsonNode e, Instant receivedAt) throws Exception {
        JsonNode payload = MAPPER.readTree(e.path("payload").asText());
        String source = e.path("sourceName").asText();
        return new IngestedEvent(
                new IngestedEvent.Identity(source, ExecutionKey.of(source, payload.path("sagaId").asText()),
                        e.path("eventId").asText(), nullable(payload, "orderId"),
                        CorrelationBasis.valueOf(e.path("correlationBasis").asText())),
                null, e.path("eventType").asText(), nullable(e, "stepName"), nullable(e, "serviceName"),
                Instant.parse(e.path("timestamp").asText()), e.path("payload").asText(), receivedAt,
                new IngestedEvent.Causality(nullable(e, "causationId"), nullable(e, "spanId"),
                        nullable(e, "parentSpanId"), nullable(e, "traceId")),
                nullable(e, "transportRef"));
    }

    // ══ C. withdrawal of absence claims ═══════════════════════════════════════

    @Test
    void aFalsifiedAbsenceClaimStopsInformingTheDiagnosis() {
        Outcome control = withdrawalScenario(true);
        Outcome ablated = withdrawalScenario(false);
        report("C withdrawal", "control", control);
        report("C withdrawal", "ablated (no retraction)", ablated);

        assertThat(control.retracted).isPositive();
        assertThat(ablated.retracted).isZero();
        assertThat(control.anomalies).doesNotContain("SAGA_STUCK");
        assertThat(ablated.anomalies).contains("SAGA_STUCK");
        assertThat(ablated.primary + "|" + ablated.rcaStatus + "|" + ablated.candidateRules)
                .as("a claim the platform knows to be falsified changes the diagnosis when it is kept")
                .isNotEqualTo(control.primary + "|" + control.rcaStatus + "|" + control.candidateRules);
    }

    /**
     * An execution first seen through an event no workflow declares (backlog read out of
     * order), silent long enough to be called stuck, then its ORDER_CREATED and the rest
     * arrive and resolve the workflow -- which withdraws what was claimed about the
     * unresolved run.
     */
    private Outcome withdrawalScenario(boolean retraction) {
        Pipeline p = new Pipeline();
        p.retractionEnabled = retraction;
        ExecutionKey key = ExecutionKey.of("shop", "ORD-9");
        Instant start = Instant.now().minus(Duration.ofMinutes(10));
        p.deliver(List.of(event("shop", key, "x0", "ORDER_ACCEPTED", "UNKNOWN", "order-service",
                start, start, "{}", null)));
        p.detector.detectStuckSagas();
        Instant later = Instant.now();
        p.deliver(List.of(
                event("shop", key, "x1", "ORDER_CREATED", "CREATE_ORDER", "order-service",
                        start.minusSeconds(1), later, "{}", null),
                event("shop", key, "x2", "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
                        start.plusSeconds(1), later.plusMillis(1), "{}", null),
                event("shop", key, "x3", "INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service",
                        start.plusSeconds(2), later.plusMillis(2), "{}", null)));
        return p.outcome(key.canonical());
    }

    // ══ harness ═══════════════════════════════════════════════════════════════

    private static final class Outcome {
        String sagaStatus, rcaStatus, primary;
        List<String> anomalies, candidateRules;
        List<com.example.correlation.domain.rca.RcaCandidate> candidates;
        int retracted;

        @Override public String toString() {
            return "status=" + sagaStatus + " rca=" + rcaStatus + " primary=" + primary
                    + " standing=" + anomalies + " candidates=" + candidateRules + " retracted=" + retracted;
        }
    }

    private static void report(String experiment, String arm, Outcome o) {
        System.out.printf("[E2 %s] %-24s %s%n", experiment, arm, o);
    }

    private static final class Pipeline {
        final Map<String, SagaInstance> instances = new LinkedHashMap<>();
        final List<SagaEvent> events = new ArrayList<>();
        final List<Anomaly> anomalies = new ArrayList<>();
        boolean retractionEnabled = true;
        final AnomalyDetectionService detector;
        final CorrelationService correlation;
        final RcaService rca;

        Pipeline() {
            SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
            SagaEventRepository eventRepo = mock(SagaEventRepository.class);
            AnomalyRepository anomalyRepo = mock(AnomalyRepository.class);
            RcaResultRepository rcaRepo = mock(RcaResultRepository.class);
            when(sagas.findById(anyString())).thenAnswer(c -> Optional.ofNullable(instances.get(c.getArgument(0))));
            when(sagas.findByIdForUpdate(anyString())).thenAnswer(c -> Optional.ofNullable(instances.get(c.getArgument(0))));
            when(sagas.save(any())).thenAnswer(c -> {
                SagaInstance s = c.getArgument(0); instances.put(s.getSagaId(), s); return s;
            });
            when(sagas.findByStatusIn(any(), any())).thenAnswer(c -> {
                Collection<SagaStatus> st = c.getArgument(0);
                return instances.values().stream().filter(s -> st.contains(s.getStatus())).toList();
            });
            when(eventRepo.findBySourceNameAndEventId(anyString(), anyString())).thenAnswer(c ->
                    events.stream().filter(e -> e.getSourceName().equals(c.getArgument(0))
                            && e.getEventId().equals(c.getArgument(1))).findFirst());
            when(eventRepo.findBySagaIdOrderByTimestampAsc(anyString())).thenAnswer(c ->
                    events.stream().filter(e -> e.getSagaId().equals(c.getArgument(0)))
                            .sorted(Comparator.comparing(SagaEvent::getTimestamp)).toList());
            when(eventRepo.findTopBySagaIdOrderByTimestampDesc(anyString())).thenAnswer(c ->
                    events.stream().filter(e -> e.getSagaId().equals(c.getArgument(0)))
                            .max(Comparator.comparing(SagaEvent::getTimestamp)));
            when(eventRepo.save(any())).thenAnswer(c -> {
                SagaEvent e = c.getArgument(0); events.add(e); return e;
            });
            when(anomalyRepo.save(any())).thenAnswer(c -> {
                Anomaly a = c.getArgument(0);
                if (a.getId() == null) { a.setId((long) anomalies.size() + 1); anomalies.add(a); }
                return a;
            });
            when(anomalyRepo.existsBySagaIdAndAnomalyType(anyString(), any())).thenAnswer(c ->
                    anomalies.stream().anyMatch(a -> a.getSagaId().equals(c.getArgument(0))
                            && a.getAnomalyType() == c.getArgument(1) && a.getRetractedAt() == null));
            when(anomalyRepo.findStandingBySagaId(anyString())).thenAnswer(c ->
                    anomalies.stream().filter(a -> a.getSagaId().equals(c.getArgument(0))
                            && a.getRetractedAt() == null).toList());
            when(anomalyRepo.findBySagaId(anyString())).thenAnswer(c ->
                    anomalies.stream().filter(a -> a.getSagaId().equals(c.getArgument(0))).toList());
            when(anomalyRepo.retractClaims(anyString(), any(), any(), anyString())).thenAnswer(c -> {
                if (!retractionEnabled) return 0;
                Collection<AnomalyType> types = c.getArgument(1);
                int n = 0;
                for (Anomaly a : anomalies) {
                    if (a.getSagaId().equals(c.getArgument(0)) && types.contains(a.getAnomalyType())
                            && a.getRetractedAt() == null) {
                        a.setRetractedAt(c.getArgument(2)); a.setRetractionReason(c.getArgument(3)); n++;
                    }
                }
                return n;
            });
            when(rcaRepo.save(any())).thenAnswer(c -> c.getArgument(0));

            WorkflowRegistry workflows = new WorkflowRegistry(); workflows.load();
            RuleRegistry rules = new RuleRegistry(); rules.load();
            DetectionProperties properties = new DetectionProperties();
            ExecutionProjectionService projection = new ExecutionProjectionService();
            detector = new AnomalyDetectionService(sagas, eventRepo, anomalyRepo, workflows, MAPPER,
                    properties, projection);
            correlation = new CorrelationService(sagas, eventRepo, detector, new DeduplicationCache(),
                    workflows, MAPPER);
            FailureSignalRegistry failureSignals =
                    new FailureSignalRegistry(repoRoot().resolve("declarations").toString());
            failureSignals.load();
            rca = new RcaService(sagas, eventRepo, anomalyRepo, rcaRepo, workflows, MAPPER,
                    new RcaSelector(), rules, properties, mock(AlertSink.class), projection,
                    failureSignals, null);
        }

        void deliver(List<IngestedEvent> batch) {
            batch.forEach(correlation::processEvent);
        }

        Outcome outcome(String executionId) {
            RcaResult r = rca.analyze(executionId);
            Outcome o = new Outcome();
            o.sagaStatus = instances.get(executionId).getStatus().name();
            o.rcaStatus = r.getRcaStatus().name();
            o.primary = r.getRootCause();
            o.candidates = r.getCandidates();
            o.candidateRules = r.getCandidates().stream()
                    .map(c -> c.getRuleId() + ":" + c.getStatus()).toList();
            o.anomalies = anomalies.stream().filter(a -> a.getSagaId().equals(executionId))
                    .filter(a -> a.getRetractedAt() == null)
                    .map(a -> a.getAnomalyType().name()).sorted().toList();
            o.retracted = (int) anomalies.stream().filter(a -> a.getSagaId().equals(executionId))
                    .filter(a -> a.getRetractedAt() != null).count();
            return o;
        }
    }

    private static IngestedEvent event(String source, ExecutionKey key, String eventId, String type,
            String step, String service, Instant at, Instant receivedAt, String payload, String traceId) {
        return new IngestedEvent(
                new IngestedEvent.Identity(source, key, eventId, null, CorrelationBasis.DECLARED_KEY),
                null, type, step, service, at, payload, receivedAt,
                new IngestedEvent.Causality(null, null, null, traceId), null);
    }

    private static <T> List<List<T>> permutations(List<T> items) {
        if (items.size() <= 1) return List.of(new ArrayList<>(items));
        List<List<T>> out = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            List<T> rest = new ArrayList<>(items);
            T head = rest.remove(i);
            for (List<T> tail : permutations(rest)) {
                List<T> p = new ArrayList<>(); p.add(head); p.addAll(tail); out.add(p);
            }
        }
        return out;
    }

    private static JsonNode fixture(String scenario) throws Exception {
        return MAPPER.readTree(repoRoot().resolve("oracle/evidence/" + scenario + ".json").toFile());
    }

    private static Path repoRoot() {
        Path here = Path.of("").toAbsolutePath();
        return Files.exists(here.resolve("oracle")) ? here : here.getParent();
    }

    private static String nullable(JsonNode node, String key) {
        return node.path(key).isMissingNode() || node.path(key).isNull() ? null : node.path(key).asText();
    }
}
