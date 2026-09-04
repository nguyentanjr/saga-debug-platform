package com.example.correlation.workflow;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The defects this catches are all silent ones: nothing throws, the engine starts, and
 * the wrongness only shows up later as a saga that never finishes or a diagnosis that
 * makes no sense. Each test therefore names the runtime symptom it prevents.
 */
class WorkflowSpecValidatorTest {

    // ── the shipped spec ──────────────────────────────────────────────────────

    @Test
    void theShippedOrderSagaIsValid() {
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();

        WorkflowSpec order = registry.byType("ORDER_SAGA").orElseThrow();
        WorkflowSpecValidator.Report report = WorkflowSpecValidator.validate(order);

        assertThat(report.errors()).isEmpty();
        assertThat(report.isUsable()).isTrue();
    }

    // ── errors: the spec cannot be interpreted as written ─────────────────────

    @Test
    void anUnreachableStepIsAnError() {
        // Symptom without this check: the step is never entered, so its timeout never
        // fires and nothing ever reports that it did not run.
        WorkflowSpec spec = minimal();
        spec.getSteps().add(step("ORPHANED_STEP", Map.of("completed", "ORPHANED_DONE")));

        assertThat(errorsOf(spec)).anySatisfy(
                e -> assertThat(e).contains("ORPHANED_STEP").contains("unreachable from STARTED"));
    }

    @Test
    void aTransitionToAnUndeclaredStepIsAnError() {
        WorkflowSpec spec = minimal();
        spec.getTransitions().add(transition("STEP_A", "GHOST_STEP", "SUCCESS"));

        assertThat(errorsOf(spec)).anySatisfy(
                e -> assertThat(e).contains("unknown step").contains("GHOST_STEP"));
    }

    @Test
    void aMissingStartTransitionIsAnError() {
        // Symptom without this check: no event is recognised as starting the workflow,
        // so every execution is reported as an orphan.
        WorkflowSpec spec = minimal();
        spec.getTransitions().removeIf(t -> "STARTED".equals(t.getFrom()));

        assertThat(errorsOf(spec)).anySatisfy(e -> assertThat(e).contains("STARTED"));
    }

    @Test
    void aCompensationNamingANonexistentStepIsAnError() {
        WorkflowSpec spec = minimal();
        spec.getSteps().get(0).setCompensation("NO_SUCH_COMPENSATION");

        assertThat(errorsOf(spec)).anySatisfy(
                e -> assertThat(e).contains("NO_SUCH_COMPENSATION").contains("not a declared step"));
    }

    @Test
    void aDuplicateStepIdIsAnError() {
        WorkflowSpec spec = minimal();
        spec.getSteps().add(step("STEP_A", Map.of("completed", "A_AGAIN")));

        assertThat(errorsOf(spec)).anySatisfy(e -> assertThat(e).contains("duplicate step id"));
    }

    @Test
    void validEventTypesPointingAtAnUnknownStepIsAnError() {
        WorkflowSpec spec = minimal();
        spec.setValidEventTypes(Map.of("A_DONE", "NOT_A_STEP"));

        assertThat(errorsOf(spec)).anySatisfy(
                e -> assertThat(e).contains("unknown step").contains("NOT_A_STEP"));
    }

    @Test
    void aSpecWithNoStepsIsAnError() {
        WorkflowSpec spec = new WorkflowSpec();
        spec.setName("EMPTY");

        assertThat(errorsOf(spec)).anySatisfy(e -> assertThat(e).contains("no steps"));
    }

    @Test
    void aNullSpecIsReportedRatherThanThrowing() {
        assertThat(WorkflowSpecValidator.validate(null).isUsable()).isFalse();
    }

    // ── warnings: interpretable, but the engine will know less ────────────────

    @Test
    void aStepWithNoWayOutIsAWarningNotAnError() {
        // Interpretable, so the engine should still run — it will simply never judge an
        // execution reaching this step as finished.
        WorkflowSpec spec = minimal();
        spec.getSteps().add(step("DEAD_END", Map.of("completed", "DEAD_END_DONE")));
        spec.getTransitions().add(transition("STEP_A", "DEAD_END", "SUCCESS"));

        WorkflowSpecValidator.Report report = WorkflowSpecValidator.validate(spec);

        assertThat(report.isUsable()).isTrue();
        assertThat(report.warnings()).anySatisfy(
                w -> assertThat(w).contains("DEAD_END").contains("no outgoing transition"));
    }

    @Test
    void anUnroutedNamedOutcomeIsAWarning() {
        // The case LOAN_APPROVAL exists to exercise: a step declaring MANUAL_REVIEW that
        // no transition routes leaves the saga's status undetermined for that outcome.
        WorkflowSpec spec = minimal();
        WorkflowSpec.StepSpec stepA = spec.getSteps().get(0);
        stepA.setOutcomes(Map.of("MANUAL_REVIEW", "A_FLAGGED"));

        WorkflowSpecValidator.Report report = WorkflowSpecValidator.validate(spec);

        assertThat(report.isUsable()).isTrue();
        assertThat(report.warnings()).anySatisfy(
                w -> assertThat(w).contains("MANUAL_REVIEW").contains("no transition"));
    }

    @Test
    void validEventTypesListingAnEventNoStepDeclaresIsAWarning() {
        WorkflowSpec spec = minimal();
        spec.setValidEventTypes(Map.of("NEVER_DECLARED", "STEP_A"));

        WorkflowSpecValidator.Report report = WorkflowSpecValidator.validate(spec);

        assertThat(report.isUsable()).isTrue();
        assertThat(report.warnings()).anySatisfy(
                w -> assertThat(w).contains("NEVER_DECLARED").contains("no step declares"));
    }

    @Test
    void aSpecWithNoTerminalTargetIsAWarning() {
        WorkflowSpec spec = minimal();
        spec.getTransitions().removeIf(t -> "COMPLETED".equals(t.getTo()));

        WorkflowSpecValidator.Report report = WorkflowSpecValidator.validate(spec);

        assertThat(report.isUsable()).isTrue();
        assertThat(report.warnings()).anySatisfy(
                w -> assertThat(w).contains("can ever be judged finished"));
    }

    @Test
    void aValidSpecReportsNothing() {
        WorkflowSpecValidator.Report report = WorkflowSpecValidator.validate(minimal());

        assertThat(report.errors()).isEmpty();
        assertThat(report.warnings()).isEmpty();
        assertThat(report.isClean()).isTrue();
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    /** STARTED -> STEP_A -> COMPLETED. The smallest spec that is fully well formed. */
    private static WorkflowSpec minimal() {
        WorkflowSpec spec = new WorkflowSpec();
        spec.setName("MINIMAL");
        spec.setVersion("1.0");

        List<WorkflowSpec.StepSpec> steps = new ArrayList<>();
        steps.add(step("STEP_A", Map.of("started", "A_STARTED", "completed", "A_DONE")));
        spec.setSteps(steps);
        spec.setCompensationSteps(new ArrayList<>());

        List<WorkflowSpec.TransitionSpec> transitions = new ArrayList<>();
        transitions.add(transition("STARTED", "STEP_A", null));
        transitions.add(transition("STEP_A", "COMPLETED", "SUCCESS"));
        spec.setTransitions(transitions);

        return spec;
    }

    private static WorkflowSpec.StepSpec step(String id, Map<String, String> events) {
        WorkflowSpec.StepSpec step = new WorkflowSpec.StepSpec();
        step.setId(id);
        step.setService("test-service");
        step.setEvents(new java.util.LinkedHashMap<>(events));
        return step;
    }

    private static WorkflowSpec.TransitionSpec transition(String from, String to, String condition) {
        WorkflowSpec.TransitionSpec t = new WorkflowSpec.TransitionSpec();
        t.setFrom(from);
        t.setTo(to);
        t.setCondition(condition);
        return t;
    }

    private static List<String> errorsOf(WorkflowSpec spec) {
        return WorkflowSpecValidator.validate(spec).errors();
    }
}
