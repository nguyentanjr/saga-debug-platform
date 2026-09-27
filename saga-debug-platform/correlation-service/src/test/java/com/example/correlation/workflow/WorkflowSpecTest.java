package com.example.correlation.workflow;

import com.example.correlation.workflow.WorkflowSpec.EventRole;
import com.example.correlation.workflow.WorkflowSpec.TransitionVerdict;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks in what the engine reads out of the workflow specification, so knowledge that
 * belongs in the spec cannot quietly migrate back into Java constants.
 */
class WorkflowSpecTest {

    private final WorkflowSpec spec = WorkflowSpecTestFixture.orderSaga();

    @Test
    void classifiesEventsByTheirDeclaredLifecycleRole() {
        assertThat(spec.classifyEvent("PAYMENT_COMPLETED"))
                .contains(new EventRole("PROCESS_PAYMENT", EventRole.Role.COMPLETED));
        assertThat(spec.classifyEvent("PAYMENT_FAILED"))
                .contains(new EventRole("PROCESS_PAYMENT", EventRole.Role.FAILED));
        assertThat(spec.classifyEvent("PAYMENT_RETRY"))
                .contains(new EventRole("PROCESS_PAYMENT", EventRole.Role.RETRY));
        assertThat(spec.classifyEvent("PAYMENT_REFUNDED"))
                .contains(new EventRole("REFUND_PAYMENT", EventRole.Role.COMPLETED));
    }

    @Test
    void anEventDeclaredAsBothStartAndCompletionIsReadAsCompletion() {
        // CREATE_ORDER declares ORDER_CREATED for started and completed alike; reading it
        // as a completion is what lets it anchor the next step's start.
        assertThat(spec.classifyEvent("ORDER_CREATED"))
                .contains(new EventRole("CREATE_ORDER", EventRole.Role.COMPLETED));
    }

    @Test
    void unknownEventTypeIsNotForcedIntoAStep() {
        assertThat(spec.classifyEvent("AUDIT_PING")).isEmpty();
        assertThat(spec.classifyEvent(null)).isEmpty();
    }

    @Test
    void predecessorComesFromSuccessTransitionsOnly() {
        assertThat(spec.predecessorStep("PROCESS_PAYMENT")).contains("CREATE_ORDER");
        assertThat(spec.predecessorStep("RESERVE_INVENTORY")).contains("PROCESS_PAYMENT");

        // STARTED -> CREATE_ORDER carries no SUCCESS condition, and STARTED is not a step.
        assertThat(spec.predecessorStep("CREATE_ORDER")).isEmpty();
    }

    @Test
    void retryEventTypesComeFromTheSpecNotFromJava() {
        assertThat(spec.retryEventTypes()).containsExactly("PAYMENT_RETRY");
    }

    @Test
    void declaredRetryPolicyIsParsedRatherThanDropped() {
        // retryPolicy previously had no field on StepSpec and was silently discarded,
        // so the declared attempt budget could not be compared against observations.
        assertThat(spec.declaredMaxAttempts("PROCESS_PAYMENT")).contains(3);
        assertThat(spec.declaredMaxAttempts("CREATE_ORDER")).isEmpty();
    }

    @Test
    void stepTimeoutsAreReadFromForwardAndCompensationSteps() {
        assertThat(spec.getStepTimeoutSeconds("PROCESS_PAYMENT")).isEqualTo(10);
        assertThat(spec.getStepTimeoutSeconds("RESERVE_INVENTORY")).isEqualTo(5);
        assertThat(spec.getStepTimeoutSeconds("REFUND_PAYMENT")).isEqualTo(10);
    }

    /** L16: a workflow that never wrote validEventTypes still places its events in steps. */
    @Test
    void withoutADeclaredMapTheStepComesFromTheStepsOwnEvents() {
        WorkflowSpec undeclared = WorkflowSpecTestFixture.orderSaga();
        undeclared.setValidEventTypes(null);

        assertThat(undeclared.getExpectedStepForEvent("PAYMENT_COMPLETED")).isEqualTo("PROCESS_PAYMENT");
        assertThat(undeclared.getExpectedStepForEvent("PAYMENT_STARTED")).isEqualTo("PROCESS_PAYMENT");
        assertThat(undeclared.getExpectedStepForEvent("PAYMENT_REFUNDED")).isEqualTo("REFUND_PAYMENT");
        assertThat(undeclared.getExpectedStepForEvent("AUDIT_PING")).isNull();
    }

    // ── L22: which edges a FAILED outcome may take ────────────────────────────

    /**
     * PROCESS_PAYMENT → RESERVE_INVENTORY is declared for SUCCESS only. A failed payment
     * followed by inventory work is the run contradicting its specification; it used to be
     * judged VALID because any edge between the two steps was accepted for FAILED.
     */
    @Test
    void aFailedOutcomeDoesNotTakeAnEdgeDeclaredOnlyForSuccess() {
        assertThat(spec.explainTransition("PROCESS_PAYMENT", "RESERVE_INVENTORY",
                WorkflowSpec.FAILED_OUTCOME).verdict()).isEqualTo(TransitionVerdict.INVALID);
    }

    /** What that branch was written for still holds: an edge with no condition takes any outcome. */
    @Test
    void aFailedOutcomeStillTakesAnEdgeDeclaredWithoutACondition() {
        WorkflowSpec unconditioned = parse("""
                name: UNCONDITIONED
                version: "1.0"
                steps:
                  - id: CHARGE
                    service: a
                    events: {completed: CHARGED, failed: CHARGE_FAILED}
                  - id: NOTIFY
                    service: b
                    events: {completed: NOTIFIED}
                transitions:
                  - {from: STARTED, to: CHARGE}
                  - {from: CHARGE, to: NOTIFY}
                  - {from: NOTIFY, to: COMPLETED, condition: SUCCESS}
                """);

        assertThat(unconditioned.explainTransition("CHARGE", "NOTIFY",
                WorkflowSpec.FAILED_OUTCOME).verdict()).isEqualTo(TransitionVerdict.VALID);
    }

    /** And a failure routed into compensation is still a declared handoff. */
    @Test
    void aFailedOutcomeStillTakesTheCompensationItIsRoutedTo() {
        assertThat(spec.explainTransition("RESERVE_INVENTORY", "REFUND_PAYMENT",
                WorkflowSpec.FAILED_OUTCOME).verdict()).isEqualTo(TransitionVerdict.VALID);
    }

    private static WorkflowSpec parse(String yaml) {
        try {
            return new ObjectMapper(new YAMLFactory()).readValue(yaml, WorkflowSpec.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test specification", e);
        }
    }

    /**
     * L21: ORDER_SAGA's map leaves out every started and retry event and two failed ones.
     * Where it is silent the steps answer, instead of the event belonging to no step.
     */
    @Test
    void whereTheDeclaredMapIsSilentTheStepsAnswer() {
        assertThat(spec.getExpectedStepForEvent("PAYMENT_STARTED")).isEqualTo("PROCESS_PAYMENT");
        assertThat(spec.getExpectedStepForEvent("PAYMENT_RETRY")).isEqualTo("PROCESS_PAYMENT");
        assertThat(spec.getExpectedStepForEvent("INVENTORY_STARTED")).isEqualTo("RESERVE_INVENTORY");
        assertThat(spec.getExpectedStepForEvent("ORDER_FAILED")).isEqualTo("CREATE_ORDER");
        assertThat(spec.getExpectedStepForEvent("REFUND_FAILED")).isEqualTo("REFUND_PAYMENT");
    }

    /** Where the map speaks it still decides, even against the steps. No shipped spec disagrees. */
    @Test
    void whereTheDeclaredMapSpeaksItStillDecides() {
        WorkflowSpec overridden = WorkflowSpecTestFixture.orderSaga();
        java.util.Map<String, String> map = new java.util.LinkedHashMap<>(overridden.getValidEventTypes());
        map.put("PAYMENT_COMPLETED", "RESERVE_INVENTORY");
        overridden.setValidEventTypes(map);

        assertThat(overridden.getExpectedStepForEvent("PAYMENT_COMPLETED")).isEqualTo("RESERVE_INVENTORY");
    }
}
