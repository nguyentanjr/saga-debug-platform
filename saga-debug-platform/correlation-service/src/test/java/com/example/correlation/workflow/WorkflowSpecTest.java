package com.example.correlation.workflow;

import com.example.correlation.workflow.WorkflowSpec.EventRole;
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

    /** Where the map is declared it decides, including where it is silent (ROADMAP L21). */
    @Test
    void aDeclaredMapStaysAuthoritativeEvenWhereItIsSilent() {
        assertThat(spec.getExpectedStepForEvent("PAYMENT_COMPLETED")).isEqualTo("PROCESS_PAYMENT");
        assertThat(spec.getExpectedStepForEvent("PAYMENT_STARTED")).isNull();
    }
}
