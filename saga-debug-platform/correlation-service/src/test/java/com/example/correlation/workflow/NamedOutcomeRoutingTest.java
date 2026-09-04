package com.example.correlation.workflow;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.execution.ExecutionProjection;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.workflow.WorkflowSpec.EventRole;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers a workflow whose steps have more than two possible outcomes.
 *
 * <p>Assessing a loan approves, refers to a human, or rejects. Only the third is a
 * failure; the other two are the step doing its job and routing differently. With a
 * fixed completed/failed vocabulary two of the three had to be miscalled, and every
 * transition was checked against one of two conditions. Outcomes are now named by the
 * workflow and matched against the transition declared for that name.
 */
class NamedOutcomeRoutingTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final WorkflowSpec spec =
            WorkflowSpecTestFixture.load("workflows/loan-approval-workflow.yaml");
    private final ExecutionProjectionService projectionService = new ExecutionProjectionService();

    @Test
    void eachDeclaredOutcomeIsClassifiedUnderItsOwnName() {
        assertThat(spec.classifyEvent("ASSESSMENT_APPROVED"))
                .contains(new EventRole("ASSESS_RISK", EventRole.Role.COMPLETED, "APPROVED"));
        assertThat(spec.classifyEvent("ASSESSMENT_FLAGGED"))
                .contains(new EventRole("ASSESS_RISK", EventRole.Role.COMPLETED, "MANUAL_REVIEW"));
        assertThat(spec.classifyEvent("ASSESSMENT_REJECTED"))
                .contains(new EventRole("ASSESS_RISK", EventRole.Role.FAILED, "REJECTED"));
    }

    @Test
    void anOutcomeThatRoutesOnwardIsNotTreatedAsAFailure() {
        // The distinction the old vocabulary could not draw: being referred to a human
        // is not the assessment failing.
        assertThat(spec.classifyEvent("ASSESSMENT_FLAGGED").orElseThrow().role())
                .isEqualTo(EventRole.Role.COMPLETED);
        assertThat(statusAfter("ASSESSMENT_FLAGGED")).contains(SagaStatus.IN_PROGRESS);
    }

    @Test
    void anOutcomeRoutingToTheFailureMarkerEndsTheExecution() {
        assertThat(statusAfter("ASSESSMENT_REJECTED")).contains(SagaStatus.FAILED);
        assertThat(statusAfter("REVIEW_REJECTED")).contains(SagaStatus.FAILED);
    }

    @Test
    void theSameStepRoutesDifferentlyPerOutcome() {
        assertThat(spec.judgeTransition("ASSESS_RISK", "DISBURSE_FUNDS", "APPROVED"))
                .isEqualTo(WorkflowSpec.TransitionVerdict.VALID);
        assertThat(spec.judgeTransition("ASSESS_RISK", "HUMAN_REVIEW", "MANUAL_REVIEW"))
                .isEqualTo(WorkflowSpec.TransitionVerdict.VALID);

        // Right destination, wrong outcome: approval does not route to human review.
        assertThat(spec.judgeTransition("ASSESS_RISK", "HUMAN_REVIEW", "APPROVED"))
                .isEqualTo(WorkflowSpec.TransitionVerdict.INVALID);
    }

    @Test
    void bothBranchesReachTheSameSuccessfulEnd() {
        assertThat(statusAfterSequence(
                "APPLICATION_SUBMITTED", "ASSESSMENT_APPROVED", "FUNDS_DISBURSED"))
                .containsExactly(SagaStatus.IN_PROGRESS, SagaStatus.IN_PROGRESS, SagaStatus.COMPLETED);

        assertThat(statusAfterSequence(
                "APPLICATION_SUBMITTED", "ASSESSMENT_FLAGGED", "REVIEW_APPROVED", "FUNDS_DISBURSED"))
                .containsExactly(SagaStatus.IN_PROGRESS, SagaStatus.IN_PROGRESS,
                        SagaStatus.IN_PROGRESS, SagaStatus.COMPLETED);
    }

    @Test
    void aStepReachedThroughEitherBranchIsMeasuredFromWhicheverBranchRanIt() {
        // Straight through: disbursement starts when the assessment approved.
        ExecutionProjection direct = project(
                event("d1", "APPLICATION_SUBMITTED", T0),
                event("d2", "ASSESSMENT_APPROVED", T0.plusSeconds(4)),
                event("d3", "FUNDS_DISBURSED", T0.plusSeconds(9)));
        assertThat(direct.operation("DISBURSE_FUNDS").orElseThrow().duration().toSeconds())
                .isEqualTo(5);

        // Via review: it starts when the reviewer approved, not when scoring finished.
        ExecutionProjection viaReview = project(
                event("r1", "APPLICATION_SUBMITTED", T0),
                event("r2", "ASSESSMENT_FLAGGED", T0.plusSeconds(4)),
                event("r3", "REVIEW_APPROVED", T0.plusSeconds(600)),
                event("r4", "FUNDS_DISBURSED", T0.plusSeconds(605)));
        assertThat(viaReview.operation("DISBURSE_FUNDS").orElseThrow().duration().toSeconds())
                .isEqualTo(5);
    }

    @Test
    void aBranchNeverTakenIsSimplyAbsent() {
        ExecutionProjection direct = project(
                event("d1", "APPLICATION_SUBMITTED", T0),
                event("d2", "ASSESSMENT_APPROVED", T0.plusSeconds(4)),
                event("d3", "FUNDS_DISBURSED", T0.plusSeconds(9)));

        // The manual-review arm was not exercised. It is missing, not overdue.
        assertThat(direct.operation("HUMAN_REVIEW")).isEmpty();
        assertThat(direct.unmappedEvents()).isEmpty();
    }

    @Test
    void convergingBranchesAreNotMistakenForAJoin() {
        // DISBURSE_FUNDS is reachable from the assessment directly and from the review,
        // but only one of those paths is ever taken. Requiring both, as a join does,
        // would leave every execution looking like it lost an arm.
        assertThat(spec.predecessorSteps("DISBURSE_FUNDS"))
                .containsExactlyInAnyOrder("ASSESS_RISK", "HUMAN_REVIEW");
        assertThat(spec.isJoin("DISBURSE_FUNDS")).isFalse();

        // What separates them: the review is itself downstream of the assessment.
        assertThat(spec.isUpstreamOf("ASSESS_RISK", "HUMAN_REVIEW")).isTrue();
    }

    @Test
    void theConventionalLifecycleKeysStillWork() {
        // Steps declaring completed/failed keep working unchanged, mapped to the two
        // conventional outcome names.
        assertThat(spec.classifyEvent("FUNDS_DISBURSED"))
                .contains(new EventRole("DISBURSE_FUNDS", EventRole.Role.COMPLETED, "SUCCESS"));
        assertThat(spec.classifyEvent("DISBURSEMENT_FAILED"))
                .contains(new EventRole("DISBURSE_FUNDS", EventRole.Role.FAILED, "FAILED"));
    }

    @Test
    void outcomeEventsCountAsDeclaredEventsOfTheWorkflow() {
        assertThat(spec.declaredEventTypes())
                .contains("ASSESSMENT_APPROVED", "ASSESSMENT_FLAGGED", "ASSESSMENT_REJECTED",
                        "REVIEW_APPROVED", "FUNDS_DISBURSED");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private Optional<SagaStatus> statusAfter(String eventType) {
        return spec.classifyEvent(eventType).flatMap(spec::statusAfter);
    }

    private List<SagaStatus> statusAfterSequence(String... eventTypes) {
        return java.util.Arrays.stream(eventTypes)
                .map(type -> statusAfter(type).orElseThrow(
                        () -> new AssertionError("no status derived for " + type)))
                .toList();
    }

    private ExecutionProjection project(SagaEvent... events) {
        return projectionService.project("saga-l1", List.of(events), spec);
    }

    private static SagaEvent event(String eventId, String eventType, Instant timestamp) {
        SagaEvent event = new SagaEvent();
        event.setEventId(eventId);
        event.setSagaId("saga-l1");
        event.setEventType(eventType);
        event.setServiceName("test");
        event.setTimestamp(timestamp);
        event.setPayload("{}");
        event.setReceivedAt(timestamp);
        return event;
    }
}
