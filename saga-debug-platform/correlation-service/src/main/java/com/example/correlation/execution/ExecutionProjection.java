package com.example.correlation.execution;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.workflow.WorkflowSpec;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A saga's event stream re-expressed as the operations that were executed and the
 * attempts each of them took.
 *
 * <p>The detectors previously read the raw event list and treated positional
 * adjacency as execution structure: the duration of a step was the gap between an
 * event and whatever event happened to sit before it in the list. That makes a
 * measurement of one operation depend on unrelated observations — inserting an
 * event that belongs to no step, or the retry events of the step itself, silently
 * changes the answer.
 *
 * <p>Here an operation's boundaries come from identity instead of position: an
 * observation belongs to a step because the workflow spec says so, and the start
 * reference is either an observed start event or the completion of the declared
 * predecessor step. Observations that map to no step are retained for reporting but
 * cannot influence any operation's timing.
 *
 * <p>Every timing therefore carries its {@link StartProvenance}, so an inferred
 * boundary is never presented as an observed one.
 */
public record ExecutionProjection(
        String sagaId,
        List<Operation> operations,
        List<SagaEvent> unmappedEvents) {

    /** How an operation's start instant was established. */
    public enum StartProvenance {
        /** The workflow declares a start event and it was observed. */
        OBSERVED_START,
        /**
         * The producer reported which event caused this step, and that event was found.
         *
         * <p>The only provenance that rests on observation of the causal link itself
         * rather than on the specification describing one. Everything below this line is
         * the platform inferring a boundary the telemetry never stated.
         */
        OBSERVED_CAUSAL_LINK,
        /** Inferred from the completion of the declared predecessor step. */
        INFERRED_FROM_PREDECESSOR,
        /**
         * Inferred from the last of several predecessors to finish.
         *
         * <p>A step behind a join cannot begin before every arm feeding it has
         * completed, so the latest of those completions is its start. Kept distinct from
         * the single-predecessor case because the inference is weaker: an arm whose
         * events were never observed would move this boundary without any sign.
         */
        INFERRED_FROM_JOIN,
        /** No start event and no usable predecessor completion; duration is unknown. */
        NO_START_REFERENCE
    }

    /** What the operation's last observation says about how it ended. */
    public enum OperationOutcome {
        SUCCEEDED,
        FAILED,
        /** Observed, but no terminal observation yet. */
        IN_FLIGHT
    }

    /**
     * One attempt at an operation. Attempts are delimited by the step's declared
     * retry events: attempt <i>k</i> ends at the <i>k</i>-th retry observation, and
     * the final attempt ends at the terminal observation.
     */
    public record Attempt(
            int ordinal,
            Instant startedAt,
            Instant endedAt,
            String startEventId,
            String endEventId) {

        public Duration duration() {
            return startedAt == null || endedAt == null ? null : Duration.between(startedAt, endedAt);
        }
    }

    /**
     * One execution of one workflow step, across all of its attempts.
     *
     * @param duration wall time from the start reference to the terminal observation,
     *                 or {@code null} when either boundary is unknown. A null duration
     *                 is an absence of evidence and must not be read as "fast".
     */
    public record Operation(
            String stepId,
            String serviceName,
            Instant startedAt,
            Instant endedAt,
            StartProvenance startProvenance,
            OperationOutcome outcome,
            List<Attempt> attempts,
            String startEventId,
            String endEventId,
            List<String> evidenceEventIds) {

        public Duration duration() {
            return startedAt == null || endedAt == null ? null : Duration.between(startedAt, endedAt);
        }

        public int attemptCount() {
            return attempts.size();
        }

        /** True when the duration rests on an observed, not inferred, start. */
        public boolean hasObservedStart() {
            return startProvenance == StartProvenance.OBSERVED_START;
        }

        public boolean hasMeasurableDuration() {
            return duration() != null;
        }
    }

    public Optional<Operation> operation(String stepId) {
        return operations.stream().filter(op -> op.stepId().equals(stepId)).findFirst();
    }

    public Map<String, Operation> byStep() {
        Map<String, Operation> map = new LinkedHashMap<>();
        for (Operation op : operations) {
            map.putIfAbsent(op.stepId(), op);
        }
        return map;
    }

    /** Operations whose duration can be compared against a declared timeout. */
    public List<Operation> measurableOperations() {
        return operations.stream().filter(Operation::hasMeasurableDuration).toList();
    }

    public static ExecutionProjection empty(String sagaId) {
        return new ExecutionProjection(sagaId, List.of(), List.of());
    }

    /** Convenience for callers that only hold a spec reference. */
    public static boolean isMapped(WorkflowSpec spec, String eventType) {
        return spec != null && spec.classifyEvent(eventType).isPresent();
    }
}
