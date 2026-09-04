package com.example.correlation.workflow;

import com.example.correlation.domain.SagaStatus;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowSpec {

    private String name;
    private String version;
    private List<StepSpec> steps;
    private List<StepSpec> compensationSteps;
    private List<TransitionSpec> transitions;
    private Map<String, String> validEventTypes;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    public List<StepSpec> getSteps() { return steps; }
    public void setSteps(List<StepSpec> steps) { this.steps = steps; }

    public List<StepSpec> getCompensationSteps() { return compensationSteps; }
    public void setCompensationSteps(List<StepSpec> compensationSteps) { this.compensationSteps = compensationSteps; }

    public List<TransitionSpec> getTransitions() { return transitions; }
    public void setTransitions(List<TransitionSpec> transitions) { this.transitions = transitions; }

    public Map<String, String> getValidEventTypes() { return validEventTypes; }
    public void setValidEventTypes(Map<String, String> validEventTypes) { this.validEventTypes = validEventTypes; }


    public String getExpectedStepForEvent(String eventType) {
        return validEventTypes != null ? validEventTypes.get(eventType) : null;
    }


    /**
     * Which step an event belongs to, what it says about that step, and under which
     * declared outcome.
     *
     * <p>{@code role} answers "did the step finish, and did it do its job" — the only
     * two things the detectors need. {@code outcome} carries the workflow's own name for
     * what happened, which is what transitions are matched on. For the conventional
     * lifecycle keys these coincide ({@code completed} is outcome SUCCESS,
     * {@code failed} is outcome FAILED), but a workflow may declare any number of named
     * outcomes and route each one differently.
     */
    public record EventRole(String stepId, Role role, String outcome) {
        public enum Role { STARTED, COMPLETED, FAILED, RETRY }

        public EventRole(String stepId, Role role) {
            this(stepId, role, defaultOutcome(role));
        }

        private static String defaultOutcome(Role role) {
            return switch (role) {
                case COMPLETED -> SUCCESS_OUTCOME;
                case FAILED -> FAILED_OUTCOME;
                case STARTED, RETRY -> null;
            };
        }
    }

    /** Conventional outcome names. A workflow may declare others alongside them. */
    public static final String SUCCESS_OUTCOME = "SUCCESS";
    public static final String FAILED_OUTCOME = "FAILED";


    /**
     * Classifies an event against the declared step lifecycles.
     *
     * <p>Resolved from the {@code events} map of each forward and compensation step,
     * which carries the lifecycle role, rather than from {@code validEventTypes},
     * which only carries the step. A step may declare the same event type for more
     * than one role (CREATE_ORDER uses ORDER_CREATED as both {@code started} and
     * {@code completed}); the most conclusive role wins, so such an event is read as
     * a completion and can serve as a predecessor reference.
     *
     * @return the role, or empty when no step declares this event type
     */
    public Optional<EventRole> classifyEvent(String eventType) {
        if (eventType == null) return Optional.empty();

        EventRole best = null;
        for (StepSpec step : allSteps()) {
            if (step.getId() == null) continue;

            if (step.getEvents() != null) {
                for (Map.Entry<String, String> entry : step.getEvents().entrySet()) {
                    if (!eventType.equals(entry.getValue())) continue;
                    EventRole.Role role = toRole(entry.getKey());
                    if (role == null) continue;
                    best = better(best, new EventRole(step.getId(), role));
                }
            }

            if (step.getOutcomes() != null) {
                for (Map.Entry<String, String> entry : step.getOutcomes().entrySet()) {
                    if (!eventType.equals(entry.getValue())) continue;
                    String outcome = entry.getKey();
                    best = better(best, new EventRole(
                            step.getId(), roleOfOutcome(step.getId(), outcome), outcome));
                }
            }
        }
        return Optional.ofNullable(best);
    }


    /**
     * Whether a named outcome means the step did its job.
     *
     * <p>Read from where the outcome routes: an outcome whose transition lands on the
     * FAILED marker is a failure, anything else is the step finishing — including an
     * outcome that routes to a different branch. A loan assessment returning
     * MANUAL_REVIEW completed successfully; it simply routed elsewhere.
     */
    private EventRole.Role roleOfOutcome(String stepId, String outcome) {
        if (FAILED_OUTCOME.equals(outcome)) return EventRole.Role.FAILED;
        boolean routesToFailure = outgoingTargets(stepId, outcome).stream()
                .anyMatch(target -> TERMINAL_TARGETS.get(target) == SagaStatus.FAILED);
        return routesToFailure ? EventRole.Role.FAILED : EventRole.Role.COMPLETED;
    }

    private static EventRole better(EventRole current, EventRole candidate) {
        if (current == null) return candidate;
        return rolePrecedence(candidate.role()) < rolePrecedence(current.role())
                ? candidate : current;
    }


    /**
     * The step that, on success, hands off to {@code stepId}.
     *
     * <p>Answers only for the unambiguous case. Steps reached through a join or through
     * a branch merge have several, and {@link #predecessorSteps} together with
     * {@link #isJoin} describe those.
     *
     * @return the predecessor step id, or empty when none is declared or when the
     *         declaration is ambiguous (several predecessors, i.e. a join)
     */
    public Optional<String> predecessorStep(String stepId) {
        if (transitions == null || stepId == null) return Optional.empty();

        List<String> candidates = predecessorSteps(stepId);
        return candidates.size() == 1 ? Optional.of(candidates.get(0)) : Optional.empty();
    }


    /**
     * Every step that hands off to {@code stepId} on success.
     *
     * <p>{@link #predecessorStep} answers only when there is exactly one, because a
     * single predecessor is the only case where "the previous step" is meaningful. A
     * join has several, and its start is when the last of them finished — which needs
     * the whole list, not a pick.
     */
    public List<String> predecessorSteps(String stepId) {
        if (transitions == null || stepId == null) return List.of();
        return transitions.stream()
                .filter(t -> stepId.equals(t.getTo()))
                .map(TransitionSpec::getFrom)
                .filter(from -> from != null && hasStep(from))
                .distinct()
                .toList();
    }


    /**
     * Whether a step waits for all its predecessors or for whichever one ran.
     *
     * <p>A step with several predecessors is either a join, where independent arms must
     * all finish, or the point where alternative branches come back together, where only
     * one arm ever runs. Treating the second as the first would leave every execution
     * looking like it was missing an arm.
     *
     * <p>The two are told apart by whether the predecessors can reach each other: arms of
     * a fan-out are independent, so neither is upstream of the other and both must
     * complete. Branches of a decision lie on the same chain — the decision step routes
     * one way directly and the other way through an intermediate step — so one is
     * upstream of the other and exactly one of them runs.
     */
    public boolean isJoin(String stepId) {
        List<String> predecessors = predecessorSteps(stepId);
        if (predecessors.size() < 2) return false;

        for (String first : predecessors) {
            for (String second : predecessors) {
                if (first.equals(second)) continue;
                if (isUpstreamOf(first, second)) return false;
            }
        }
        return true;
    }


    public boolean hasStep(String stepId) {
        return allSteps().stream().anyMatch(s -> s.getId() != null && s.getId().equals(stepId));
    }


    private List<StepSpec> allSteps() {
        List<StepSpec> all = new ArrayList<>();
        if (steps != null) all.addAll(steps);
        if (compensationSteps != null) all.addAll(compensationSteps);
        return all;
    }

    private static EventRole.Role toRole(String eventsKey) {
        return switch (eventsKey) {
            case "started" -> EventRole.Role.STARTED;
            case "completed" -> EventRole.Role.COMPLETED;
            case "failed" -> EventRole.Role.FAILED;
            case "retry" -> EventRole.Role.RETRY;
            default -> null;
        };
    }

    private static int rolePrecedence(EventRole.Role role) {
        return switch (role) {
            case COMPLETED -> 0;
            case FAILED -> 1;
            case RETRY -> 2;
            case STARTED -> 3;
        };
    }


    /** Whether a transition is declared, not declared, or cannot be judged. */
    public enum TransitionVerdict { VALID, INVALID, UNKNOWN }


    /**
     * Judges an observed step-to-step transition against the declared graph.
     *
     * <p>Returns three outcomes rather than a boolean. The previous version answered
     * {@code true} when the workflow declared no transitions at all, so a spec that said
     * nothing was indistinguishable from a spec that approved everything. UNKNOWN keeps
     * "no basis to judge" separate from "declared and allowed", and only INVALID raises
     * an anomaly.
     */
    public TransitionVerdict judgeTransition(String fromStep, String toStep, String condition) {
        if (transitions == null || transitions.isEmpty()) {
            return TransitionVerdict.UNKNOWN;
        }
        boolean anyFromThisStep = transitions.stream()
                .anyMatch(t -> t.getFrom() != null && t.getFrom().equals(fromStep));
        if (!anyFromThisStep) {
            return TransitionVerdict.UNKNOWN;
        }

        boolean exactMatch = transitions.stream().anyMatch(t ->
                t.getFrom().equals(fromStep) &&
                t.getTo().equals(toStep) &&
                (condition == null || condition.equals(t.getCondition())));
        if (exactMatch) return TransitionVerdict.VALID;

        // A failure may route to a compensation target declared without a condition.
        if ("FAILED".equals(condition)) {
            boolean targetReachable = transitions.stream().anyMatch(t ->
                    t.getFrom().equals(fromStep) && t.getTo().equals(toStep));
            if (targetReachable) return TransitionVerdict.VALID;
        }

        return TransitionVerdict.INVALID;
    }


    /** Pass/fail reading for callers that do not distinguish UNKNOWN; UNKNOWN passes. */
    public boolean isValidTransition(String fromStep, String toStep, String condition) {
        return judgeTransition(fromStep, toStep, condition) != TransitionVerdict.INVALID;
    }


    /**
     * Whether {@code candidate} lies on the declared SUCCESS path leading into
     * {@code step} — whether work at the candidate step had to happen before work here.
     *
     * <p>Used to tell "earlier in this execution" from "earlier and on the same
     * dependency path". Two steps on unrelated branches can be ordered in time without
     * either being upstream of the other.
     */
    public boolean isUpstreamOf(String candidate, String step) {
        if (candidate == null || step == null || candidate.equals(step)) return false;

        // Breadth-first over every predecessor, so a step reachable through one arm of a
        // join still counts as upstream. Walking a single chain would miss it.
        Set<String> visited = new LinkedHashSet<>();
        Deque<String> frontier = new ArrayDeque<>(predecessorSteps(step));
        while (!frontier.isEmpty()) {
            String current = frontier.poll();
            if (current == null || !visited.add(current)) continue;
            if (candidate.equals(current)) return true;
            frontier.addAll(predecessorSteps(current));
        }
        return false;
    }


    public int getStepTimeoutSeconds(String stepId) {
        if (steps != null) {
            for (StepSpec step : steps) {
                if (step.getId().equals(stepId)) {
                    return parseTimeout(step.getTimeout());
                }
            }
        }
        if (compensationSteps != null) {
            for (StepSpec step : compensationSteps) {
                if (step.getId().equals(stepId)) {
                    return parseTimeout(step.getTimeout());
                }
            }
        }
        return 30; 
    }

    private int parseTimeout(String timeout) {
        if (timeout == null) return 30;
        timeout = timeout.trim().toLowerCase();
        if (timeout.endsWith("s")) {
            return Integer.parseInt(timeout.replace("s", ""));
        } else if (timeout.endsWith("m")) {
            return Integer.parseInt(timeout.replace("m", "")) * 60;
        }
        return 30;
    }

    

    /**
     * Every event type any step declares as its retry signal.
     *
     * <p>Replaces a hard-coded {@code PAYMENT_RETRY} constant in the detectors: which
     * events mean "attempted again" is a property of the workflow, not of the engine.
     */
    public Set<String> retryEventTypes() {
        return allSteps().stream()
                .map(StepSpec::getEvents)
                .filter(Objects::nonNull)
                .map(events -> events.get("retry"))
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }


    /**
     * The attempt budget the workflow declares for a step, if any.
     *
     * <p>Read so that retry findings can state how the observed count compares with
     * the declared policy, instead of only against an engine-side threshold.
     */
    public Optional<Integer> declaredMaxAttempts(String stepId) {
        return allSteps().stream()
                .filter(s -> stepId != null && stepId.equals(s.getId()))
                .map(StepSpec::getRetryPolicy)
                .filter(Objects::nonNull)
                .map(RetryPolicySpec::getMaxAttempts)
                .filter(Objects::nonNull)
                .findFirst();
    }



    /**
     * Pseudo-steps that {@code transitions} uses to name a saga outcome rather than a
     * unit of work. They are the bridge between the declared graph and SagaStatus.
     */
    private static final Map<String, SagaStatus> TERMINAL_TARGETS = Map.of(
            "COMPLETED", SagaStatus.COMPLETED,
            "FAILED", SagaStatus.FAILED,
            "COMPENSATING", SagaStatus.COMPENSATING,
            "COMPENSATED", SagaStatus.COMPENSATED);

    private static final String START_MARKER = "STARTED";


    /**
     * The event types that begin an execution of this workflow.
     *
     * <p>Derived from the {@code STARTED -> firstStep} transition rather than declared
     * separately, so it cannot disagree with the graph. Replaces the hard-coded
     * {@code "ORDER_CREATED"} check that decided whether an event was an orphan.
     */
    public Set<String> startEventTypes() {
        if (transitions == null) return Set.of();
        return transitions.stream()
                .filter(t -> START_MARKER.equals(t.getFrom()))
                .map(TransitionSpec::getTo)
                .filter(Objects::nonNull)
                .flatMap(stepId -> lifecycleEvents(stepId).stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }


    /** Every event type any step of this workflow declares, in any role. */
    public Set<String> declaredEventTypes() {
        return allSteps().stream()
                .flatMap(WorkflowSpec::allDeclaredEvents)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static Stream<String> allDeclaredEvents(StepSpec step) {
        return Stream.concat(
                        step.getEvents() != null ? step.getEvents().values().stream() : Stream.empty(),
                        step.getOutcomes() != null ? step.getOutcomes().values().stream() : Stream.empty())
                .filter(Objects::nonNull);
    }


    public boolean isCompensationStep(String stepId) {
        return compensationSteps != null && compensationSteps.stream()
                .anyMatch(s -> s.getId() != null && s.getId().equals(stepId));
    }


    /**
     * The saga status implied by an observation, or empty when the observation says
     * nothing about it.
     *
     * <p>This is where the workflow's shape replaces hard-coded event names. Whether a
     * step's success ends the saga is read from its outgoing SUCCESS transition: if it
     * points at the COMPLETED marker the saga is done, if it points at another step the
     * saga is still running. The previous implementation answered the same question
     * with {@code COMPLETED_EVENTS = Set.of("INVENTORY_RESERVED")} — true only because
     * reserving inventory happened to be the last step of the only workflow that
     * existed, and wrong for any workflow with a step after it.
     *
     * <p>Compensation steps are read structurally: starting one means the saga is
     * compensating, finishing one means it is compensated, and a failed compensation
     * leaves it compensating rather than resolved.
     */
    public Optional<SagaStatus> statusAfter(EventRole role) {
        if (role == null) return Optional.empty();

        if (isCompensationStep(role.stepId())) {
            return switch (role.role()) {
                case STARTED -> Optional.of(SagaStatus.COMPENSATING);
                case COMPLETED -> Optional.of(SagaStatus.COMPENSATED);
                case FAILED -> Optional.of(SagaStatus.COMPENSATING);
                case RETRY -> Optional.empty();
            };
        }

        if (role.role() == EventRole.Role.STARTED) return Optional.of(SagaStatus.IN_PROGRESS);
        if (role.role() == EventRole.Role.RETRY) return Optional.empty();

        // Matched on the workflow's own outcome name, so a step with three declared
        // outcomes routes three different ways. SUCCESS and FAILED are just the two
        // conventional names, not the only ones the engine understands.
        String condition = role.outcome() != null ? role.outcome() : SUCCESS_OUTCOME;

        // Every declared target has to agree on what this outcome means. A step that
        // fans out to two further steps agrees (both mean "still running"); a step whose
        // targets disagree is under-specified, and picking one of them — which is what
        // findFirst() used to do, silently, in YAML order — would invent an answer.
        Set<SagaStatus> implied = outgoingTargets(role.stepId(), condition).stream()
                .map(target -> TERMINAL_TARGETS.getOrDefault(target, SagaStatus.IN_PROGRESS))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return implied.size() == 1 ? Optional.of(implied.iterator().next()) : Optional.empty();
    }


    private List<String> outgoingTargets(String stepId, String condition) {
        if (transitions == null) return List.of();
        return transitions.stream()
                .filter(t -> stepId != null && stepId.equals(t.getFrom()))
                .filter(t -> condition.equals(t.getCondition()))
                .map(TransitionSpec::getTo)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
    }


    private Set<String> lifecycleEvents(String stepId) {
        return allSteps().stream()
                .filter(s -> stepId != null && stepId.equals(s.getId()))
                .flatMap(WorkflowSpec::allDeclaredEvents)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RetryPolicySpec {
        private Integer maxAttempts;
        private String backoff;

        public Integer getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(Integer maxAttempts) { this.maxAttempts = maxAttempts; }

        public String getBackoff() { return backoff; }
        public void setBackoff(String backoff) { this.backoff = backoff; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StepSpec {
        private String id;
        private String service;
        private String timeout;
        private String compensation;
        private RetryPolicySpec retryPolicy;
        private Map<String, String> events;

        /**
         * Named business outcomes of this step: outcome name to event type.
         *
         * <p>Open-ended, unlike {@code events}, whose keys are fixed lifecycle words.
         * Each name is matched against transition conditions, so a step can route to a
         * different successor per outcome instead of only success-or-failure.
         */
        private Map<String, String> outcomes;

        public Map<String, String> getOutcomes() { return outcomes; }
        public void setOutcomes(Map<String, String> outcomes) { this.outcomes = outcomes; }

        public RetryPolicySpec getRetryPolicy() { return retryPolicy; }
        public void setRetryPolicy(RetryPolicySpec retryPolicy) { this.retryPolicy = retryPolicy; }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }

        public String getService() { return service; }
        public void setService(String service) { this.service = service; }

        public String getTimeout() { return timeout; }
        public void setTimeout(String timeout) { this.timeout = timeout; }

        public String getCompensation() { return compensation; }
        public void setCompensation(String compensation) { this.compensation = compensation; }

        public Map<String, String> getEvents() { return events; }
        public void setEvents(Map<String, String> events) { this.events = events; }
    }

    public static class TransitionSpec {
        private String from;
        private String to;
        private String condition;

        public String getFrom() { return from; }
        public void setFrom(String from) { this.from = from; }

        public String getTo() { return to; }
        public void setTo(String to) { this.to = to; }

        public String getCondition() { return condition; }
        public void setCondition(String condition) { this.condition = condition; }
    }
}
