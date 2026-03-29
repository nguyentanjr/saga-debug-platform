package com.example.correlation.workflow;

import java.util.List;
import java.util.Map;

/**
 * Domain model for workflow specification loaded from YAML.
 */
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

    /**
     * Get the expected step name for a given event type.
     */
    public String getExpectedStepForEvent(String eventType) {
        return validEventTypes != null ? validEventTypes.get(eventType) : null;
    }

    /**
     * Check if a transition from -> to is valid.
     */
    public boolean isValidTransition(String fromStep, String toStep, String condition) {
        if (transitions == null) return true; // no validation if not defined
        return transitions.stream().anyMatch(t ->
                t.getFrom().equals(fromStep) &&
                t.getTo().equals(toStep) &&
                (condition == null || condition.equals(t.getCondition()))
        );
    }

    /**
     * Get timeout for a given step in seconds.
     */
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
        return 30; // default 30s
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

    // --- Nested classes ---

    public static class StepSpec {
        private String id;
        private String service;
        private String timeout;
        private String compensation;
        private Map<String, String> events;

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
