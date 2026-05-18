package com.example.correlation.domain.rca;

import java.util.ArrayList;
import java.util.List;

public class CandidateAssessment {

    private String ruleId;
    private String rootCause;
    private String severity;
    private Directness directness;
    private CandidateStatus status;
    private List<Evidence> evidence = new ArrayList<>();
    private List<String> suggestions = new ArrayList<>();
    private String statusReason;

    public static Builder builder() {
        return new Builder();
    }

    public String getRuleId() { return ruleId; }
    public void setRuleId(String ruleId) { this.ruleId = ruleId; }

    public String getRootCause() { return rootCause; }
    public void setRootCause(String rootCause) { this.rootCause = rootCause; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public Directness getDirectness() { return directness; }
    public void setDirectness(Directness directness) { this.directness = directness; }

    public CandidateStatus getStatus() { return status; }
    public void setStatus(CandidateStatus status) { this.status = status; }

    public List<Evidence> getEvidence() { return evidence; }
    public void setEvidence(List<Evidence> evidence) {
        this.evidence = evidence != null ? new ArrayList<>(evidence) : new ArrayList<>();
    }

    public List<String> getSuggestions() { return suggestions; }
    public void setSuggestions(List<String> suggestions) {
        this.suggestions = suggestions != null ? new ArrayList<>(suggestions) : new ArrayList<>();
    }

    public String getStatusReason() { return statusReason; }
    public void setStatusReason(String statusReason) { this.statusReason = statusReason; }

    public static class Builder {
        private final CandidateAssessment candidate = new CandidateAssessment();

        public Builder ruleId(String ruleId) {
            candidate.setRuleId(ruleId);
            return this;
        }

        public Builder rootCause(String rootCause) {
            candidate.setRootCause(rootCause);
            return this;
        }

        public Builder severity(String severity) {
            candidate.setSeverity(severity);
            return this;
        }

        public Builder directness(Directness directness) {
            candidate.setDirectness(directness);
            return this;
        }

        public Builder status(CandidateStatus status) {
            candidate.setStatus(status);
            return this;
        }

        public Builder evidence(Evidence evidence) {
            if (evidence != null) {
                candidate.evidence.add(evidence);
            }
            return this;
        }

        public Builder evidence(List<Evidence> evidence) {
            candidate.setEvidence(evidence);
            return this;
        }

        public Builder suggestion(String suggestion) {
            if (suggestion != null) {
                candidate.suggestions.add(suggestion);
            }
            return this;
        }

        public Builder suggestions(List<String> suggestions) {
            candidate.setSuggestions(suggestions);
            return this;
        }

        public Builder statusReason(String statusReason) {
            candidate.setStatusReason(statusReason);
            return this;
        }

        public CandidateAssessment build() {
            return candidate;
        }
    }
}
