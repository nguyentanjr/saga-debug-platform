package com.example.correlation.domain.rca;

import java.util.ArrayList;
import java.util.List;

public class CandidateAssessment {

    private String ruleId;
    private String rootCause;
    private String severity;
    private Directness directness;
    private Specificity specificity;
    private CandidateStatus status;
    private List<Evidence> evidence = new ArrayList<>();
    private List<String> suggestions = new ArrayList<>();
    private String selectionReason;
    private String contributionReason;
    private String ruledOutReason;
    private String missingForStronger;

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

    public Specificity getSpecificity() { return specificity; }
    public void setSpecificity(Specificity specificity) { this.specificity = specificity; }

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

    public String getSelectionReason() { return selectionReason; }
    public void setSelectionReason(String selectionReason) { this.selectionReason = selectionReason; }

    public String getContributionReason() { return contributionReason; }
    public void setContributionReason(String contributionReason) { this.contributionReason = contributionReason; }

    public String getRuledOutReason() { return ruledOutReason; }
    public void setRuledOutReason(String ruledOutReason) { this.ruledOutReason = ruledOutReason; }

    public String getMissingForStronger() { return missingForStronger; }
    public void setMissingForStronger(String missingForStronger) { this.missingForStronger = missingForStronger; }

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

        public Builder specificity(Specificity specificity) {
            candidate.setSpecificity(specificity);
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

        public Builder missingForStronger(String missingForStronger) {
            candidate.setMissingForStronger(missingForStronger);
            return this;
        }

        public CandidateAssessment build() {
            return candidate;
        }
    }
}
