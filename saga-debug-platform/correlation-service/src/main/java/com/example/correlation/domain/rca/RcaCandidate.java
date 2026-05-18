package com.example.correlation.domain.rca;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RcaCandidate {

    private String ruleId;
    private String rootCause;
    private String severity;
    private CandidateStatus status;
    private Directness directness;
    private List<Evidence> evidence = new ArrayList<>();
    private List<String> suggestions = new ArrayList<>();
    private String statusReason;

    public static RcaCandidate from(CandidateAssessment assessment) {
        RcaCandidate candidate = new RcaCandidate();
        candidate.setRuleId(assessment.getRuleId());
        candidate.setRootCause(assessment.getRootCause());
        candidate.setSeverity(assessment.getSeverity());
        candidate.setStatus(assessment.getStatus());
        candidate.setDirectness(assessment.getDirectness());
        candidate.setEvidence(assessment.getEvidence());
        candidate.setSuggestions(assessment.getSuggestions());
        candidate.setStatusReason(assessment.getStatusReason());
        return candidate;
    }

    public String getRuleId() { return ruleId; }
    public void setRuleId(String ruleId) { this.ruleId = ruleId; }

    public String getRootCause() { return rootCause; }
    public void setRootCause(String rootCause) { this.rootCause = rootCause; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public CandidateStatus getStatus() { return status; }
    public void setStatus(CandidateStatus status) { this.status = status; }

    public Directness getDirectness() { return directness; }
    public void setDirectness(Directness directness) { this.directness = directness; }

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
}
