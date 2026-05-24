package com.example.correlation.service.rca;

import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.rca.RcaStatus;

import java.util.ArrayList;
import java.util.List;

public class SelectionResult {

    private RcaStatus rcaStatus;
    private List<String> primaryRootCauses = new ArrayList<>();
    private List<RcaCandidate> candidates = new ArrayList<>();
    private List<String> missingEvidence = new ArrayList<>();

    public RcaStatus getRcaStatus() { return rcaStatus; }
    public void setRcaStatus(RcaStatus rcaStatus) { this.rcaStatus = rcaStatus; }

    public List<String> getPrimaryRootCauses() { return primaryRootCauses; }
    public void setPrimaryRootCauses(List<String> primaryRootCauses) {
        this.primaryRootCauses = primaryRootCauses != null ? new ArrayList<>(primaryRootCauses) : new ArrayList<>();
    }

    public List<RcaCandidate> getCandidates() { return candidates; }
    public void setCandidates(List<RcaCandidate> candidates) {
        this.candidates = candidates != null ? new ArrayList<>(candidates) : new ArrayList<>();
    }

    public List<String> getMissingEvidence() { return missingEvidence; }
    public void setMissingEvidence(List<String> missingEvidence) {
        this.missingEvidence = missingEvidence != null ? new ArrayList<>(missingEvidence) : new ArrayList<>();
    }
}
