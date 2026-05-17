package com.example.correlation.service.rca;

import com.example.correlation.domain.rca.CandidateAssessment;

import java.util.function.BiPredicate;

public class ExclusionRule {

    private final String ruleId;
    private final BiPredicate<CandidateAssessment, SelectionContext> predicate;
    private final String reason;

    public ExclusionRule(String ruleId,
            BiPredicate<CandidateAssessment, SelectionContext> predicate,
            String reason) {
        this.ruleId = ruleId;
        this.predicate = predicate;
        this.reason = reason;
    }

    public boolean appliesTo(CandidateAssessment candidate, SelectionContext context) {
        return ruleId.equals(candidate.getRuleId()) && predicate.test(candidate, context);
    }

    public String getReason() {
        return reason;
    }
}
