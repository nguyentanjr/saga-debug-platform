package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.rca.RcaStatus;
import com.example.correlation.dto.DashboardSummaryResponse;
import com.example.correlation.dto.MetricItem;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class DashboardMetricsService {

    private static final Pattern STEP_IN_QUOTES = Pattern.compile("Step '([A-Z0-9_]+)'");
    private static final Pattern UPPERCASE_STEP = Pattern.compile("\\b([A-Z]+_[A-Z0-9_]+)\\b");

    private final SagaInstanceRepository sagaInstanceRepository;
    private final AnomalyRepository anomalyRepository;
    private final RcaResultRepository rcaResultRepository;

    public DashboardMetricsService(SagaInstanceRepository sagaInstanceRepository,
            AnomalyRepository anomalyRepository,
            RcaResultRepository rcaResultRepository) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.anomalyRepository = anomalyRepository;
        this.rcaResultRepository = rcaResultRepository;
    }

    public DashboardSummaryResponse summarize(int requestedWindowHours) {
        int windowHours = requestedWindowHours > 0 ? requestedWindowHours : 24;
        Instant since = Instant.now().minus(windowHours, ChronoUnit.HOURS);

        List<Anomaly> anomalies = anomalyRepository.findByDetectedAtAfter(since);
        List<RcaResult> rcaResults = rcaResultRepository.findByCreatedAtAfter(since);

        // Historical conclusions under a disputed specification are not current diagnoses.
        Set<String> ids = new HashSet<>();
        anomalies.forEach(a -> ids.add(a.getSagaId()));
        rcaResults.forEach(r -> ids.add(r.getSagaId()));
        Set<String> conflicted = new HashSet<>();
        sagaInstanceRepository.findAllById(ids).stream()
                .filter(SagaInstance::hasWorkflowConflict)
                .forEach(saga -> conflicted.add(saga.getSagaId()));
        anomalies = anomalies.stream().filter(a -> !conflicted.contains(a.getSagaId())).toList();
        rcaResults = rcaResults.stream().filter(r -> !conflicted.contains(r.getSagaId())).toList();

        long activeSagas = countStatusSince(SagaStatus.STARTED, since)
                + countStatusSince(SagaStatus.IN_PROGRESS, since)
                + countStatusSince(SagaStatus.COMPENSATING, since);

        return new DashboardSummaryResponse(
                windowHours,
                since,
                sagaInstanceRepository.countByStartTimeAfter(since),
                countStatusSince(SagaStatus.COMPLETED, since),
                countStatusSince(SagaStatus.FAILED, since),
                countStatusSince(SagaStatus.COMPENSATED, since),
                activeSagas,
                anomalies.size(),
                countEvidenceAvailable(rcaResults),
                countRcaStatus(rcaResults, RcaStatus.INSUFFICIENT_EVIDENCE),
                countRcaStatus(rcaResults, RcaStatus.NO_DIAGNOSIS_NEEDED),
                topAnomalyTypes(anomalies),
                topRootCauses(rcaResults),
                topAffectedSteps(anomalies));
    }

    private long countStatusSince(SagaStatus status, Instant since) {
        return sagaInstanceRepository.countUncontestedByStatusSince(status.name(), since);
    }

    private long countRcaStatus(List<RcaResult> rcaResults, RcaStatus status) {
        return rcaResults.stream()
                .filter(result -> result.getRcaStatus() == status)
                .count();
    }

    private long countEvidenceAvailable(List<RcaResult> rcaResults) {
        return rcaResults.stream()
                .filter(result -> result.getRcaStatus() == RcaStatus.EVIDENCE_AVAILABLE)
                .count();
    }

    private List<MetricItem> topAnomalyTypes(List<Anomaly> anomalies) {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (Anomaly anomaly : anomalies) {
            counts.merge(anomaly.getAnomalyType().name(), 1L, Long::sum);
        }
        return topItems(counts, 5);
    }

    private List<MetricItem> topRootCauses(List<RcaResult> results) {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (RcaResult result : results) {
            if (result.getRcaStatus() != RcaStatus.EVIDENCE_AVAILABLE) {
                continue;
            }
            List<String> roots = concreteEvidenceRootCauses(result);
            if (roots.isEmpty()) {
                if (result.getRootCause() != null && !result.getRootCause().isBlank()) {
                    counts.merge(result.getRootCause(), 1L, Long::sum);
                }
                continue;
            }
            for (String root : roots) {
                if (root != null && !root.isBlank()) {
                    counts.merge(root, 1L, Long::sum);
                }
            }
        }
        return topItems(counts, 5);
    }

    private List<String> concreteEvidenceRootCauses(RcaResult result) {
        if (result.getCandidates() != null && !result.getCandidates().isEmpty()) {
            return result.getCandidates().stream()
                    .filter(candidate -> candidate.getStatus() == CandidateStatus.STRONG_EVIDENCE
                            || candidate.getStatus() == CandidateStatus.SUPPORTING_EVIDENCE)
                    .map(RcaCandidate::getRootCause)
                    .filter(root -> root != null && !root.isBlank())
                    .distinct()
                    .toList();
        }

        List<String> roots = result.getPrimaryRootCauses();
        return roots != null ? roots : List.of();
    }

    private List<MetricItem> topAffectedSteps(List<Anomaly> anomalies) {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (Anomaly anomaly : anomalies) {
            inferStep(anomaly).ifPresent(step -> counts.merge(step, 1L, Long::sum));
        }
        return topItems(counts, 5);
    }

    private Optional<String> inferStep(Anomaly anomaly) {
        String source = anomaly.getThresholdSource();
        if (source != null && source.startsWith("workflow:")) {
            String token = source.substring("workflow:".length());
            int dot = token.indexOf('.');
            String step = dot >= 0 ? token.substring(0, dot) : token;
            if (!step.isBlank()) {
                return Optional.of(step);
            }
        }

        String text = String.join(" ",
                nullToEmpty(anomaly.getObservedValue()),
                nullToEmpty(anomaly.getDescription()));

        Matcher quoted = STEP_IN_QUOTES.matcher(text);
        if (quoted.find()) {
            return Optional.of(quoted.group(1));
        }

        Matcher uppercase = UPPERCASE_STEP.matcher(text);
        if (uppercase.find()) {
            return Optional.of(uppercase.group(1));
        }

        return Optional.empty();
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private List<MetricItem> topItems(Map<String, Long> counts, int limit) {
        return counts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue(Comparator.reverseOrder())
                        .thenComparing(Map.Entry.comparingByKey()))
                .limit(limit)
                .map(entry -> new MetricItem(entry.getKey(), entry.getValue()))
                .toList();
    }
}
