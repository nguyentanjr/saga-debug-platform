package com.example.correlation.service;

import com.example.correlation.domain.SagaStatus;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Deletes settled sagas and their telemetry once they age past the retention window.
 *
 * <p>Nothing removed anything before this existed: {@code deleteByTimestampBefore} and
 * {@code deleteByDetectedAtBefore} were declared on the repositories but never called,
 * so every table grew for the lifetime of the deployment. That interacts badly with the
 * scheduled detectors, whose cost is driven by how much history they have to sift.
 *
 * <p>Disabled unless {@code retention.enabled} is set, because this destroys telemetry
 * and the evaluation suite in this repository reads historical sagas.
 *
 * <p>Only sagas in a terminal state are eligible. An old saga still in STARTED,
 * IN_PROGRESS or COMPENSATING is the evidence for a real stuck-saga finding, and
 * deleting it would erase the very thing the platform is meant to report.
 */
@Service
@ConditionalOnProperty(prefix = "retention", name = "enabled", havingValue = "true")
public class RetentionService {

    private static final Logger log = LoggerFactory.getLogger(RetentionService.class);

    private static final List<SagaStatus> TERMINAL_STATUSES =
            List.of(SagaStatus.COMPLETED, SagaStatus.FAILED, SagaStatus.COMPENSATED);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyRepository anomalyRepository;
    private final RcaResultRepository rcaResultRepository;
    private final RetentionProperties properties;

    public RetentionService(SagaInstanceRepository sagaInstanceRepository,
            SagaEventRepository sagaEventRepository,
            AnomalyRepository anomalyRepository,
            RcaResultRepository rcaResultRepository,
            RetentionProperties properties) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyRepository = anomalyRepository;
        this.rcaResultRepository = rcaResultRepository;
        this.properties = properties;
    }

    // Transactional here rather than on purgeOlderThan: a self-invoked method does not
    // go through the proxy, so annotating the inner one would leave the four deletes
    // running outside any transaction and able to half-complete.
    @Transactional
    public void purgeExpiredSagas() {
        Instant cutoff = Instant.now().minus(properties.getMaxAge());
        int purged = purgeOlderThan(cutoff);
        if (purged > 0) {
            log.info("Retention purge removed {} saga(s) settled before {}", purged, cutoff);
        }
    }

    /**
     * Removes one batch of expired sagas, children first so no row is ever orphaned.
     *
     * @return how many sagas were removed; fewer than the batch size means the backlog
     *         is drained, so the caller can tell a finished purge from a capped one
     */
    int purgeOlderThan(Instant cutoff) {
        List<String> expired = sagaInstanceRepository
                .findByStatusInAndLastUpdatedBefore(TERMINAL_STATUSES, cutoff,
                        PageRequest.of(0, properties.getBatchSize(),
                                Sort.by(Sort.Direction.ASC, "lastUpdated")))
                .stream()
                .map(saga -> saga.getSagaId())
                .toList();

        if (expired.isEmpty()) {
            return 0;
        }

        rcaResultRepository.deleteBySagaIdIn(expired);
        anomalyRepository.deleteBySagaIdIn(expired);
        sagaEventRepository.deleteBySagaIdIn(expired);
        sagaInstanceRepository.deleteAllById(expired);

        return expired.size();
    }
}
