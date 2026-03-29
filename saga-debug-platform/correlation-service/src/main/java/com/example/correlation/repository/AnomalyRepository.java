package com.example.correlation.repository;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface AnomalyRepository extends JpaRepository<Anomaly, Long> {
    List<Anomaly> findBySagaId(String sagaId);
    boolean existsBySagaIdAndAnomalyType(String sagaId, AnomalyType anomalyType);
    int deleteByDetectedAtBefore(Instant cutoff);
}
