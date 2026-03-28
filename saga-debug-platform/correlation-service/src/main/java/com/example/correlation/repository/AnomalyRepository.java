package com.example.correlation.repository;

import com.example.correlation.domain.Anomaly;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface AnomalyRepository extends JpaRepository<Anomaly, Long> {
    List<Anomaly> findBySagaId(String sagaId);
    int deleteByDetectedAtBefore(Instant cutoff);
}
