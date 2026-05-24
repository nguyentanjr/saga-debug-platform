package com.example.correlation.repository;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface AnomalyRepository extends JpaRepository<Anomaly, Long> {
    List<Anomaly> findBySagaId(String sagaId);
    List<Anomaly> findByDetectedAtAfter(Instant since);
    boolean existsBySagaIdAndAnomalyType(String sagaId, AnomalyType anomalyType);
    int deleteByDetectedAtBefore(Instant cutoff);

    @Query("SELECT DISTINCT a.sagaId FROM Anomaly a WHERE a.detectedAt >= :since")
    List<String> findSagaIdsWithAnomaliesSince(@Param("since") Instant since);

    @Query("SELECT MAX(a.detectedAt) FROM Anomaly a WHERE a.sagaId = :sagaId")
    Instant findLatestDetectedAtBySagaId(@Param("sagaId") String sagaId);
}
