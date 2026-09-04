package com.example.correlation.repository;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
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

    /**
     * Which of these sagas already carry this anomaly type.
     *
     * <p>Replaces one existsBySagaIdAndAnomalyType call per saga inside the scheduler
     * loops: the same answer for a whole batch in a single round trip.
     */
    @Query("SELECT DISTINCT a.sagaId FROM Anomaly a "
            + "WHERE a.anomalyType = :type AND a.sagaId IN :sagaIds")
    List<String> findSagaIdsWithAnomalyType(@Param("type") AnomalyType type,
            @Param("sagaIds") Collection<String> sagaIds);

    List<Anomaly> findBySagaIdIn(Collection<String> sagaIds);

    /** Bulk removal used by retention; children are deleted before their saga. */
    void deleteBySagaIdIn(Collection<String> sagaIds);
}
