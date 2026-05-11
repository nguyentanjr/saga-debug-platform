package com.example.correlation.repository;

import com.example.correlation.domain.SagaEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface SagaEventRepository extends JpaRepository<SagaEvent, Long> {
    List<SagaEvent> findBySagaIdOrderByTimestampAsc(String sagaId);
    List<SagaEvent> findBySagaIdOrderByReceivedAtAsc(String sagaId);
    Optional<SagaEvent> findByEventId(String eventId);
    long countBySagaId(String sagaId);
    int deleteByTimestampBefore(Instant cutoff);
    Optional<SagaEvent> findTopBySagaIdOrderByTimestampDesc(String sagaId);

    // Bulk count — eliminates N+1 in SagaController.listSagas()
    @Query("SELECT e.sagaId, COUNT(e) FROM SagaEvent e WHERE e.sagaId IN :sagaIds GROUP BY e.sagaId")
    List<Object[]> countBySagaIdIn(@Param("sagaIds") List<String> sagaIds);
}

