package com.example.correlation.repository;

import com.example.correlation.domain.SagaEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface SagaEventRepository extends JpaRepository<SagaEvent, Long> {
    List<SagaEvent> findBySagaIdOrderByTimestampAsc(String sagaId);
    List<SagaEvent> findBySagaIdOrderByReceivedAtAsc(String sagaId);
    Optional<SagaEvent> findByEventId(String eventId);
    long countBySagaId(String sagaId);
    int deleteByTimestampBefore(Instant cutoff);
    Optional<SagaEvent> findTopBySagaIdOrderByTimestampDesc(String sagaId);

    
    @Query("SELECT e.sagaId, COUNT(e) FROM SagaEvent e WHERE e.sagaId IN :sagaIds GROUP BY e.sagaId")
    List<Object[]> countBySagaIdIn(@Param("sagaIds") List<String> sagaIds);

    /**
     * Events for a batch of sagas in one round trip.
     *
     * <p>The scheduled detectors used to issue one findBySagaIdOrderByTimestampAsc per
     * saga per pass. Batching keeps the query count constant in the size of the batch
     * instead of linear.
     */
    List<SagaEvent> findBySagaIdInOrderBySagaIdAscTimestampAsc(Collection<String> sagaIds);

    /** Bulk removal used by retention; children are deleted before their saga. */
    void deleteBySagaIdIn(Collection<String> sagaIds);
}

