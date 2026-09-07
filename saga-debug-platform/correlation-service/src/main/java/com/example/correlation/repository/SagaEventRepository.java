package com.example.correlation.repository;

import com.example.correlation.domain.SagaEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface SagaEventRepository extends JpaRepository<SagaEvent, Long> {
    /**
     * Files every observation of one execution under another identifier.
     *
     * <p>Used when two identifiers turn out to name the same execution: see
     * {@code ExecutionAliasResolver}. A bulk update rather than a load-and-save because
     * the losing chain can be any length and none of it needs to pass through memory to
     * be renamed.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE SagaEvent x SET x.sagaId = :canonical WHERE x.sagaId = :merged")
    int repointSaga(@Param("merged") String merged, @Param("canonical") String canonical);

    List<SagaEvent> findBySagaIdOrderByTimestampAsc(String sagaId);
    List<SagaEvent> findBySagaIdOrderByReceivedAtAsc(String sagaId);
    /**
     * One observation, addressed by the pair that identifies it.
     *
     * <p>Replaces a lookup by {@code eventId} alone. An event id is unique only within the
     * system that issued it, so the bare query answered "yes, seen before" for a second
     * system's event that merely reused a number — and the caller, a duplicate check,
     * discarded it. The unique constraint behind this is on the pair for the same reason.
     */
    Optional<SagaEvent> findBySourceNameAndEventId(String sourceName, String eventId);
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

