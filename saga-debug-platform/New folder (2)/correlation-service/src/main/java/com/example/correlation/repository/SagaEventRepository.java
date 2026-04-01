package com.example.correlation.repository;

import com.example.correlation.domain.SagaEvent;
import org.springframework.data.jpa.repository.JpaRepository;

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

}
