package com.example.correlation.repository;

import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface SagaInstanceRepository extends JpaRepository<SagaInstance, String> {
    List<SagaInstance> findByStatus(SagaStatus status);
    List<SagaInstance> findByStatusAndLastUpdatedBefore(SagaStatus status, Instant before);
    Page<SagaInstance> findByStatus(SagaStatus status, Pageable pageable);
    Page<SagaInstance> findByStartTimeBetween(Instant start, Instant end, Pageable pageable);
    Page<SagaInstance> findByStatusAndStartTimeBetween(SagaStatus status, Instant start, Instant end, Pageable pageable);
}
