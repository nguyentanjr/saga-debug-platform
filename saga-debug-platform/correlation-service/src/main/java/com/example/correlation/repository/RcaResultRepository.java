package com.example.correlation.repository;

import com.example.correlation.domain.RcaResult;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface RcaResultRepository extends JpaRepository<RcaResult, Long> {
    List<RcaResult> findBySagaIdOrderByCreatedAtDesc(String sagaId);
    List<RcaResult> findByCreatedAtAfter(Instant since);
    void deleteAllBySagaId(String sagaId);
}
