package com.example.correlation.repository;

import com.example.correlation.domain.RcaResult;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface RcaResultRepository extends JpaRepository<RcaResult, Long> {
    List<RcaResult> findBySagaIdOrderByCreatedAtDesc(String sagaId);
    void deleteAllBySagaId(String sagaId);
}
