package com.example.correlation.repository;

import com.example.correlation.domain.ExecutionAlias;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * Aliases, addressed by the pair that identifies them.
 *
 * <p>There is deliberately no lookup by {@code aliasKey} alone. Such a method would
 * compile, read naturally, and resolve one system's identifier against another system's
 * execution — the exact defect the composite key exists to prevent.
 */
public interface ExecutionAliasRepository
        extends JpaRepository<ExecutionAlias, ExecutionAlias.Key> {

    /** Every identifier already resolving to this execution, for a merge to re-point. */
    List<ExecutionAlias> findByExecutionId(String executionId);
}
