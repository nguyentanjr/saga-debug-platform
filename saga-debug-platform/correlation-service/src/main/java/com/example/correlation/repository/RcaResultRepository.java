package com.example.correlation.repository;

import com.example.correlation.domain.RcaResult;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

public interface RcaResultRepository extends JpaRepository<RcaResult, Long> {
    /**
     * Files every observation of one execution under another identifier.
     *
     * <p>Used when two identifiers turn out to name the same execution: see
     * {@code ExecutionAliasResolver}. A bulk update rather than a load-and-save because
     * the losing chain can be any length and none of it needs to pass through memory to
     * be renamed.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE RcaResult x SET x.sagaId = :canonical WHERE x.sagaId = :merged")
    int repointSaga(@Param("merged") String merged, @Param("canonical") String canonical);

    List<RcaResult> findBySagaIdOrderByCreatedAtDesc(String sagaId);
    List<RcaResult> findBySagaIdInOrderByCreatedAtDesc(Collection<String> sagaIds);
    List<RcaResult> findByCreatedAtAfter(Instant since);
    void deleteAllBySagaId(String sagaId);

    /** Bulk removal used by retention; children are deleted before their saga. */
    void deleteBySagaIdIn(Collection<String> sagaIds);
}
