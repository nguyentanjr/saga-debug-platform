package com.example.correlation.repository;

import com.example.correlation.domain.ServiceLifecycleEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface ServiceLifecycleEventRepository extends JpaRepository<ServiceLifecycleEvent, Long> {

    /** Everything reported for these services in a window, oldest first. */
    List<ServiceLifecycleEvent> findByServiceNameInAndOccurredAtBetweenOrderByOccurredAtAsc(
            Collection<String> serviceNames, Instant from, Instant to);

    boolean existsBySourceAndExternalId(String source, String externalId);

    /** Where a reconnecting collector resumes from. */
    Optional<ServiceLifecycleEvent> findTopBySourceOrderByOccurredAtDesc(String source);

    /**
     * Retention: rows older than the cutoff, except each service's latest one. That row is
     * the service's current state; if it is an exit with no start after it, a run still
     * waiting on the service needs it as the start of its down interval however old it is.
     */
    @Modifying
    @Query("DELETE FROM ServiceLifecycleEvent e WHERE e.occurredAt < :cutoff "
            + "AND e.occurredAt < (SELECT MAX(x.occurredAt) FROM ServiceLifecycleEvent x "
            + "WHERE x.serviceName = e.serviceName)")
    int deleteExpiredKeepingLatestPerService(@Param("cutoff") Instant cutoff);
}
