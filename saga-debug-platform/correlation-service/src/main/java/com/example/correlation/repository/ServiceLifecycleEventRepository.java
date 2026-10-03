package com.example.correlation.repository;

import com.example.correlation.domain.ServiceLifecycleEvent;
import org.springframework.data.jpa.repository.JpaRepository;

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
}
