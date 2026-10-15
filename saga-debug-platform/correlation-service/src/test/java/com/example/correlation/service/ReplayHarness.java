package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.workflow.WorkflowRegistry;

import java.time.Instant;
import java.util.List;

/**
 * Public entry to {@link ReplayPipeline} for tests outside this package: delivers captured
 * observations of one run, in the order given, and reports what the platform concluded.
 */
public final class ReplayHarness {

    public record CapturedEvent(String eventId, String eventType, Instant timestamp, String serviceName) {}

    public record Result(String status, List<AnomalyType> standing) {}

    private ReplayHarness() {}

    public static Result replay(WorkflowRegistry workflows, String source, String runId, String workflow,
                                List<CapturedEvent> deliveryOrder) {
        ReplayPipeline pipeline = new ReplayPipeline(workflows);
        ExecutionKey key = ExecutionKey.of(source, runId);
        // Arrival follows delivery order; business time is what the source stamped.
        Instant arrival = Instant.parse("2026-10-11T00:00:00Z");
        for (CapturedEvent e : deliveryOrder) {
            arrival = arrival.plusMillis(10);
            pipeline.deliver(List.of(new IngestedEvent(
                    new IngestedEvent.Identity(source, key, e.eventId(), null, CorrelationBasis.DECLARED_KEY),
                    workflow, e.eventType(), null, e.serviceName(), e.timestamp(), "{}", arrival,
                    IngestedEvent.Causality.NONE, null)));
        }
        pipeline.outcome(key.canonical());
        return new Result(pipeline.instances.get(key.canonical()).getStatus().name(),
                pipeline.anomalies.stream()
                        .filter(a -> a.getSagaId().equals(key.canonical()) && a.getRetractedAt() == null)
                        .map(Anomaly::getAnomalyType).toList());
    }
}
