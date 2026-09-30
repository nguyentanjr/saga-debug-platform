package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpecTestFixture;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * F20 evidence: the two step-timeout measurements over every recorded run of the official
 * set (oracle/evidence, 27 runs), before the default moved from SHADOW to PROJECTION.
 *
 * <p>This is what SHADOW mode existed to collect. None of these runs observes a step's
 * declared start event, so the projection falls back to the predecessor's completion --
 * the same interval the adjacency walk measures -- and the two must agree. A disagreement
 * here would mean the switch changes the headline, which is why it is asserted rather than
 * printed.
 */
class StepTimeoutModeOnRecordedRunsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final WorkflowSpec spec = WorkflowSpecTestFixture.orderSaga();

    @Test
    void bothMeasurementsAgreeOnEveryRecordedRun() throws Exception {
        Path evidence = CorrectnessAblationTest.repoRoot().resolve("oracle/evidence");
        int runs = 0, fired = 0;
        List<String> divergent = new ArrayList<>();
        try (var files = Files.list(evidence)) {
            for (Path file : files.filter(p -> p.toString().endsWith(".json")).sorted().toList()) {
                for (JsonNode run : MAPPER.readTree(file.toFile()).path("raw_runs")) {
                    List<SagaEvent> events = events(run);
                    String legacy = measure(DetectionProperties.StepTimeoutMode.LEGACY, events);
                    String projected = measure(DetectionProperties.StepTimeoutMode.PROJECTION, events);
                    String id = run.path("scenario_id").asText() + "#" + run.path("run_index").asInt();
                    System.out.printf("[F20 recorded] %-34s legacy=%s projection=%s%n", id, legacy, projected);
                    runs++;
                    if (legacy != null) fired++;
                    if (!Objects.equals(legacy, projected)) divergent.add(id);
                }
            }
        }
        System.out.printf("[F20 recorded] runs=%d legacy_fired=%d divergent=%s%n", runs, fired, divergent);

        assertThat(runs).isEqualTo(27);
        assertThat(fired).as("the set does contain step timeouts to compare").isPositive();
        assertThat(divergent).as("runs whose STEP_TIMEOUT changes with the switch").isEmpty();
    }

    /** Step and whole-millisecond duration, e.g. PROCESS_PAYMENT=41210ms; null when nothing fires. */
    private String measure(DetectionProperties.StepTimeoutMode mode, List<SagaEvent> events) {
        List<Anomaly> saved = new ArrayList<>();
        AnomalyRepository repository = mock(AnomalyRepository.class);
        when(repository.save(any(Anomaly.class))).thenAnswer(c -> {
            saved.add(c.getArgument(0));
            return c.getArgument(0);
        });
        DetectionProperties properties = new DetectionProperties();
        properties.setStepTimeoutMode(mode);
        WorkflowRegistry registry = mock(WorkflowRegistry.class);
        new AnomalyDetectionService(mock(SagaInstanceRepository.class), mock(SagaEventRepository.class),
                repository, registry, MAPPER, properties, new ExecutionProjectionService())
                .evaluateStepTimeouts("run", events, spec);
        if (saved.isEmpty()) return null;
        String observed = saved.get(0).getObservedValue();
        return observed.substring(0, observed.indexOf(' ')) + "="
                + observed.replaceAll(".* took (\\d+)ms.*", "$1") + "ms";
    }

    private static List<SagaEvent> events(JsonNode run) {
        List<SagaEvent> events = new ArrayList<>();
        for (JsonNode e : run.path("raw_events")) {
            SagaEvent event = new SagaEvent();
            event.setEventId(e.path("eventId").asText());
            event.setSagaId("run");
            event.setEventType(e.path("eventType").asText());
            event.setServiceName(e.path("serviceName").asText());
            event.setTimestamp(Instant.parse(e.path("timestamp").asText()));
            event.setReceivedAt(Instant.parse(e.path("receivedAt").asText()));
            event.setPayload(e.path("payload").asText());
            events.add(event);
        }
        // As the production caller supplies them: event-time order.
        events.sort(Comparator.comparing(SagaEvent::getTimestamp));
        return events;
    }
}
