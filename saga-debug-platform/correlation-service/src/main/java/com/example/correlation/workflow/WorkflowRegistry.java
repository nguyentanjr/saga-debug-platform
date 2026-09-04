package com.example.correlation.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Every workflow specification the platform knows about, indexed for lookup.
 *
 * <p>Replaces {@code WorkflowLoader}, which held a single spec behind a method called
 * {@code getOrderSagaWorkflow()}. Every detector and every rule called that method, so
 * a second workflow could not be added without editing Java: the engine had exactly one
 * expected behaviour and it was the order saga's.
 *
 * <p>Specs are discovered from {@code classpath*:workflows/*.yaml}, so adding a
 * workflow is adding a file.
 *
 * <h2>How a saga is matched to a spec</h2>
 * By {@code workflowType} once the saga exists, and by start event when it does not.
 * Two workflows that declare the same event type make that second lookup ambiguous;
 * rather than silently picking one, the registry refuses to answer and the caller
 * treats the execution as unrecognised. The durable fix is a workflow identifier in the
 * event envelope, which the current {@code BaseEvent} does not carry.
 */
@Component
public class WorkflowRegistry {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRegistry.class);

    private static final String LOCATION_PATTERN = "classpath*:workflows/*.yaml";

    private final Map<String, WorkflowSpec> byName = new LinkedHashMap<>();

    /** Event type to the specs declaring it; more than one entry means ambiguous. */
    private final Map<String, List<WorkflowSpec>> byStartEvent = new LinkedHashMap<>();

    @PostConstruct
    public void load() {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        try {
            Resource[] resources = new PathMatchingResourcePatternResolver()
                    .getResources(LOCATION_PATTERN);

            for (Resource resource : resources) {
                try (InputStream is = resource.getInputStream()) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> root = yamlMapper.readValue(is, Map.class);
                    WorkflowSpec spec = yamlMapper.convertValue(root.get("workflow"), WorkflowSpec.class);
                    register(spec, resource.getFilename());
                } catch (Exception e) {
                    log.error("Failed to load workflow spec {}: {}", resource.getFilename(), e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error("Failed to scan for workflow specs: {}", e.getMessage(), e);
        }

        if (byName.isEmpty()) {
            log.error("No workflow specifications loaded. Every execution will be treated "
                    + "as unrecognised and no spec-driven detection will run.");
        } else {
            log.info("Loaded {} workflow spec(s): {}", byName.size(), byName.keySet());
        }
    }

    private void register(WorkflowSpec spec, String filename) {
        if (spec == null || spec.getName() == null) {
            log.error("Workflow spec {} has no name; ignored", filename);
            return;
        }

        WorkflowSpec previous = byName.putIfAbsent(spec.getName(), spec);
        if (previous != null) {
            log.error("Duplicate workflow name '{}' in {}; keeping the one already loaded",
                    spec.getName(), filename);
            return;
        }

        for (String eventType : spec.startEventTypes()) {
            byStartEvent.computeIfAbsent(eventType, k -> new ArrayList<>()).add(spec);
        }

        log.info("Workflow '{}' v{} — {} step(s), start event(s) {}",
                spec.getName(), spec.getVersion(),
                spec.getSteps() != null ? spec.getSteps().size() : 0,
                spec.startEventTypes());
    }

    /** The spec for an execution whose workflow is already known. */
    public Optional<WorkflowSpec> byType(String workflowType) {
        return Optional.ofNullable(workflowType).map(byName::get);
    }

    /**
     * The spec whose start event this is.
     *
     * @return empty when no workflow declares the event as a start, and also when more
     *         than one does — an ambiguous match is not a match
     */
    public Optional<WorkflowSpec> forStartEvent(String eventType) {
        List<WorkflowSpec> candidates = byStartEvent.get(eventType);
        if (candidates == null || candidates.isEmpty()) {
            return Optional.empty();
        }
        if (candidates.size() > 1) {
            log.warn("Event type '{}' starts more than one workflow ({}); cannot pick one",
                    eventType, candidates.stream().map(WorkflowSpec::getName).toList());
            return Optional.empty();
        }
        return Optional.of(candidates.get(0));
    }

    /**
     * The spec for a saga, falling back to the workflow that this event would start.
     *
     * <p>The fallback matters for an execution whose first observation was lost or
     * arrived out of order: the saga row may carry a workflow type that was never
     * resolved, and the event still needs to be interpreted.
     */
    public Optional<WorkflowSpec> resolve(String workflowType, String eventType) {
        return byType(workflowType).or(() -> forStartEvent(eventType));
    }

    public List<String> workflowNames() {
        return List.copyOf(byName.keySet());
    }
}
