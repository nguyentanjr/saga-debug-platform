package com.example.correlation.workflow;

import com.example.correlation.config.DeclarationFiles;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
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
 * <p>Specs are discovered from {@code workflows/*.yaml} on the classpath and from the
 * directory named by {@code correlation.declarations.dir}, so adding a workflow is adding
 * a file — and describing a newly observed system does not need a rebuild.
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

    /** Where a deployment keeps the declarations it did not build into the jar. */
    @Value("${correlation.declarations.dir:}")
    private String declarationsDirectory;

    private final Map<String, WorkflowSpec> byName = new LinkedHashMap<>();

    /** Event type to the specs declaring it; more than one entry means ambiguous. */
    private final Map<String, List<WorkflowSpec>> byStartEvent = new LinkedHashMap<>();

    @PostConstruct
    public void load() {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        try {
            List<Resource> resources = DeclarationFiles.find("workflows", declarationsDirectory);

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

    /** Package-private so a test can build a registry without putting YAML on the classpath,
     * where it would also reach every other test. */
    void register(WorkflowSpec spec, String filename) {
        if (spec == null || spec.getName() == null) {
            log.error("Workflow spec {} has no name; ignored", filename);
            return;
        }

        // Checked before the spec is indexed, not when a saga first needs it. Every
        // detector reads its expectations from here, so a spec that cannot be
        // interpreted does not degrade the diagnosis — it produces confident nonsense,
        // with nothing in the logs pointing back at the file that caused it.
        WorkflowSpecValidator.Report report = WorkflowSpecValidator.validate(spec);
        report.warnings().forEach(warning ->
                log.warn("Workflow '{}' ({}): {}", spec.getName(), filename, warning));
        if (!report.isUsable()) {
            report.errors().forEach(error ->
                    log.error("Workflow '{}' ({}) rejected: {}", spec.getName(), filename, error));
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

        log.info("Workflow '{}' v{} — {} step(s), start event(s) {}{}",
                spec.getName(), spec.getVersion(),
                spec.getSteps() != null ? spec.getSteps().size() : 0,
                spec.startEventTypes(),
                report.isClean() ? "" : " (" + report.warnings().size() + " warning(s))");
    }

    /** The spec for an execution whose workflow is already known. */
    public Optional<WorkflowSpec> byType(String workflowType) {
        return Optional.ofNullable(workflowType).map(byName::get);
    }

    /**
     * The spec whose start event this is, across every system.
     *
     * @return empty when no workflow declares the event as a start, and also when more
     *         than one does — an ambiguous match is not a match
     */
    public Optional<WorkflowSpec> forStartEvent(String eventType) {
        return unique(candidatesFor(eventType), eventType, null);
    }

    /**
     * The spec whose start event this is, among those describing one observed system.
     *
     * <p>The narrowing that makes a multi-system deployment work. Two systems both calling
     * their first event {@code ORDER_CREATED} is not a conflict once each specification
     * says which system it was written about; it only looked like one because resolution
     * had no way to ask.
     */
    public Optional<WorkflowSpec> forStartEvent(String eventType, String sourceName) {
        List<WorkflowSpec> scoped = candidatesFor(eventType).stream()
                .filter(spec -> spec.describesSource(sourceName))
                .toList();
        return unique(scoped, eventType, sourceName);
    }

    private List<WorkflowSpec> candidatesFor(String eventType) {
        List<WorkflowSpec> candidates = byStartEvent.get(eventType);
        return candidates == null ? List.of() : candidates;
    }

    /**
     * One candidate, or none.
     *
     * <p>Refusing an ambiguous match is the whole point: a wrong specification does not
     * fail visibly, it measures the run against another system's steps and timeouts and
     * reports the differences as defects.
     */
    private Optional<WorkflowSpec> unique(List<WorkflowSpec> candidates,
            String eventType, String sourceName) {
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        if (candidates.size() > 1) {
            log.warn("Event type '{}'{} starts more than one workflow ({}); cannot pick one. "
                            + "Declare 'sources:' on the specifications, or 'workflow:' on the "
                            + "source mapping.",
                    eventType,
                    sourceName != null ? " from source '" + sourceName + "'" : "",
                    candidates.stream().map(WorkflowSpec::getName).toList());
            return Optional.empty();
        }
        return Optional.of(candidates.get(0));
    }

    /**
     * The specification to judge an observation against.
     *
     * <h2>The ladder</h2>
     * In descending order of how directly the answer was stated, and stopping at the first
     * rung that gives one. Nothing here guesses; the last rung is a refusal.
     *
     * <ol>
     *   <li><b>Explicit</b> — the execution already resolved to a workflow, so that is what
     *       it is. Every observation after the first takes this rung.</li>
     *   <li><b>Source-scoped</b> — a specification that names this observed system and
     *       declares this event as a start. The specification is the authority on what it
     *       describes.</li>
     *   <li><b>Mapping-declared</b> — the source mapping names the workflow its events
     *       belong to. Useful when a source has exactly one workflow and its owner would
     *       rather say so in the mapping than edit the specification.</li>
     *   <li><b>Event-based</b> — one workflow, anywhere, declares this event as a start.
     *       What a single-system deployment has always used, unchanged.</li>
     *   <li><b>Unresolved</b> — more than one candidate and nothing to separate them. The
     *       spec-driven detectors then abstain rather than measuring against a workflow
     *       that may not be this run's.</li>
     * </ol>
     *
     * <p>The fallback down the ladder matters for an execution whose first observation was
     * lost or arrived out of order: the saga row may carry no resolved workflow, and the
     * event still needs to be interpreted.
     *
     * @param workflowType    the workflow this execution already resolved to, or null
     * @param declaredWorkflow the workflow the source's mapping names, or null
     * @param sourceName      the observed system this observation came from
     */
    public Optional<WorkflowSpec> resolve(String workflowType, String declaredWorkflow,
            String sourceName, String eventType) {
        Optional<WorkflowSpec> explicit = byType(workflowType);
        if (explicit.isPresent()) {
            return explicit;
        }
        return resolveFromEvidence(declaredWorkflow, sourceName, eventType);
    }

    /**
     * What this observation alone says the workflow is, ignoring any earlier resolution.
     *
     * <p>The ladder below the first rung. Separated because an execution that resolved to
     * nothing is not the same as one that resolved: for the first, a later observation is
     * the only chance to learn what workflow it belongs to, and for the second, this is how
     * a disagreement between the two is noticed rather than silently taken or ignored.
     */
    public Optional<WorkflowSpec> resolveFromEvidence(String declaredWorkflow,
            String sourceName, String eventType) {
        Optional<WorkflowSpec> sourceScoped = forStartEvent(eventType, sourceName)
                .filter(WorkflowSpec::isSourceScoped);
        Optional<WorkflowSpec> mappingDeclared = byType(declaredWorkflow);

        if (sourceScoped.isPresent()) {
            // A disagreement between the two declarations is a configuration mistake, not
            // something to resolve quietly: one of the two files is wrong about a system.
            if (mappingDeclared.isPresent()
                    && !mappingDeclared.get().getName().equals(sourceScoped.get().getName())) {
                log.error("Source '{}' is mapped to workflow '{}', but specification '{}' "
                                + "declares that source and starts on '{}'. Using '{}'; fix "
                                + "whichever declaration is wrong.",
                        sourceName, mappingDeclared.get().getName(),
                        sourceScoped.get().getName(), eventType, sourceScoped.get().getName());
            }
            return sourceScoped;
        }
        if (mappingDeclared.isPresent()) {
            return mappingDeclared;
        }
        return forStartEvent(eventType, sourceName).or(() -> forStartEvent(eventType));
    }

    /** @deprecated resolution without a source cannot separate two systems' workflows. */
    @Deprecated
    public Optional<WorkflowSpec> resolve(String workflowType, String eventType) {
        return resolve(workflowType, null, null, eventType);
    }

    public List<String> workflowNames() {
        return List.copyOf(byName.keySet());
    }
}
