package com.example.correlation.ingestion;

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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The declared ways of reading external systems' events.
 *
 * <p>Loaded from {@code ingestion/*.yaml} on the classpath and from the directory named
 * by {@code correlation.declarations.dir}, so observing a new system is adding a file —
 * the same shape as adding a workflow, and without a rebuild. A topic that matches no
 * mapping falls back to the identity mapping, which reads this platform's own field
 * names, so producers already emitting the envelope keep working with nothing declared.
 */
@Component
public class SourceMappingRegistry {

    private static final Logger log = LoggerFactory.getLogger(SourceMappingRegistry.class);

    /** Where a deployment keeps the declarations it did not build into the jar. */
    @Value("${correlation.declarations.dir:}")
    private String declarationsDirectory;

    private final List<SourceMapping> mappings = new ArrayList<>();
    private final SourceMapping identity = SourceMapping.identity();

    @PostConstruct
    public void load() {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        try {
            List<Resource> resources = DeclarationFiles.find("ingestion", declarationsDirectory);

            for (Resource resource : resources) {
                try (InputStream is = resource.getInputStream()) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> root = yamlMapper.readValue(is, Map.class);
                    SourceMapping mapping =
                            yamlMapper.convertValue(root.get("source"), SourceMapping.class);
                    register(mapping, resource.getFilename());
                } catch (Exception e) {
                    log.error("Failed to load source mapping {}: {}",
                            resource.getFilename(), e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error("Failed to scan for source mappings: {}", e.getMessage(), e);
        }

        log.info("Loaded {} source mapping(s): {}", mappings.size(),
                mappings.stream().map(SourceMapping::getName).toList());
    }

    /** Package-private rather than private so a test can register a mapping without a file. */
    void register(SourceMapping mapping, String filename) {
        if (mapping == null || mapping.getName() == null) {
            log.error("Source mapping {} has no name; ignored", filename);
            return;
        }
        if (mapping.systemName().contains(com.example.correlation.domain.ExecutionKey.SEPARATOR)) {
            // System names are the namespace half of every execution key, and a key is
            // split at its first separator. A name containing one would make that split
            // ambiguous and quietly file observations under a system that does not exist.
            log.error("Source mapping '{}' observes system '{}', which contains '{}' — "
                            + "that sequence is reserved for namespacing; ignored",
                    mapping.getName(), mapping.systemName(),
                    com.example.correlation.domain.ExecutionKey.SEPARATOR);
            return;
        }
        if (mapping.getTopicPattern() == null) {
            log.error("Source mapping '{}' declares no topicPattern; it could never match",
                    mapping.getName());
            return;
        }
        // Two files claiming one name load quietly, so a stale copy left on the classpath
        // shadows the live mapping for the topics they both match and the only symptom is a
        // field that stopped being populated. Said out loud instead.
        //
        // Sharing a name is no longer how several files declare one observed system —
        // `system:` says that, and leaves each file its own name. A duplicate name is
        // therefore a mistake again rather than the only available workaround.
        mappings.stream()
                .filter(existing -> existing.getName().equals(mapping.getName()))
                .findFirst()
                .ifPresent(existing -> log.error(
                        "Source mapping '{}' is declared more than once ({} is a duplicate). "
                                + "Both are registered and the first whose topicPattern matches "
                                + "is used, so one of them may never be read. Give each file its "
                                + "own name, and use 'system:' if they observe one system.",
                        mapping.getName(), filename));
        mappings.add(mapping);
        if (mapping.getName().equals(mapping.systemName())) {
            log.info("Source '{}' reads topics matching {} — correlation key at '{}'",
                    mapping.getName(), mapping.getTopicPattern(),
                    SourceMapping.describe(mapping.getFields().getExecutionId()));
        } else {
            log.info("Source '{}' reads topics matching {} for observed system '{}' — "
                            + "correlation key at '{}'",
                    mapping.getName(), mapping.getTopicPattern(), mapping.systemName(),
                    SourceMapping.describe(mapping.getFields().getExecutionId()));
        }
    }

    /**
     * The mapping for a topic.
     *
     * <p>First declared match wins, so ordering between overlapping patterns is decided
     * by filename rather than left to chance. Never empty: an unmatched topic gets the
     * identity mapping.
     */
    public SourceMapping forTopic(String topic) {
        return mappings.stream()
                .filter(mapping -> mapping.matches(null, topic))
                .findFirst()
                .orElse(identity);
    }

    /**
     * The mapping for a topic that arrived on a named transport.
     *
     * <p>A mapping scoped to a different cluster is skipped even though its pattern matches
     * the topic name, and that skip is the whole point: the same topic name on two clusters
     * is two different systems' telemetry.
     *
     * <p>An observation with no origin -- an HTTP post, or a deployment reading a single
     * cluster -- matches only mappings that named no cluster. It is tempting to let it fall
     * through to the scoped one when that is the only mapping there is, and that is exactly
     * the guess invariant I8 forbids: the record cannot confirm where it came from, so
     * reading it as one cluster's telemetry is the platform deciding something the evidence
     * does not say. It gets the identity mapping instead, and the deployment that meant it
     * to be read says so by naming the cluster it arrives on.
     */
    public SourceMapping forTopic(String origin, String topic) {
        // Without an origin this is the single-transport question, and it is asked through
        // the one-argument method on purpose: that is the seam tests substitute, and a
        // deployment reading one cluster should not travel a different path here than the
        // one its tests exercise.
        if (origin == null || origin.isBlank()) return forTopic(topic);

        // A mapping that named this cluster is preferred over one that named none, whatever
        // order the files were read in. This is not the tie-break that ordering usually is:
        // between "the mapping for order-created on retail-eu" and "the mapping for
        // order-created anywhere", only one of them was written about the record in hand.
        // Leaving it to filename would mean a correct, specific declaration could be
        // shadowed by a general one that happens to sort earlier, and the symptom would be
        // two estates' runs quietly filed under one system.
        return mappings.stream()
                .filter(mapping -> origin.equals(mapping.getCluster()) && mapping.matches(topic))
                .findFirst()
                .orElseGet(() -> mappings.stream()
                        .filter(mapping -> mapping.matches(origin, topic))
                        .findFirst()
                        .orElse(identity));
    }

    public List<String> sourceNames() {
        return mappings.stream().map(SourceMapping::getName).toList();
    }

    /** The distinct observed systems these mappings read, which is what a key is namespaced by. */
    public Set<String> observedSystems() {
        return mappings.stream()
                .map(SourceMapping::systemName)
                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Every namespace an execution key can carry in this deployment.
     *
     * <p>The declared mappings, the namespaces they deliberately file aliases under, and
     * the identity mapping's own name for topics nothing declares. Resolving a raw
     * identifier means asking each of these and no others, so this set is what keeps that
     * from becoming a search for a key that merely ends in the right characters.
     */
    public Set<String> knownSourceNames() {
        Set<String> names = new LinkedHashSet<>();
        names.add(identity.getName());
        for (SourceMapping mapping : mappings) {
            // The observed system, not the file that reads it: a mapping name that is not
            // also a system name never appears in an execution key, so offering it here
            // would invite a lookup for a namespace nothing is ever filed under.
            names.add(mapping.systemName());
            names.add(mapping.aliasNamespace());
        }
        names.remove(null);
        return names;
    }
}
