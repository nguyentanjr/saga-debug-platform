package com.example.correlation.service.rca;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The declared failure-message conventions, one entry per observed system.
 *
 * <p>Loaded from {@code failure-signals/*.yaml} beside the other declarations, so telling
 * the platform how a new system words its failures is adding a file, like telling it how
 * to read that system's events or what its workflow is.
 *
 * <p>An empty directory is normal and is logged at debug, not warn. Unlike a missing
 * workflow, declaring no failure signals is a coherent position: it says the deployment
 * does not claim to understand any source's failure text, and the rules that depend on it
 * then produce nothing rather than producing a guess.
 */
@Component
public class FailureSignalRegistry {

    private static final Logger log = LoggerFactory.getLogger(FailureSignalRegistry.class);

    /**
     * Where a deployment keeps declarations it did not build into the jar.
     *
     * <p>Taken through the constructor rather than injected into the field, unlike the
     * sibling registries. The paired F4 experiment has to build this registry outside
     * Spring and point it at the repository's own declarations, so that the replay reads
     * the same file production reads rather than a copy kept in test resources that can
     * drift away from it.
     */
    private final String declarationsDirectory;

    private final Map<String, FailureSignalDeclaration> bySource = new LinkedHashMap<>();

    public FailureSignalRegistry(
            @Value("${correlation.declarations.dir:}") String declarationsDirectory) {
        this.declarationsDirectory = declarationsDirectory;
    }

    @PostConstruct
    public void load() {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        try {
            List<Resource> resources = DeclarationFiles.find("failure-signals", declarationsDirectory);
            for (Resource resource : resources) {
                try (InputStream is = resource.getInputStream()) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> root = yamlMapper.readValue(is, Map.class);
                    Object declared = root.get("failureSignals");
                    if (declared == null) {
                        log.error("Failure-signal file {} has no 'failureSignals' key; ignored",
                                resource.getFilename());
                        continue;
                    }
                    FailureSignalDeclaration declaration =
                            yamlMapper.convertValue(declared, FailureSignalDeclaration.class);
                    register(declaration, resource.getFilename());
                } catch (Exception e) {
                    log.error("Failed to load failure-signal file {}: {}",
                            resource.getFilename(), e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error("Failed to scan for failure-signal files: {}", e.getMessage(), e);
        }

        if (bySource.isEmpty()) {
            log.debug("No failure-signal declarations. Rules that read a source's failure "
                    + "text produce no candidate, which is the intended behaviour when the "
                    + "platform has not been told how that source words its failures.");
        } else {
            log.info("Loaded failure-signal declaration(s) for source(s): {}", bySource.keySet());
        }
    }

    /**
     * Registers one declaration, refusing rather than choosing when two files claim a source.
     *
     * <p>Same stance as the workflow registry on ambiguity: two declarations for one source
     * is a deployment mistake, and letting file order settle it would mean the diagnosis of
     * a run depended on a directory listing.
     */
    private void register(FailureSignalDeclaration declaration, String filename) {
        if (declaration == null || declaration.getSource() == null
                || declaration.getSource().isBlank()) {
            log.error("Failure-signal file {} declares no source; ignored", filename);
            return;
        }
        FailureSignalDeclaration existing = bySource.get(declaration.getSource());
        if (existing != null) {
            log.error("Source '{}' already has a failure-signal declaration; {} ignored. "
                    + "Two declarations for one source is a deployment error, and choosing "
                    + "between them by file order would make a diagnosis depend on a "
                    + "directory listing.", declaration.getSource(), filename);
            return;
        }
        bySource.put(declaration.getSource(), declaration);
    }

    /** What the deployment has been told about how {@code sourceName} words its failures. */
    public Optional<FailureSignalDeclaration> forSource(String sourceName) {
        if (sourceName == null) return Optional.empty();
        return Optional.ofNullable(bySource.get(sourceName));
    }

    /** The sources a declaration exists for, for reporting what the deployment understands. */
    public java.util.Set<String> declaredSources() {
        return java.util.Collections.unmodifiableSet(bySource.keySet());
    }
}
