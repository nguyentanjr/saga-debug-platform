package com.example.correlation.ingestion;

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
import java.util.List;
import java.util.Map;

/**
 * The declared ways of reading external systems' events.
 *
 * <p>Loaded from {@code classpath*:ingestion/*.yaml}, so observing a new system is
 * adding a file — the same shape as adding a workflow. A topic that matches no mapping
 * falls back to the identity mapping, which reads this platform's own field names, so
 * producers already emitting the envelope keep working with nothing declared.
 */
@Component
public class SourceMappingRegistry {

    private static final Logger log = LoggerFactory.getLogger(SourceMappingRegistry.class);

    private static final String LOCATION_PATTERN = "classpath*:ingestion/*.yaml";

    private final List<SourceMapping> mappings = new ArrayList<>();
    private final SourceMapping identity = SourceMapping.identity();

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

    private void register(SourceMapping mapping, String filename) {
        if (mapping == null || mapping.getName() == null) {
            log.error("Source mapping {} has no name; ignored", filename);
            return;
        }
        if (mapping.getTopicPattern() == null) {
            log.error("Source mapping '{}' declares no topicPattern; it could never match",
                    mapping.getName());
            return;
        }
        mappings.add(mapping);
        log.info("Source '{}' reads topics matching {} — correlation key at '{}'",
                mapping.getName(), mapping.getTopicPattern(),
                mapping.getFields().getExecutionId());
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
                .filter(mapping -> mapping.matches(topic))
                .findFirst()
                .orElse(identity);
    }

    public List<String> sourceNames() {
        return mappings.stream().map(SourceMapping::getName).toList();
    }
}
