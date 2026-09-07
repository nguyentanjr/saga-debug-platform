package com.example.correlation.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Where the platform looks for the YAML it is told about a system with.
 *
 * <p>Workflow specifications and source mappings were discovered from the classpath
 * alone. Both are statements about a system being observed, not about this platform, so
 * observing one more system meant rebuilding and redeploying the correlation service —
 * for a file that changes nothing about the code inside the jar. That undercuts the claim
 * this whole layer exists to make: the cost of attaching to a system is a YAML file.
 *
 * <p>An operator sets {@code correlation.declarations.dir} to a directory holding
 * {@code ingestion/} and {@code workflows/} subdirectories. Files found there are read
 * alongside the ones packaged in the jar, and the packaged ones keep working untouched
 * when nothing is configured.
 *
 * <p>External files are read <em>after</em> the packaged ones, which matters because both
 * registries resolve a conflict by keeping the first declaration. A deployment cannot
 * silently redefine what the jar ships; it can only add. Overriding is a bigger decision
 * than a file appearing in a directory, and it is not one this class makes quietly.
 */
public final class DeclarationFiles {

    private static final Logger log = LoggerFactory.getLogger(DeclarationFiles.class);

    private DeclarationFiles() {
    }

    /**
     * @param kind      the subdirectory and classpath folder: {@code ingestion} or
     *                  {@code workflows}
     * @param directory the configured external directory, or null/blank when there is none
     * @return every YAML file to read, packaged ones first
     */
    public static List<Resource> find(String kind, String directory) {
        List<Resource> resources = new ArrayList<>(packaged(kind));
        resources.addAll(external(kind, directory));
        return resources;
    }

    private static List<Resource> packaged(String kind) {
        try {
            return Arrays.asList(new PathMatchingResourcePatternResolver()
                    .getResources("classpath*:" + kind + "/*.yaml"));
        } catch (Exception e) {
            log.error("Failed to scan the classpath for {} declarations: {}", kind, e.getMessage(), e);
            return List.of();
        }
    }

    private static List<Resource> external(String kind, String directory) {
        if (directory == null || directory.isBlank()) {
            return List.of();
        }

        File folder = new File(directory, kind);
        if (!folder.isDirectory()) {
            // Said out loud: a mistyped path would otherwise look exactly like a
            // deployment that declared nothing.
            log.warn("correlation.declarations.dir is set but {} is not a directory; "
                    + "no external {} declarations were read", folder.getAbsolutePath(), kind);
            return List.of();
        }

        File[] files = folder.listFiles((dir, fileName) ->
                fileName.endsWith(".yaml") || fileName.endsWith(".yml"));
        if (files == null || files.length == 0) {
            return List.of();
        }

        // The filesystem gives no order guarantee, and both registries let the first
        // declaration win a conflict. Sorting makes which one that is a property of the
        // filenames rather than of the machine.
        return Arrays.stream(files)
                .sorted(Comparator.comparing(File::getName))
                .map(file -> (Resource) new FileSystemResource(file))
                .toList();
    }
}
