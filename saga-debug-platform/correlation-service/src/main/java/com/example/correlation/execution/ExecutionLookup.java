package com.example.correlation.execution;

import com.example.correlation.domain.ExecutionAlias;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.ingestion.SourceMappingRegistry;
import com.example.correlation.repository.ExecutionAliasRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Which stored execution an identifier in a request path names.
 *
 * <h2>The gap this closes</h2>
 * Identity is {@code (source, rawId)} and reaches the database as {@code source::rawId}
 * (ADR 0002). The read API took the path variable and passed it to {@code findById}
 * unchanged, which is correct for a caller that got the identifier from this platform —
 * the dashboard, following a link from the list endpoint — and wrong for every caller
 * holding the identifier the observed system itself issued. An operator with an order
 * number, or the evaluation harness with the saga id order-service returned, asked for a
 * run that exists and was told it does not.
 *
 * <p>That is a defect in the API contract rather than in the identity model: the platform
 * knows the namespaces it files observations under, so it can answer the question instead
 * of making the caller guess at a prefix.
 *
 * <h2>Why this is not a search</h2>
 * Resolution never scans for a key ending in the given string. It builds the finite set of
 * keys the raw identifier could be — one per source name this deployment can assign — and
 * asks for each. Two consequences, both deliberate:
 *
 * <ul>
 *   <li>A raw identifier that names runs in two observed systems is <em>ambiguous</em>,
 *       and this reports that rather than returning whichever row came back first.
 *       Answering with one of two different systems' executions is the fabricated-run
 *       failure {@link ExecutionKey} exists to prevent, arriving through the front door.</li>
 *   <li>{@code ORD-1} of a system this deployment does not observe resolves to nothing,
 *       because its namespace is not in the candidate set.</li>
 * </ul>
 *
 * <p>An identifier that already carries a namespace is not widened to other sources. It
 * is a complete key and is taken as one.
 *
 * @see ExecutionKey
 */
@Component
public class ExecutionLookup {

    /**
     * A raw identifier that names an execution in more than one observed system.
     *
     * <p>Answered with 409 rather than 404 or a row: the run exists, and which one is
     * meant is a question the caller has to settle by naming a namespace. Picking one
     * here would hand back another system's execution under this system's identifier.
     */
    @ResponseStatus(HttpStatus.CONFLICT)
    public static class AmbiguousExecutionException extends RuntimeException {
        private final transient Set<String> candidates;

        AmbiguousExecutionException(String rawId, Set<String> candidates) {
            super("'" + rawId + "' names an execution in more than one observed system: "
                    + candidates + ". Ask for one of these keys instead.");
            this.candidates = candidates;
        }

        public Set<String> candidates() {
            return candidates;
        }
    }

    private final SagaInstanceRepository sagaInstances;
    private final ExecutionAliasRepository aliases;
    private final SourceMappingRegistry sourceMappings;

    public ExecutionLookup(SagaInstanceRepository sagaInstances,
            ExecutionAliasRepository aliases,
            SourceMappingRegistry sourceMappings) {
        this.sagaInstances = sagaInstances;
        this.aliases = aliases;
        this.sourceMappings = sourceMappings;
    }

    /**
     * The stored execution id for what a caller asked about.
     *
     * @param given either a full {@code source::rawId} key or the raw identifier the
     *              observed system issued
     * @return the canonical execution id, or empty when nothing is filed under it
     * @throws AmbiguousExecutionException when a raw identifier names executions in more
     *                                     than one observed system, which is a question
     *                                     only the caller can settle
     */
    public Optional<String> resolve(String given) {
        if (given == null || given.isBlank()) {
            return Optional.empty();
        }

        if (given.contains(ExecutionKey.SEPARATOR)) {
            return executionUnder(ExecutionKey.parse(given));
        }

        // Every namespace this deployment can file an observation under, and nothing else.
        Set<String> found = new LinkedHashSet<>();
        for (String source : sourceMappings.knownSourceNames()) {
            executionUnder(ExecutionKey.of(source, given)).ifPresent(found::add);
        }

        if (found.isEmpty()) {
            return Optional.empty();
        }
        if (found.size() > 1) {
            throw new AmbiguousExecutionException(given, found);
        }
        return Optional.of(found.iterator().next());
    }

    /**
     * What this key names: the execution stored under it, or the one it is an alias of.
     *
     * <p>The alias step matters after a fold. Two identifiers turned out to be one run,
     * everything moved to the survivor, and the absorbed key survives only as an alias
     * row — so a caller still holding it is asking a question with an answer.
     */
    private Optional<String> executionUnder(ExecutionKey key) {
        if (sagaInstances.existsById(key.canonical())) {
            return Optional.of(key.canonical());
        }
        return aliases.findById(ExecutionAlias.Key.of(key))
                .map(ExecutionAlias::getExecutionId)
                .filter(sagaInstances::existsById);
    }
}
