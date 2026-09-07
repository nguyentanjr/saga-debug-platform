package com.example.correlation.execution;

import com.example.correlation.domain.ExecutionAlias;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.ingestion.SourceMappingRegistry;
import com.example.correlation.repository.ExecutionAliasRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Addressing an execution by the identifier the observed system issued.
 *
 * <h2>The failure these pin down</h2>
 * Identity became {@code (source, rawId)} and the stored key became {@code source::rawId},
 * but the read API kept passing the path variable to {@code findById} unchanged. Callers
 * holding the platform's own key — the dashboard, following a link from the list endpoint
 * — were fine. Every caller holding the source's own identifier got 404 for a run that was
 * in the database the whole time, which is how the evaluation harness stopped being able
 * to follow a single execution and the project's measured numbers stopped being
 * reproducible.
 *
 * <p>The remedy must not become the thing the namespace exists to prevent, so the cases
 * below are as much about what resolution refuses as about what it finds.
 */
class ExecutionLookupTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private Map<String, SagaInstance> instances;
    private Map<ExecutionAlias.Key, ExecutionAlias> aliasRows;
    private Set<String> sourceNames;

    private ExecutionLookup lookup;

    @BeforeEach
    void setUp() {
        instances = new LinkedHashMap<>();
        aliasRows = new LinkedHashMap<>();
        sourceNames = new LinkedHashSet<>();
        sourceNames.add(ExecutionKey.DEFAULT_SOURCE);

        SagaInstanceRepository sagaInstances = mock(SagaInstanceRepository.class);
        when(sagaInstances.existsById(anyString()))
                .thenAnswer(call -> instances.containsKey(call.<String>getArgument(0)));

        ExecutionAliasRepository aliases = mock(ExecutionAliasRepository.class);
        when(aliases.findById(any(ExecutionAlias.Key.class)))
                .thenAnswer(call -> Optional.ofNullable(aliasRows.get(call.getArgument(0))));

        SourceMappingRegistry registry = mock(SourceMappingRegistry.class);
        when(registry.knownSourceNames()).thenAnswer(call -> sourceNames);

        lookup = new ExecutionLookup(sagaInstances, aliases, registry);
    }

    // ── the key the platform stores ───────────────────────────────────────────

    /** What the dashboard does: follow a link carrying the full key. Unchanged behaviour. */
    @Test
    void aStoredKeyResolvesToItself() {
        run("default::ORD-1");

        assertThat(lookup.resolve("default::ORD-1")).contains("default::ORD-1");
    }

    /**
     * A full key is a complete statement of identity, so it is asked as given and never
     * widened to other namespaces. A key naming a source that holds no such run resolves
     * to nothing rather than to the same-named run in another system.
     */
    @Test
    void aStoredKeyIsNotWidenedToOtherSources() {
        sourceNames.add("shop");
        sourceNames.add("partner");
        run("shop::ORD-1");

        assertThat(lookup.resolve("partner::ORD-1")).isEmpty();
    }

    // ── the identifier the observed system issued ─────────────────────────────

    /**
     * The case that was broken. The harness holds the saga id order-service returned and
     * knows nothing about namespaces; the platform knows exactly which namespaces it could
     * have filed that observation under.
     */
    @Test
    void aRawIdentifierResolvesToTheOnlyExecutionItCanName() {
        run("default::7f3c-order");

        assertThat(lookup.resolve("7f3c-order")).contains("default::7f3c-order");
    }

    /** One deployment observing several systems: the right namespace, not the first one. */
    @Test
    void aRawIdentifierResolvesWithinWhicheverSourceHoldsIt() {
        sourceNames.add("shop");
        sourceNames.add("partner");
        run("partner::ORD-9");

        assertThat(lookup.resolve("ORD-9")).contains("partner::ORD-9");
    }

    // ── nothing filed under it ────────────────────────────────────────────────

    @Test
    void anUnknownRawIdentifierResolvesToNothing() {
        run("default::ORD-1");

        assertThat(lookup.resolve("ORD-404")).isEmpty();
    }

    @Test
    void anUnknownStoredKeyResolvesToNothing() {
        assertThat(lookup.resolve("default::ORD-404")).isEmpty();
    }

    @Test
    void nothingAtAllResolvesToNothing() {
        assertThat(lookup.resolve(null)).isEmpty();
        assertThat(lookup.resolve("")).isEmpty();
        assertThat(lookup.resolve("   ")).isEmpty();
    }

    // ── an execution known by another name ────────────────────────────────────

    /**
     * After a fold, the absorbed identifier survives only as an alias row. A caller still
     * holding it is asking about a run that exists, so it is answered with the survivor
     * rather than with 404.
     */
    @Test
    void anAliasResolvesToTheExecutionItWasFoldedInto() {
        run("default::ORD-1");
        alias("default", "PAY-7", "default::ORD-1");

        assertThat(lookup.resolve("PAY-7")).contains("default::ORD-1");
        assertThat(lookup.resolve("default::PAY-7")).contains("default::ORD-1");
    }

    /**
     * An alias whose execution is gone — retention removed the run — is not an answer.
     * Returning the dangling id would send the caller to a 404 one hop further on.
     */
    @Test
    void anAliasPointingAtNothingIsNotAnAnswer() {
        alias("default", "PAY-7", "default::ORD-GONE");

        assertThat(lookup.resolve("PAY-7")).isEmpty();
    }

    // ── the refusal that keeps this from being a search ───────────────────────

    /**
     * The whole reason identity is namespaced: {@code ORD-1} in two observed systems is
     * two runs. Asked without a namespace, resolution states that and names both keys.
     * Returning either would hand back one system's execution under another system's
     * identifier — the fabricated-run failure, re-entering through the API.
     */
    @Test
    void aRawIdentifierNamingTwoSystemsIsRefusedRatherThanPicked() {
        sourceNames.add("shop");
        sourceNames.add("partner");
        run("shop::ORD-1");
        run("partner::ORD-1");

        assertThatThrownBy(() -> lookup.resolve("ORD-1"))
                .isInstanceOf(ExecutionLookup.AmbiguousExecutionException.class)
                .hasMessageContaining("shop::ORD-1")
                .hasMessageContaining("partner::ORD-1");
    }

    /** Naming the namespace settles it, which is what the refusal asks the caller to do. */
    @Test
    void namingTheSourceSettlesAnAmbiguousIdentifier() {
        sourceNames.add("shop");
        sourceNames.add("partner");
        run("shop::ORD-1");
        run("partner::ORD-1");

        assertThat(lookup.resolve("shop::ORD-1")).contains("shop::ORD-1");
        assertThat(lookup.resolve("partner::ORD-1")).contains("partner::ORD-1");
    }

    /**
     * Two names for one run are not two runs. An identifier reaching the same execution
     * directly in one namespace and through an alias in another is answered, not refused:
     * there is only one thing it can mean.
     */
    @Test
    void twoRoutesToOneExecutionAreNotAmbiguous() {
        sourceNames.add("shop");
        run("shop::ORD-1");
        alias("default", "ORD-1", "shop::ORD-1");

        assertThat(lookup.resolve("ORD-1")).contains("shop::ORD-1");
    }

    /**
     * A raw identifier containing a single colon is not a namespaced key. Real identifiers
     * carry them — URNs, prefixed business references — and splitting on one would look up
     * a source that does not exist.
     */
    @Test
    void aSingleColonInsideAnIdentifierIsPartOfIt() {
        run("default::urn:order:1");

        assertThat(lookup.resolve("urn:order:1")).contains("default::urn:order:1");
    }

    private void run(String executionId) {
        SagaInstance instance = new SagaInstance();
        instance.setSagaId(executionId);
        instance.setSourceName(ExecutionKey.parse(executionId).source());
        instance.setStartTime(T0);
        instance.setLastUpdated(T0);
        instances.put(executionId, instance);
    }

    private void alias(String source, String rawId, String executionId) {
        aliasRows.put(new ExecutionAlias.Key(source, rawId),
                new ExecutionAlias(source, rawId, executionId, "e1", T0));
    }
}
