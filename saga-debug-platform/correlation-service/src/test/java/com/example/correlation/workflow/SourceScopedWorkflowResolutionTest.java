package com.example.correlation.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Judging a run against the right system's specification when two systems are watched.
 *
 * <h2>The limitation these were written against</h2>
 * Event names are not globally unique. {@code ORDER_CREATED} is what an order system's
 * first event is called, in every company that has one. Resolution keyed on the event name
 * alone, so attaching a second observed system whose specification happened to use the same
 * name made the <em>first</em> system's runs unrecognisable: two workflows claimed the
 * start event, the registry refused to guess, and every execution fell to
 * {@code UNRESOLVED} — where the spec-driven detectors abstain.
 *
 * <p>Reproduced on a running deployment before the change: the same event, from the same
 * source, resolved to {@code ORDER_SAGA} before a partner system's specification was
 * dropped into the declarations directory and to {@code UNRESOLVED} after it. The observed
 * systems were both fine; what changed was only what else the observer had been told about.
 *
 * <h2>What fixes it, and what deliberately does not</h2>
 * The observation knows which system it came from. Narrowing the candidates by that turns a
 * collision into a filter, and no guess is involved. Refusing to answer stays the behaviour
 * whenever the narrowing does not separate the candidates: a wrong specification does not
 * fail visibly, it measures a run against another system's steps and timeouts and reports
 * every difference as a defect.
 */
class SourceScopedWorkflowResolutionTest {

    private static final String SHOP = "shop";
    private static final String PARTNER = "partner";

    // ── the collision that started this ───────────────────────────────────────

    /**
     * Two systems, two specifications, one event name. Each observation resolves to the
     * specification written about the system it came from.
     */
    @Test
    void twoSystemsSharingAnEventNameEachResolveToTheirOwnWorkflow() {
        WorkflowRegistry registry = registryWith(shopOrderSaga(), partnerOrderSaga());

        assertThat(resolveFresh(registry, SHOP, "ORDER_CREATED"))
                .map(WorkflowSpec::getName).contains("SHOP_ORDER_SAGA");
        assertThat(resolveFresh(registry, PARTNER, "ORDER_CREATED"))
                .map(WorkflowSpec::getName).contains("PARTNER_ORDER_SAGA");
    }

    /**
     * The regression in its original form: adding the second system must not change what
     * the first system's runs resolve to.
     */
    @Test
    void addingASecondSystemDoesNotUnresolveTheFirst() {
        Optional<WorkflowSpec> alone =
                resolveFresh(registryWith(shopOrderSaga()), SHOP, "ORDER_CREATED");
        Optional<WorkflowSpec> alongside =
                resolveFresh(registryWith(shopOrderSaga(), partnerOrderSaga()), SHOP, "ORDER_CREATED");

        assertThat(alone).map(WorkflowSpec::getName).contains("SHOP_ORDER_SAGA");
        assertThat(alongside).map(WorkflowSpec::getName).contains("SHOP_ORDER_SAGA");
    }

    // ── the deployment that watches one system ────────────────────────────────

    /**
     * A specification naming no source applies to any of them, so no existing file had to
     * change and a single-system deployment resolves exactly as it always did.
     */
    @Test
    void aSpecificationNamingNoSourceStillAnswersForAnySource() {
        WorkflowRegistry registry = registryWith(unscopedOrderSaga());

        assertThat(resolveFresh(registry, "whatever-source", "ORDER_CREATED"))
                .map(WorkflowSpec::getName).contains("ORDER_SAGA");
        assertThat(resolveFresh(registry, null, "ORDER_CREATED"))
                .map(WorkflowSpec::getName).contains("ORDER_SAGA");
    }

    // ── refusing to guess ─────────────────────────────────────────────────────

    /**
     * Two specifications that both apply to this source and both claim the event. Nothing
     * separates them, so nothing is chosen. Measuring the run against a coin flip would
     * produce a full report of defects that are only disagreements with the wrong document.
     */
    @Test
    void anAmbiguityThatNarrowingDoesNotResolveStaysUnresolved() {
        WorkflowRegistry registry = registryWith(unscopedOrderSaga(), unscopedOtherSaga());

        assertThat(resolveFresh(registry, SHOP, "ORDER_CREATED")).isEmpty();
    }

    /** A source no scoped specification names does not borrow another system's. */
    @Test
    void anUnknownSourceDoesNotBorrowAScopedWorkflow() {
        WorkflowRegistry registry = registryWith(shopOrderSaga(), partnerOrderSaga());

        assertThat(resolveFresh(registry, "some-third-system", "ORDER_CREATED")).isEmpty();
    }

    // ── the other places an answer can come from ──────────────────────────────

    /**
     * Once an execution has resolved, that is what it is. Every observation after the first
     * takes this rung, so a workflow cannot change underneath a running execution.
     */
    @Test
    void anExecutionThatAlreadyResolvedKeepsItsWorkflow() {
        WorkflowRegistry registry = registryWith(shopOrderSaga(), partnerOrderSaga());

        // Arriving from the partner's source, but this run is already known to be the shop's.
        assertThat(registry.resolve("SHOP_ORDER_SAGA", null, PARTNER, "ORDER_CREATED"))
                .map(WorkflowSpec::getName).contains("SHOP_ORDER_SAGA");
    }

    /**
     * A source mapping may name the workflow instead, for the owner who would rather say it
     * once in the mapping than edit a specification they did not write.
     */
    @Test
    void theSourceMappingCanNameTheWorkflowWhenSpecificationsDoNot() {
        WorkflowRegistry registry = registryWith(unscopedOrderSaga(), unscopedOtherSaga());

        assertThat(registry.resolve(null, "ORDER_SAGA", SHOP, "ORDER_CREATED"))
                .map(WorkflowSpec::getName).contains("ORDER_SAGA");
    }

    /**
     * The specification is the authority on which systems it describes, so it outranks the
     * mapping's claim. The disagreement is a configuration mistake and is logged as one —
     * silently preferring either would leave one of the two files quietly wrong.
     */
    @Test
    void aSpecificationsOwnSourceClaimOutranksTheMappings() {
        WorkflowRegistry registry = registryWith(shopOrderSaga(), partnerOrderSaga());

        assertThat(registry.resolve(null, "PARTNER_ORDER_SAGA", SHOP, "ORDER_CREATED"))
                .map(WorkflowSpec::getName).contains("SHOP_ORDER_SAGA");
    }

    /** An event no specification declares as a start resolves to nothing, as before. */
    @Test
    void anUndeclaredStartEventResolvesToNothing() {
        WorkflowRegistry registry = registryWith(shopOrderSaga());

        assertThat(resolveFresh(registry, SHOP, "SOMETHING_ELSE_ENTIRELY")).isEmpty();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /** Resolution for an execution that has not resolved yet and whose mapping is silent. */
    private static Optional<WorkflowSpec> resolveFresh(WorkflowRegistry registry,
            String sourceName, String eventType) {
        return registry.resolve(null, null, sourceName, eventType);
    }

    private static WorkflowRegistry registryWith(WorkflowSpec... specs) {
        WorkflowRegistry registry = new WorkflowRegistry();
        for (WorkflowSpec spec : specs) {
            registry.register(spec, spec.getName() + ".yaml");
        }
        return registry;
    }

    private static WorkflowSpec shopOrderSaga() {
        return spec("SHOP_ORDER_SAGA", "sources: [shop]\n");
    }

    private static WorkflowSpec partnerOrderSaga() {
        return spec("PARTNER_ORDER_SAGA", "sources: [partner]\n");
    }

    private static WorkflowSpec unscopedOrderSaga() {
        return spec("ORDER_SAGA", "");
    }

    private static WorkflowSpec unscopedOtherSaga() {
        return spec("OTHER_SAGA", "");
    }

    /** The smallest specification the validator accepts, starting on ORDER_CREATED. */
    private static WorkflowSpec spec(String name, String sourcesLine) {
        String yaml = """
                name: %s
                version: "1.0"
                %ssteps:
                  - id: CREATE
                    service: a-service
                    timeout: 30s
                    events:
                      completed: ORDER_CREATED
                  - id: FINISH
                    service: b-service
                    timeout: 30s
                    events:
                      completed: RUN_FINISHED
                      failed: RUN_FAILED
                transitions:
                  - from: STARTED
                    to: CREATE
                    condition: on_success
                  - from: CREATE
                    to: FINISH
                    condition: on_success
                  - from: FINISH
                    to: COMPLETED
                    condition: on_success
                  - from: FINISH
                    to: FAILED
                    condition: on_failure
                """.formatted(name, sourcesLine);
        try {
            return new ObjectMapper(new YAMLFactory()).readValue(yaml, WorkflowSpec.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test specification", e);
        }
    }

    /** Guards the assumption the fixtures rest on: these really do share a start event. */
    @Test
    void theFixturesActuallyCollide() {
        assertThat(shopOrderSaga().startEventTypes()).contains("ORDER_CREATED");
        assertThat(partnerOrderSaga().startEventTypes()).contains("ORDER_CREATED");
        assertThat(Map.of("shop", shopOrderSaga().getSources(),
                        "partner", partnerOrderSaga().getSources()))
                .containsEntry("shop", java.util.List.of("shop"))
                .containsEntry("partner", java.util.List.of("partner"));
    }
}
