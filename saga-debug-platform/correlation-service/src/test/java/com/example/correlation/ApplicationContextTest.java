package com.example.correlation;

import com.example.correlation.messaging.SagaEventConsumer;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.AnomalyDetectionService;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.RcaService;
import com.example.correlation.service.RetentionService;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Starts the application context.
 *
 * <p>Several things only fail at startup and are invisible to every other test here:
 * Spring Data parses each derived query name against the JPA metamodel, {@code @Lazy}
 * self-injection either resolves or reports a cycle, and conditional beans either
 * appear or do not. None of that is compile-checked, so without this test the first
 * time the wiring was exercised would be a deployment.
 *
 * <p>Runs against H2 with Flyway off: the point is bean wiring and query derivation,
 * not schema or SQL behaviour, both of which need real Postgres.
 */
@SpringBootTest
@ActiveProfiles("test")
class ApplicationContextTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void contextLoadsWithEveryRepositoryQueryDerived() {
        assertThat(context.getBean(SagaInstanceRepository.class)).isNotNull();
        assertThat(context.getBean(SagaEventRepository.class)).isNotNull();
        assertThat(context.getBean(AnomalyRepository.class)).isNotNull();
        assertThat(context.getBean(RcaResultRepository.class)).isNotNull();
    }

    @Test
    void coreServicesAreWired() {
        assertThat(context.getBean(CorrelationService.class)).isNotNull();
        assertThat(context.getBean(AnomalyDetectionService.class)).isNotNull();
        assertThat(context.getBean(SagaEventConsumer.class)).isNotNull();
    }

    @Test
    void rcaServiceResolvesItsOwnLazyProxy() {
        // The scheduled sweep calls analyze through this reference so each saga gets its
        // own transaction. A plain self-injection would be a circular dependency and
        // fail the context; @Lazy is what makes it legal.
        assertThat(context.getBean(RcaService.class)).isNotNull();
    }

    @Test
    void retentionIsAbsentUnlessExplicitlyEnabled() {
        assertThat(context.getBeanNamesForType(RetentionService.class)).isEmpty();
    }

    @Test
    void everyWorkflowOnTheClasspathIsRegistered() {
        // ORDER_SAGA ships in main resources; FULFILLMENT_SAGA exists only as a test
        // resource. Both are discovered by the same classpath scan, which is what
        // "adding a workflow is adding a file" has to mean in practice — no Java change
        // was needed to make the registry aware of the second one.
        WorkflowRegistry registry = context.getBean(WorkflowRegistry.class);

        assertThat(registry.workflowNames())
                .contains("ORDER_SAGA", "FULFILLMENT_SAGA");
        assertThat(registry.forStartEvent("ORDER_CREATED"))
                .get().extracting(WorkflowSpec::getName).isEqualTo("ORDER_SAGA");
        assertThat(registry.forStartEvent("SHIPMENT_CREATED"))
                .get().extracting(WorkflowSpec::getName).isEqualTo("FULFILLMENT_SAGA");
        assertThat(registry.forStartEvent("PAYMENT_COMPLETED")).isEmpty();
    }

    /**
     * L9: every scheduled pass runs against the test schema without a SQL error.
     *
     * <p>The sweeps start with the context, and schema-h2.sql had fallen behind the
     * migrations (source_name, correlation_basis, trace_id, transport_ref, retraction, the
     * alias table), so each tick logged {@code Column "SI1_0.SOURCE_NAME" not found} while
     * this class stayed green — the very noise its schema comment warns will hide a real
     * wiring failure. Called directly rather than through ScheduledSweeps: a lease held by
     * the background scheduler would skip the pass and let this pass without running it.
     */
    @Test
    void everyScheduledPassRunsAgainstTheTestSchema() {
        AnomalyDetectionService detection = context.getBean(AnomalyDetectionService.class);
        RcaService rca = context.getBean(RcaService.class);

        assertThatCode(() -> {
            detection.detectStuckSagas();
            detection.detectStepTimeouts();
            detection.detectMissingTerminalEvents();
            detection.detectCompensationMissing();
            detection.detectMissingCompensationAck();
            detection.detectDelayedCompensation();
            rca.autoAnalyzeNewlyDetectedAnomalies();
            rca.autoAnalyzeEndedRuns();
        }).doesNotThrowAnyException();
    }

    @Test
    void kafkaErrorHandlerIsRegistered() {
        assertThat(context.getBean(DefaultErrorHandler.class)).isNotNull();
    }
}
