package com.example.correlation.service.rca;

import com.example.correlation.repository.ServiceLifecycleEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The ablation switch removes the rule itself. Turning off only the Docker collector would
 * leave earlier lifecycle facts in the table for R25 to read, and the "events-only" numbers
 * would not be events-only.
 */
class ServiceDownRuleSwitchTest {

    private final ApplicationContextRunner context = new ApplicationContextRunner()
            // Boot's converters, so the rule's Duration property (PT1H) binds as in the app.
            .withInitializer(ctx -> ctx.getBeanFactory().setConversionService(
                    org.springframework.boot.convert.ApplicationConversionService.getSharedInstance()))
            .withBean(ServiceLifecycleEventRepository.class, () -> mock(ServiceLifecycleEventRepository.class))
            .withUserConfiguration(ServiceDownRule.class);

    @Test
    void onByDefault() {
        context.run(c -> assertThat(c).hasSingleBean(ServiceDownRule.class));
    }

    @Test
    void offMeansTheRuleIsNotThere() {
        context.withPropertyValues("correlation.liveness.enabled=false")
                .run(c -> assertThat(c).doesNotHaveBean(ServiceDownRule.class));
    }
}
