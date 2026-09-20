package com.example.correlation.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;

/**
 * Reading two Kafka clusters at once.
 *
 * <p>This file covers one of the two claims: that the number of clusters is configuration,
 * so a deployment declaring two gets two listeners without anyone editing Java. The other
 * claim — that telling the clusters apart reaches the source mapping, because a topic name
 * is only unique inside one cluster — is the one that would fail silently, and it is
 * covered where mappings are resolved, in {@code ClusterScopedMappingTest}.
 */
class MultiClusterIngestionTest {

    private static KafkaProperties kafkaProperties(String bootstrap) {
        KafkaProperties properties = new KafkaProperties();
        properties.setBootstrapServers(List.of(bootstrap));
        properties.getListener().setConcurrency(3);
        return properties;
    }

    private static KafkaSourceProperties.Cluster cluster(String name, String bootstrap, String pattern) {
        KafkaSourceProperties.Cluster cluster = new KafkaSourceProperties.Cluster();
        cluster.setName(name);
        cluster.setBootstrapServers(bootstrap);
        cluster.setTopicPattern(pattern);
        return cluster;
    }

    private static KafkaClusterListeners listenersFor(KafkaSourceProperties sources) {
        CommonErrorHandler errorHandler = new DefaultErrorHandler();
        return new KafkaClusterListeners(kafkaProperties("localhost:9092"), sources,
                new SagaEventConsumer(null), errorHandler);
    }

    @Nested
    @DisplayName("how many listeners get built")
    class ContainerWiring {

        @Test
        @DisplayName("a deployment that declares nothing still reads its one cluster")
        void singleClusterNeedsNoDeclaration() {
            assertThat(listenersFor(new KafkaSourceProperties()).containerCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("declaring two clusters builds two listeners, with no code change")
        void twoDeclaredClustersBuildTwoListeners() {
            KafkaSourceProperties sources = new KafkaSourceProperties();
            sources.setClusters(List.of(
                    cluster("retail-eu", "kafka-eu:9092", "order-.*"),
                    cluster("logistics-us", "kafka-us:9092", "shipment-.*")));

            assertThat(listenersFor(sources).containerCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("an incomplete declaration is skipped, not started on a guess")
        void incompleteDeclarationIsSkipped() {
            KafkaSourceProperties.Cluster noPattern = new KafkaSourceProperties.Cluster();
            noPattern.setName("half-declared");
            noPattern.setBootstrapServers("kafka-eu:9092");

            KafkaSourceProperties sources = new KafkaSourceProperties();
            sources.setClusters(List.of(noPattern, cluster("retail-eu", "kafka-eu:9092", "order-.*")));

            // Reading it under an invented pattern would produce findings about a system
            // nobody asked to observe, so the usable one runs and the other does not.
            assertThat(listenersFor(sources).containerCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("a name declared twice is refused rather than resolved arbitrarily")
        void duplicateClusterNameIsRefused() {
            KafkaSourceProperties sources = new KafkaSourceProperties();
            sources.setClusters(List.of(
                    cluster("retail", "kafka-a:9092", "order-.*"),
                    cluster("retail", "kafka-b:9092", "payment-.*")));

            // A mapping scoped to "retail" could not say which of the two it meant.
            assertThat(listenersFor(sources).containerCount()).isEqualTo(1);
        }
    }
}
