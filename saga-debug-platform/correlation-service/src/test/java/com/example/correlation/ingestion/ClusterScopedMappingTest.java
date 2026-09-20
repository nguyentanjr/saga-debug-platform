package com.example.correlation.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Telling two clusters' identically named topics apart.
 *
 * <p>This is the half of multi-cluster reading that would fail silently. A topic name is
 * unique inside one cluster and nowhere else: two estates both having {@code order-created}
 * is ordinary, not perverse. If the cluster a record came from does not reach the source
 * mapping, the platform reads both under one mapping, files both systems' runs in one
 * identity namespace, and then reports the differences between two unrelated systems as
 * faults in one of them — the exact failure invariant I4 exists to prevent, arriving
 * through a new door.
 *
 * <p>Nothing here is about performance or convenience. It is about what the platform is
 * entitled to conclude from a topic name.
 */
class ClusterScopedMappingTest {

    private static SourceMapping mapping(String name, String pattern, String cluster) {
        SourceMapping mapping = new SourceMapping();
        mapping.setName(name);
        mapping.setTopicPattern(pattern);
        mapping.setCluster(cluster);
        return mapping;
    }

    private static SourceMappingRegistry registryOf(SourceMapping... mappings) {
        SourceMappingRegistry registry = new SourceMappingRegistry();
        for (SourceMapping mapping : mappings) {
            registry.register(mapping, mapping.getName() + ".yaml");
        }
        return registry;
    }

    @Test
    @DisplayName("the same topic name on two clusters reaches two different mappings")
    void sameTopicOnTwoClustersIsTwoSources() {
        SourceMappingRegistry registry = registryOf(
                mapping("eu-orders", "order-created", "retail-eu"),
                mapping("us-orders", "order-created", "logistics-us"));

        assertThat(registry.forTopic("retail-eu", "order-created").getName()).isEqualTo("eu-orders");
        assertThat(registry.forTopic("logistics-us", "order-created").getName()).isEqualTo("us-orders");
    }

    @Test
    @DisplayName("a mapping scoped to one cluster does not claim another's topic")
    void aScopedMappingStaysOnItsCluster() {
        SourceMappingRegistry registry = registryOf(mapping("eu-orders", "order-created", "retail-eu"));

        assertThat(registry.forTopic("logistics-us", "order-created").getName())
                .isNotEqualTo("eu-orders");
    }

    @Test
    @DisplayName("an unscoped mapping reads whatever cluster a record came from")
    void unscopedMappingsAreUnchanged() {
        SourceMappingRegistry registry = registryOf(mapping("any-orders", "order-created", null));

        assertThat(registry.forTopic("retail-eu", "order-created").getName()).isEqualTo("any-orders");
        assertThat(registry.forTopic("logistics-us", "order-created").getName()).isEqualTo("any-orders");
        assertThat(registry.forTopic("order-created").getName()).isEqualTo("any-orders");
    }

    @Test
    @DisplayName("an observation with no origin never matches a cluster-scoped mapping")
    void originlessObservationsDoNotMatchScopedMappings() {
        SourceMappingRegistry registry = registryOf(mapping("eu-orders", "order-created", "retail-eu"));

        // An HTTP post carries no cluster, and a single-cluster deployment says nothing
        // about which one. Reading either as EU telemetry would be the platform deciding
        // something the evidence does not say, which is what I8 forbids.
        assertThat(registry.forTopic(null, "order-created").getName()).isNotEqualTo("eu-orders");
        assertThat(registry.forTopic("order-created").getName()).isNotEqualTo("eu-orders");
    }

    @Test
    @DisplayName("between a scoped and an unscoped mapping, declaration order still decides")
    void declarationOrderStillDecides() {
        SourceMappingRegistry registry = registryOf(
                mapping("eu-orders", "order-.*", "retail-eu"),
                mapping("any-orders", "order-.*", null));

        assertThat(registry.forTopic("retail-eu", "order-created").getName()).isEqualTo("eu-orders");
        assertThat(registry.forTopic("logistics-us", "order-created").getName()).isEqualTo("any-orders");
    }

    @Test
    @DisplayName("an existing single-cluster deployment resolves exactly as before")
    void singleClusterDeploymentsAreUnaffected() {
        SourceMappingRegistry packaged = new SourceMappingRegistry();
        packaged.load();

        // The declarations that ship with this build name no cluster, so every one of them
        // has to keep answering the question it answered before any of this existed.
        for (String topic : new String[]{"order-created", "payment-completed", "inventory-reserved"}) {
            assertThat(packaged.forTopic(null, topic).getName())
                    .isEqualTo(packaged.forTopic(topic).getName());
        }
    }

    @Test
    @DisplayName("cluster: is read out of the declaration file, not only settable from Java")
    void clusterBindsFromYaml() throws Exception {
        String yaml = """
                source:
                  name: eu-orders
                  cluster: retail-eu
                  topicPattern: "order-.*"
                """;
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        @SuppressWarnings("unchecked")
        Map<String, Object> root = yamlMapper.readValue(yaml, Map.class);
        SourceMapping parsed = yamlMapper.convertValue(root.get("source"), SourceMapping.class);

        assertThat(parsed.getCluster()).isEqualTo("retail-eu");
        assertThat(parsed.matches("retail-eu", "order-created")).isTrue();
        assertThat(parsed.matches("logistics-us", "order-created")).isFalse();
    }

    @Test
    @DisplayName("the cluster travels on the observation, so ingestion can use it")
    void theObservationCarriesItsOrigin() {
        RawObservation fromEu = new RawObservation("order-created", "{}", Instant.now(),
                "order-created-0-1", null, Map.of(), "retail-eu");
        RawObservation fromHttp = new RawObservation("order-created", "{}", Instant.now(),
                null, null, Map.of());

        assertThat(fromEu.origin()).isEqualTo("retail-eu");
        // The six-argument form is what every existing caller means: one transport, and
        // nothing to say about which.
        assertThat(fromHttp.origin()).isNull();
    }
}
