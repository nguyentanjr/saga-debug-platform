package com.example.correlation.messaging;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The Kafka clusters this deployment reads, declared rather than compiled in.
 *
 * <h2>Why this exists</h2>
 * Everything else about attaching to a new system is a file and a restart: how to read its
 * events, what its saga is supposed to do, what a symptom means there. One thing was not.
 * The service had a single {@code bootstrap-servers} and a single topic pattern, so two
 * observed systems living on two clusters — which is the normal shape of a real estate,
 * not an exotic one — meant editing Java. That put a hard limit on the zero-touch claim
 * exactly one layer below where the claim was being made: the integration was three YAML
 * files only as long as the new system happened to publish to the cluster this build was
 * already pointed at.
 *
 * <h2>Compatibility</h2>
 * A deployment that declares no cluster keeps reading {@code spring.kafka.bootstrap-servers}
 * with {@code correlation.kafka.topic-pattern}, exactly as before. Nothing existing has to
 * be rewritten to stand still.
 *
 * <pre>
 * correlation:
 *   kafka:
 *     topic-pattern: "order-.*|payment-.*"      # the single-cluster form, still valid
 *     clusters:
 *       - name: retail-eu
 *         bootstrap-servers: kafka-eu:9092
 *         topic-pattern: "order-.*|payment-.*"
 *       - name: logistics-us
 *         bootstrap-servers: kafka-us:9092
 *         topic-pattern: "shipment-.*"
 *         group-id: correlation-engine-group      # optional, defaults to the shared one
 *         concurrency: 1                          # optional
 * </pre>
 *
 * <p>A {@code name} is not decoration. It is what a source mapping's {@code cluster:} field
 * refers to, and the only way the platform can tell two clusters' identically named topics
 * apart.
 */
@ConfigurationProperties(prefix = "correlation.kafka")
public class KafkaSourceProperties {

    /**
     * The single-cluster pattern, used when no cluster is declared. Kept as the default so
     * an existing deployment behaves identically after this change.
     */
    private String topicPattern = "order-.*|payment-.*|inventory-.*|refund-.*";

    private List<Cluster> clusters = new ArrayList<>();

    public String getTopicPattern() { return topicPattern; }
    public void setTopicPattern(String topicPattern) { this.topicPattern = topicPattern; }

    public List<Cluster> getClusters() { return clusters; }
    public void setClusters(List<Cluster> clusters) {
        this.clusters = clusters != null ? clusters : new ArrayList<>();
    }

    /** Whether the deployment named its clusters, or is running the single-cluster form. */
    public boolean declaresClusters() {
        return !clusters.isEmpty();
    }

    /** One Kafka cluster carrying telemetry the platform should read. */
    public static class Cluster {

        /**
         * What a source mapping refers to in its {@code cluster:} field, and what appears in
         * logs. Required: an unnamed cluster cannot be told apart from another one.
         */
        private String name;

        private String bootstrapServers;

        /** Which topics on this cluster carry execution telemetry. */
        private String topicPattern;

        /**
         * Optional. Defaults to the shared consumer group, which is what two clusters
         * normally want: the group is scoped to a cluster by the broker, so the same id on
         * two clusters is two independent groups, not one split across them.
         */
        private String groupId;

        /** Optional per-cluster listener concurrency; falls back to the shared setting. */
        private Integer concurrency;

        /** Optional; falls back to the shared {@code spring.kafka.consumer} setting. */
        private String autoOffsetReset;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

        public String getTopicPattern() { return topicPattern; }
        public void setTopicPattern(String topicPattern) { this.topicPattern = topicPattern; }

        public String getGroupId() { return groupId; }
        public void setGroupId(String groupId) { this.groupId = groupId; }

        public Integer getConcurrency() { return concurrency; }
        public void setConcurrency(Integer concurrency) { this.concurrency = concurrency; }

        public String getAutoOffsetReset() { return autoOffsetReset; }
        public void setAutoOffsetReset(String autoOffsetReset) { this.autoOffsetReset = autoOffsetReset; }

        /** What is missing before this declaration can be read, or empty when it is usable. */
        public List<String> missingFields() {
            List<String> missing = new ArrayList<>();
            if (name == null || name.isBlank()) missing.add("name");
            if (bootstrapServers == null || bootstrapServers.isBlank()) missing.add("bootstrap-servers");
            if (topicPattern == null || topicPattern.isBlank()) missing.add("topic-pattern");
            return missing;
        }
    }
}
