package com.example.correlation.messaging;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;

/** L2: how far ingestion is behind the observed systems, and when that is reported. */
class IngestionBacklogTest {

    @Test
    void theBacklogIsThePerPartitionLagSummedAcrossClients() {
        Map<MetricName, Metric> first = new LinkedHashMap<>();
        put(first, "records-lag", Map.of("topic", "order-created", "partition", "0"), 120);
        put(first, "records-lag", Map.of("topic", "order-created", "partition", "1"), 30);
        // Windowed maximum over the client, not a backlog; counting it would double up.
        put(first, "records-lag-max", Map.of("client-id", "c1"), 120);
        Map<MetricName, Metric> second = new LinkedHashMap<>();
        put(second, "records-lag", Map.of("topic", "payment-completed", "partition", "0"), 5);
        // A partition assigned but never fetched reports NaN: not measured, not zero.
        put(second, "records-lag", Map.of("topic", "payment-completed", "partition", "1"), Double.NaN);

        assertThat(KafkaClusterListeners.backlogOf(Map.of("c1", first, "c2", second))).isEqualTo(155L);
    }

    @Test
    void nothingFetchedYetIsNotReportedAsCaughtUp() {
        assertThat(KafkaClusterListeners.backlogOf(Map.of())).isNull();
        assertThat(new IngestionBacklogHealth(() -> null, 100).health().getStatus()).isEqualTo(Status.UNKNOWN);
    }

    @Test
    void pastTheThresholdTheComponentSaysLaggingWithoutGoingDown() {
        assertThat(new IngestionBacklogHealth(() -> 100L, 100).health().getStatus()).isEqualTo(Status.UP);
        var lagging = new IngestionBacklogHealth(() -> 101L, 100).health();
        assertThat(lagging.getStatus()).isEqualTo(IngestionBacklogHealth.LAGGING);
        assertThat(lagging.getDetails()).containsEntry("backlog", 101L).containsEntry("threshold", 100L);
    }

    private static void put(Map<MetricName, Metric> metrics, String name, Map<String, String> tags, double value) {
        MetricName metricName = new MetricName(name, "consumer-fetch-manager-metrics", "", tags);
        metrics.put(metricName, new Metric() {
            @Override public MetricName metricName() { return metricName; }
            @Override public Object metricValue() { return value; }
        });
    }
}
