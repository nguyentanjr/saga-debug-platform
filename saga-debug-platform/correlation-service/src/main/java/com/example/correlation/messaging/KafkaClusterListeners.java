package com.example.correlation.messaging;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

/**
 * One listener container per declared Kafka cluster.
 *
 * <h2>Why the listener is built here instead of annotated</h2>
 * {@code @KafkaListener} is one listener against one set of connection properties, decided
 * at compile time. That was the last place where attaching to a new observed system could
 * still require editing Java: everything about how to read a system was already a file on
 * disk, but <em>where</em> to read it from was not. Two systems on two clusters is an
 * ordinary shape for a real estate, and it made the zero-touch claim false one layer below
 * where it was being made.
 *
 * <p>Building the containers means the count and the connection details come from
 * configuration, and adding a cluster is a restart rather than a release.
 *
 * <h2>What is shared and what is not</h2>
 * Each cluster gets its own consumer factory, because bootstrap servers, group and offset
 * policy are per-cluster facts. Everything downstream is shared: one
 * {@link SagaEventConsumer}, one ingestion path, one error handler. A second cluster must
 * not become a second implementation of how observations are read, which is the failure
 * this class is arranged to avoid.
 *
 * <p>Each record is handed on with the name of the cluster it came from. That is not
 * bookkeeping: a topic name identifies a topic only within one cluster, so once two are
 * read, matching a source mapping on the topic alone would file two systems' runs under
 * one identity. See {@link RawObservation#origin()}.
 *
 * <h2>The single-cluster deployment</h2>
 * With no cluster declared, one container is built from {@code spring.kafka} and
 * {@code correlation.kafka.topic-pattern}, and records arrive with no origin — the same
 * behaviour, and the same mapping decisions, as before any of this existed.
 */
@Component
public class KafkaClusterListeners implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(KafkaClusterListeners.class);

    private final List<ConcurrentMessageListenerContainer<String, String>> containers = new ArrayList<>();
    private final List<ConsumerGroup> groups = new ArrayList<>();
    private volatile boolean running;

    public KafkaClusterListeners(KafkaProperties kafkaProperties,
                                 KafkaSourceProperties sourceProperties,
                                 SagaEventConsumer consumer,
                                 CommonErrorHandler errorHandler) {
        for (KafkaSourceProperties.Cluster cluster : declaredClusters(kafkaProperties, sourceProperties)) {
            containers.add(containerFor(kafkaProperties, cluster, consumer, errorHandler));
            groups.add(groupFor(kafkaProperties, cluster));
        }
    }

    /**
     * The clusters to read, with the single-cluster deployment expressed as one unnamed
     * cluster so there is only one code path below this point.
     *
     * <p>A declaration missing a required field is reported and skipped rather than
     * started with something invented for it: a cluster read under a guessed pattern would
     * produce findings about a system nobody asked to observe.
     */
    private static List<KafkaSourceProperties.Cluster> declaredClusters(
            KafkaProperties kafkaProperties, KafkaSourceProperties sourceProperties) {

        if (!sourceProperties.declaresClusters()) {
            KafkaSourceProperties.Cluster only = new KafkaSourceProperties.Cluster();
            only.setBootstrapServers(String.join(",", kafkaProperties.getBootstrapServers()));
            only.setTopicPattern(sourceProperties.getTopicPattern());
            log.info("Reading one Kafka cluster at {} for topics matching {}",
                    only.getBootstrapServers(), only.getTopicPattern());
            return List.of(only);
        }

        List<KafkaSourceProperties.Cluster> usable = new ArrayList<>();
        Set<String> namesSeen = new HashSet<>();
        for (KafkaSourceProperties.Cluster cluster : sourceProperties.getClusters()) {
            List<String> missing = cluster.missingFields();
            if (!missing.isEmpty()) {
                log.error("Kafka cluster declaration is missing {} and will not be read: {}",
                        missing, cluster.getName() != null ? cluster.getName() : "(unnamed)");
                continue;
            }
            // Two clusters under one name leave every mapping scoped to it ambiguous, and
            // the ambiguity would only show up as runs filed under the wrong system.
            if (!namesSeen.add(cluster.getName())) {
                log.error("Kafka cluster '{}' is declared more than once; the second "
                        + "declaration is ignored. A mapping scoped to that name could not "
                        + "say which of them it meant.", cluster.getName());
                continue;
            }
            usable.add(cluster);
            log.info("Reading Kafka cluster '{}' at {} for topics matching {}",
                    cluster.getName(), cluster.getBootstrapServers(), cluster.getTopicPattern());
        }
        if (usable.isEmpty()) {
            log.error("Every declared Kafka cluster was unusable; this deployment reads no "
                    + "Kafka telemetry at all. HTTP ingestion is unaffected.");
        }
        return usable;
    }

    private static ConsumerGroup groupFor(KafkaProperties kafkaProperties, KafkaSourceProperties.Cluster cluster) {
        Map<String, Object> admin = kafkaProperties.buildAdminProperties(null);
        admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());
        String groupId = cluster.getGroupId() != null && !cluster.getGroupId().isBlank()
                ? cluster.getGroupId() : kafkaProperties.getConsumer().getGroupId();
        return new ConsumerGroup(admin, groupId);
    }

    private static ConcurrentMessageListenerContainer<String, String> containerFor(
            KafkaProperties kafkaProperties, KafkaSourceProperties.Cluster cluster,
            SagaEventConsumer consumer, CommonErrorHandler errorHandler) {

        Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());
        if (cluster.getGroupId() != null && !cluster.getGroupId().isBlank()) {
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, cluster.getGroupId());
        }
        if (cluster.getAutoOffsetReset() != null && !cluster.getAutoOffsetReset().isBlank()) {
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, cluster.getAutoOffsetReset());
        }

        ContainerProperties containerProperties =
                new ContainerProperties(Pattern.compile(cluster.getTopicPattern()));
        String origin = cluster.getName();
        containerProperties.setMessageListener(
                (MessageListener<String, String>) (ConsumerRecord<String, String> record) ->
                        consumer.handle(record, origin));
        containerProperties.setGroupId(
                consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG) instanceof String id
                        ? id : null);

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(
                        new DefaultKafkaConsumerFactory<>(consumerProperties), containerProperties);
        container.setConcurrency(cluster.getConcurrency() != null
                ? cluster.getConcurrency()
                : kafkaProperties.getListener().getConcurrency() != null
                        ? kafkaProperties.getListener().getConcurrency()
                        : 1);
        container.setCommonErrorHandler(errorHandler);
        container.setBeanName("saga-events-" + (origin != null ? origin : "default"));
        container.setAutoStartup(false);   // started by this class, so stop() is symmetric
        return container;
    }

    /**
     * Records published but not yet processed, summed over every partition the observer's
     * groups have committed on — how far ingestion is behind the systems it observes.
     *
     * <p>Log-end offset minus <em>committed</em> offset, read with an admin client (describe
     * only; nothing is written to the observed cluster). Not the consumer's own
     * {@code records-lag} metric: that is measured against the fetch position, which moves
     * as soon as records are handed to the listener. Measured live during a store outage,
     * a partition held at offset 29 with 20 records behind it reported {@code records-lag}
     * 0 — the one situation this number exists to reveal.
     *
     * @return null when no cluster could be measured, because "not measured" is not
     *         "caught up"
     */
    public Long backlog() {
        Long total = null;
        for (ConsumerGroup group : groups) {
            Long lag = group.backlog();
            if (lag != null) total = (total == null ? 0 : total) + lag;
        }
        return total;
    }

    /** Sum of end minus committed over the partitions that have both; a partition never committed on is not counted. */
    static Long backlogOf(Map<TopicPartition, Long> committed, Map<TopicPartition, Long> ends) {
        if (committed.isEmpty()) return null;
        long total = 0;
        for (Map.Entry<TopicPartition, Long> entry : committed.entrySet()) {
            Long end = ends.get(entry.getKey());
            if (end != null) total += Math.max(0, end - entry.getValue());
        }
        return total;
    }

    /** One cluster's consumer group, measured on demand with a lazily created admin client. */
    private static final class ConsumerGroup {
        private final Map<String, Object> adminProperties;
        private final String groupId;
        private Admin admin;

        ConsumerGroup(Map<String, Object> adminProperties, String groupId) {
            this.adminProperties = adminProperties;
            this.groupId = groupId;
        }

        synchronized Long backlog() {
            if (groupId == null) return null;
            try {
                if (admin == null) admin = Admin.create(adminProperties);
                Map<TopicPartition, Long> committed = new HashMap<>();
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                        .get(5, TimeUnit.SECONDS)
                        .forEach((tp, om) -> { if (om != null) committed.put(tp, om.offset()); });
                if (committed.isEmpty()) return null;
                Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
                committed.keySet().forEach(tp -> latest.put(tp, OffsetSpec.latest()));
                Map<TopicPartition, Long> ends = new HashMap<>();
                admin.listOffsets(latest).all().get(5, TimeUnit.SECONDS)
                        .forEach((tp, info) -> ends.put(tp, info.offset()));
                return backlogOf(committed, ends);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            } catch (Exception e) {
                log.debug("Could not measure backlog of group {}: {}", groupId, e.toString());
                return null;
            }
        }

        synchronized void close() {
            if (admin != null) admin.close(Duration.ofSeconds(5));
            admin = null;
        }
    }

    /** How many containers are live; the count a deployment can check against what it declared. */
    public int containerCount() {
        return containers.size();
    }

    @Override
    public void start() {
        containers.forEach(ConcurrentMessageListenerContainer::start);
        running = true;
    }

    @Override
    public void stop() {
        containers.forEach(ConcurrentMessageListenerContainer::stop);
        groups.forEach(ConsumerGroup::close);
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
