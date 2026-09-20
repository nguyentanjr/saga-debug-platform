package com.example.correlation.messaging;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

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
    private volatile boolean running;

    public KafkaClusterListeners(KafkaProperties kafkaProperties,
                                 KafkaSourceProperties sourceProperties,
                                 SagaEventConsumer consumer,
                                 CommonErrorHandler errorHandler) {
        for (KafkaSourceProperties.Cluster cluster : declaredClusters(kafkaProperties, sourceProperties)) {
            containers.add(containerFor(kafkaProperties, cluster, consumer, errorHandler));
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
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
