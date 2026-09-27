package com.example.correlation.config;

import com.example.correlation.ingestion.SourceMapping;
import com.example.correlation.ingestion.SourceMappingRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * L14 follow-up: a record the listener gave up on is logged in full so it can be replayed,
 * and that log used to carry the fields a source declared the observer must not keep — the
 * hotel commands' card numbers among them.
 */
class KafkaErrorHandlingConfigTest {

    @Test
    void aGivenUpRecordIsLoggedWithItsDeclaredFieldsRedacted() {
        SourceMapping mapping = new SourceMapping();
        mapping.setName("payments");
        mapping.setTopicPattern("payment[.]inbox[.]events");
        mapping.setRedact(List.of("creditCardNo"));
        SourceMappingRegistry registry = new SourceMappingRegistry() {
            @Override
            public SourceMapping forTopic(String topic) {
                return mapping.matches(topic) ? mapping : SourceMapping.identity();
            }
        };

        String logged = KafkaErrorHandlingConfig.valueForLog(new ConsumerRecord<>("payment.inbox.events", 0, 7L,
                "SAGA-1", "{\"type\":\"REQUEST\",\"creditCardNo\":\"4111111111111111\"}"), registry);

        assertThat(logged).doesNotContain("4111111111111111").contains("\"type\":\"REQUEST\"");
        assertThat(KafkaErrorHandlingConfig.valueForLog(
                new ConsumerRecord<>("other", 0, 1L, "k", "{\"creditCardNo\":\"x\"}"), registry))
                .isEqualTo("{\"creditCardNo\":\"x\"}");
    }
}
