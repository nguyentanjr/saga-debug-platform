package com.example.inventory.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic paymentCompletedTopic() {
        return TopicBuilder.name("payment-completed")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic inventoryReservedTopic() {
        return TopicBuilder.name("inventory-reserved")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic inventoryFailedTopic() {
        return TopicBuilder.name("inventory-failed")
                .partitions(3)
                .replicas(1)
                .build();
    }
}

