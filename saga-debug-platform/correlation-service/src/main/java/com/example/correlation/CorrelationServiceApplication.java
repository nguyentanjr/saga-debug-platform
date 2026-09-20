package com.example.correlation;

import com.example.correlation.messaging.KafkaSourceProperties;
import com.example.correlation.service.DetectionProperties;
import com.example.correlation.service.RetentionProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({DetectionProperties.class, RetentionProperties.class,
        KafkaSourceProperties.class})
public class CorrelationServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(CorrelationServiceApplication.class, args);
    }
}
