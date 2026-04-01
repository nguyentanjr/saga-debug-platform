package com.example.correlation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CorrelationServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(CorrelationServiceApplication.class, args);
    }
}
