package com.example.demo.scenario;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Drives the demo services so the dashboard can produce a saga run on demand.
 *
 * <h2>Why this is a service of its own</h2>
 * It used to live inside correlation-service, which made the observer capable of creating
 * orders on the order service and of switching the payment service into failure mode.
 * That is the observer issuing business commands to the systems it observes — the one
 * thing {@code docs/ADR/0001-observer-boundary.md} forbids. A profile flag would have
 * hidden it rather than removed it: the class would still have shipped in the observer's
 * jar, and any deployment of the observer would still have carried the ability to write to
 * the systems it is supposed to only watch.
 *
 * <p>The demo capability itself is worth keeping — a dashboard button that produces a real
 * compensating saga is how the platform is shown to work — so it moved rather than being
 * deleted. It depends on nothing from correlation-service, and correlation-service depends
 * on nothing here. Neither module can drift into the other.
 *
 * <p>Nothing outside a demo needs to run this. The observer works with it absent.
 */
@SpringBootApplication
public class DemoScenariosApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoScenariosApplication.class, args);
    }

    /**
     * The dashboard calls this service directly, on a different origin from the observer.
     *
     * <p>Unauthenticated, like the rest of the demo stack. This service can create orders
     * and disable payments, so it belongs nowhere near a real deployment — which is the
     * other reason it is not part of the observer.
     */
    @Bean
    WebMvcConfigurer demoCors(@Value("${demo.allowed-origin:http://localhost:4200}") String origin) {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/api/scenarios/**")
                        .allowedOrigins(origin)
                        .allowedMethods("GET", "POST");
            }
        };
    }
}
