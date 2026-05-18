package com.example.correlation.service.scenario;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class ScenarioService {

    private final RestTemplate restTemplate = new RestTemplate();
    private final String orderServiceUrl;
    private final String paymentServiceUrl;

    public ScenarioService(
            @Value("${scenario.order-service-url:http://order-service:8081}") String orderServiceUrl,
            @Value("${scenario.payment-service-url:http://payment-service:8082}") String paymentServiceUrl) {
        this.orderServiceUrl = orderServiceUrl;
        this.paymentServiceUrl = paymentServiceUrl;
    }

    public List<ScenarioDefinition> listScenarios() {
        return List.of(
                new ScenarioDefinition(
                        "SC_UI_HEALTHY",
                        "Healthy order",
                        "real",
                        "Create a normal order that should complete without anomalies.",
                        "Saga COMPLETED, no anomalies",
                        "NO_DIAGNOSIS_NEEDED",
                        3),
                new ScenarioDefinition(
                        "SC_UI_COMPENSATION",
                        "Compensation path",
                        "real",
                        "Create an order with quantity above stock so inventory fails and refund compensation runs.",
                        "Saga COMPENSATED",
                        "FORWARD_STEP_FAILURE",
                        5),
                new ScenarioDefinition(
                        "SC_UI_CIRCUIT_BREAKER",
                        "Payment circuit breaker",
                        "real",
                        "Temporarily enable payment fail-mode, create an order, then restore payment automatically.",
                        "Saga FAILED",
                        "CIRCUIT_BREAKER_OPEN",
                        12)
        );
    }

    public ScenarioRunResponse runScenario(String scenarioId) {
        String normalized = scenarioId == null ? "" : scenarioId.toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "SC_UI_HEALTHY" -> runHealthy();
            case "SC_UI_COMPENSATION" -> runCompensation();
            case "SC_UI_CIRCUIT_BREAKER" -> runCircuitBreaker();
            default -> throw new IllegalArgumentException("Unknown scenario: " + scenarioId);
        };
    }

    private ScenarioRunResponse runHealthy() {
        Map<String, Object> order = createOrder("ui-healthy", "PRODUCT-1", 1, BigDecimal.valueOf(50));
        return response(
                "SC_UI_HEALTHY",
                "Healthy order",
                order,
                "Saga COMPLETED, no anomalies",
                "NO_DIAGNOSIS_NEEDED",
                List.of(),
                List.of("Open the saga after a few seconds and run RCA. Expected rcaStatus is NO_DIAGNOSIS_NEEDED."));
    }

    private ScenarioRunResponse runCompensation() {
        Map<String, Object> order = createOrder("ui-compensation", "PRODUCT-1", 9999, BigDecimal.valueOf(999900));
        return response(
                "SC_UI_COMPENSATION",
                "Compensation path",
                order,
                "Saga COMPENSATED after refund",
                "FORWARD_STEP_FAILURE",
                List.of(),
                List.of("This tests successful compensation, not a platform crash."));
    }

    private ScenarioRunResponse runCircuitBreaker() {
        setPaymentFailMode(true);
        try {
            Map<String, Object> order = createOrder("ui-circuit-breaker", "PRODUCT-1", 1, BigDecimal.valueOf(75));
            restorePaymentFailModeLater();
            return response(
                    "SC_UI_CIRCUIT_BREAKER",
                    "Payment circuit breaker",
                    order,
                    "Saga FAILED after payment retries",
                    "CIRCUIT_BREAKER_OPEN",
                    List.of("RETRY_BURST"),
                    List.of(
                            "Payment fail-mode was enabled before creating the order.",
                            "Payment fail-mode will be restored automatically after 20 seconds.",
                            "Wait for retries and then run RCA from the saga detail page."));
        } catch (RuntimeException e) {
            setPaymentFailMode(false);
            throw e;
        }
    }

    private Map<String, Object> createOrder(String customerPrefix, String productId, int quantity, BigDecimal amount) {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("customerId", customerPrefix + "-" + Instant.now().toEpochMilli());
        request.put("productId", productId);
        request.put("quantity", quantity);
        request.put("amount", amount);

        try {
            ResponseEntity<Map> response = restTemplate.postForEntity(
                    orderServiceUrl + "/api/orders", request, Map.class);
            Map<String, Object> body = response.getBody();
            if (body == null || body.get("sagaId") == null) {
                throw new IllegalStateException("Order service response did not include sagaId");
            }
            return body;
        } catch (RestClientException e) {
            throw new IllegalStateException("Order service is unreachable at " + orderServiceUrl, e);
        }
    }

    private void setPaymentFailMode(boolean enabled) {
        try {
            restTemplate.postForEntity(
                    paymentServiceUrl + "/api/admin/fail-mode?enabled=" + enabled,
                    null,
                    Map.class);
        } catch (RestClientException e) {
            throw new IllegalStateException("Payment service admin endpoint is unreachable at " + paymentServiceUrl, e);
        }
    }

    private void restorePaymentFailModeLater() {
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(20_000);
                setPaymentFailMode(false);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException ignored) {
                // Demo helper only: the user can still restore via payment-service admin endpoint.
            }
        });
    }

    private ScenarioRunResponse response(String scenarioId,
            String name,
            Map<String, Object> order,
            String expectedOutcome,
            String expectedRca,
            List<String> expectedAnomalies,
            List<String> notes) {
        return new ScenarioRunResponse(
                scenarioId,
                name,
                String.valueOf(order.get("sagaId")),
                String.valueOf(order.get("id")),
                expectedOutcome,
                expectedRca,
                expectedAnomalies,
                notes);
    }
}
