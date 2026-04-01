package com.example.payment.controller;

import com.example.payment.service.PaymentService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Admin endpoint to toggle payment failure simulation.
 * Used for circuit breaker demo scenario.
 */
@RestController
@RequestMapping("/api/admin")
public class AdminController {

    @PostMapping("/fail-mode")
    public ResponseEntity<Map<String, Object>> toggleFailMode(
            @RequestParam(value = "enabled", defaultValue = "true") boolean enabled) {
        PaymentService.setFailMode(enabled);
        return ResponseEntity.ok(Map.of(
                "failMode", enabled,
                "message", enabled
                        ? "Payment will now REJECT all requests (circuit breaker scenario)"
                        : "Payment restored to normal operation"
        ));
    }

    @GetMapping("/fail-mode")
    public ResponseEntity<Map<String, Object>> getFailMode() {
        return ResponseEntity.ok(Map.of("failMode", PaymentService.isFailMode()));
    }
}
