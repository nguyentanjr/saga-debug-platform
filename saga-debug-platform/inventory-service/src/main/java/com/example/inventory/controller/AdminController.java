package com.example.inventory.controller;

import com.example.inventory.admin.CrashSwitch;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Demo fault injection, used by demo-scenarios. Not part of the business API. */
@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private final CrashSwitch crashSwitch;

    public AdminController(CrashSwitch crashSwitch) {
        this.crashSwitch = crashSwitch;
    }

    @PostMapping("/crash-on-next-payment")
    public ResponseEntity<Map<String, Object>> armCrash() {
        crashSwitch.arm();
        return ResponseEntity.ok(Map.of(
                "armed", true,
                "message", "inventory-service will exit on the next payment-completed message "
                        + "and be restarted by its restart policy"));
    }

    @GetMapping("/crash-on-next-payment")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of("armed", crashSwitch.isArmed()));
    }
}
