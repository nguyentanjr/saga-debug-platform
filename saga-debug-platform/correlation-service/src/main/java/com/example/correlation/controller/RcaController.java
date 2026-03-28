package com.example.correlation.controller;

import com.example.correlation.domain.RcaResult;
import com.example.correlation.service.RcaService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/rca")
public class RcaController {

    private final RcaService rcaService;

    public RcaController(RcaService rcaService) {
        this.rcaService = rcaService;
    }

    @PostMapping("/analyze/{sagaId}")
    public ResponseEntity<?> analyze(@PathVariable("sagaId") String sagaId) {
        RcaResult result = rcaService.analyze(sagaId);
        if (result == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(result);
    }

    @GetMapping("/results/{sagaId}")
    public ResponseEntity<List<RcaResult>> getResults(@PathVariable("sagaId") String sagaId) {
        List<RcaResult> results = rcaService.getResults(sagaId);
        return ResponseEntity.ok(results);
    }
}
