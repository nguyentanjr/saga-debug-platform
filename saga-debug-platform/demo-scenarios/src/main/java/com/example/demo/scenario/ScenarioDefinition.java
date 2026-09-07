package com.example.demo.scenario;

public record ScenarioDefinition(
        String id,
        String name,
        String kind,
        String description,
        String expectedOutcome,
        String expectedRca,
        int openAfterSeconds) {
}
