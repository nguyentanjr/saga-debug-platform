package com.example.correlation.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates event schema according to business-events-spec.
 * Required fields: eventId, eventType, sagaId, timestamp
 */
@Component
public class EventSchemaValidator {

    private static final Logger log = LoggerFactory.getLogger(EventSchemaValidator.class);

    /**
     * Validate a parsed JSON event node.
     * @return list of violations (empty = valid)
     */
    public List<String> validate(JsonNode eventNode) {
        List<String> violations = new ArrayList<>();

        if (eventNode == null) {
            violations.add("Event is null");
            return violations;
        }

        checkRequired(eventNode, "eventId", violations);
        checkRequired(eventNode, "eventType", violations);
        checkRequired(eventNode, "sagaId", violations);
        checkRequired(eventNode, "timestamp", violations);

        return violations;
    }

    /**
     * Quick check — is the event valid?
     */
    public boolean isValid(JsonNode eventNode) {
        return validate(eventNode).isEmpty();
    }

    /**
     * Validate and log warnings for missing optional but recommended fields.
     */
    public void logWarnings(JsonNode eventNode, String topic) {
        if (eventNode == null) return;

        if (!eventNode.has("traceId") || eventNode.get("traceId").isNull()) {
            log.debug("Event on topic {} missing recommended field 'traceId'", topic);
        }
        if (!eventNode.has("stepName") || eventNode.get("stepName").isNull()) {
            log.debug("Event on topic {} missing recommended field 'stepName'", topic);
        }
        if (!eventNode.has("serviceName") || eventNode.get("serviceName").isNull()) {
            log.debug("Event on topic {} missing recommended field 'serviceName'", topic);
        }
    }

    private void checkRequired(JsonNode node, String fieldName, List<String> violations) {
        if (!node.has(fieldName) || node.get(fieldName).isNull() || node.get(fieldName).asText().isEmpty()) {
            violations.add("Missing required field: " + fieldName);
        }
    }
}
