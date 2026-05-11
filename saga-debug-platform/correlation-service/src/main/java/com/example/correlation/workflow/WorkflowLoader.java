package com.example.correlation.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.Map;


@Component
public class WorkflowLoader {

    private static final Logger log = LoggerFactory.getLogger(WorkflowLoader.class);
    private WorkflowSpec orderSagaWorkflow;

    @PostConstruct
    public void init() {
        try {
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            ClassPathResource resource = new ClassPathResource("workflows/order-saga-workflow.yaml");
            try (InputStream is = resource.getInputStream()) {
                Map<String, Object> root = yamlMapper.readValue(is, Map.class);
                Object workflowNode = root.get("workflow");
                orderSagaWorkflow = yamlMapper.convertValue(workflowNode, WorkflowSpec.class);
                log.info("Loaded workflow spec: {} v{} with {} steps, {} transitions",
                        orderSagaWorkflow.getName(),
                        orderSagaWorkflow.getVersion(),
                        orderSagaWorkflow.getSteps() != null ? orderSagaWorkflow.getSteps().size() : 0,
                        orderSagaWorkflow.getTransitions() != null ? orderSagaWorkflow.getTransitions().size() : 0);
            }
        } catch (Exception e) {
            log.error("Failed to load workflow spec: {}", e.getMessage(), e);
            // Create a minimal fallback
            orderSagaWorkflow = new WorkflowSpec();
            orderSagaWorkflow.setName("ORDER_SAGA");
            orderSagaWorkflow.setVersion("1.0-fallback");
        }
    }

    public WorkflowSpec getOrderSagaWorkflow() {
        return orderSagaWorkflow;
    }
}
