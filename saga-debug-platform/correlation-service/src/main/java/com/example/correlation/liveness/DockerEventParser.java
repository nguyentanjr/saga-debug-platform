package com.example.correlation.liveness;

import com.example.correlation.domain.ServiceLifecycleEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Optional;

/**
 * Reads one line of the Docker Engine {@code /events} stream into a lifecycle fact.
 *
 * <p>Only two actions are kept, and both are statements about the process itself:
 * <ul>
 *   <li>{@code die} — the container's main process exited, for whatever reason: a crash, an
 *       OOM kill, {@code docker stop}. That is the service being down.</li>
 *   <li>{@code start} — it was started again.</li>
 * </ul>
 *
 * <p>Everything else is ignored on purpose, because each would claim more than happened:
 * {@code pause} and {@code kill} with SIGSTOP freeze a process that is still there and
 * resumes where it was; {@code kill} alone is a signal sent, not an exit (a SIGTERM that is
 * honoured is followed by its own {@code die}); a network disconnect leaves the service
 * running. Treating those as "down" would turn a stall or a partition into a crash — the
 * very confusion this evidence is meant to remove.
 */
public final class DockerEventParser {

    public static final String SOURCE = "docker";
    /** Compose's project label: which deployment the container belongs to (V21). */
    static final String PROJECT_LABEL = "com.docker.compose.project";

    private final ObjectMapper objectMapper;
    private final String serviceLabel;

    /**
     * @param serviceLabel the container label naming the service as the workflow does;
     *                     Compose sets {@code com.docker.compose.service}. Falls back to the
     *                     container name when the label is absent.
     */
    public DockerEventParser(ObjectMapper objectMapper, String serviceLabel) {
        this.objectMapper = objectMapper;
        this.serviceLabel = serviceLabel;
    }

    public Optional<ServiceLifecycleEvent> parse(String line) {
        if (line == null || !line.startsWith("{")) return Optional.empty();
        JsonNode node;
        try {
            node = objectMapper.readTree(line);
        } catch (Exception e) {
            return Optional.empty();
        }
        if (!"container".equals(node.path("Type").asText())) return Optional.empty();

        String action = node.path("Action").asText();
        ServiceLifecycleEvent.State state = switch (action) {
            case "die" -> ServiceLifecycleEvent.State.DOWN;
            case "start" -> ServiceLifecycleEvent.State.UP;
            default -> null;
        };
        if (state == null) return Optional.empty();

        JsonNode attributes = node.path("Actor").path("Attributes");
        String service = attributes.path(serviceLabel).asText("");
        if (service.isBlank()) service = attributes.path("name").asText("");
        if (service.isBlank()) return Optional.empty();

        long timeNano = node.path("timeNano").asLong(0);
        if (timeNano <= 0) return Optional.empty();
        Instant occurredAt = Instant.ofEpochSecond(0, timeNano);

        String detail = action;
        String exitCode = attributes.path("exitCode").asText("");
        if (!exitCode.isBlank()) detail += " exitCode=" + exitCode;

        String containerId = node.path("Actor").path("ID").asText(node.path("id").asText(""));
        String externalId = containerId + ":" + action + ":" + timeNano;
        String scope = attributes.path(PROJECT_LABEL).asText("");
        return Optional.of(new ServiceLifecycleEvent(service, state, occurredAt, SOURCE, detail, externalId)
                .inScope(scope));
    }
}
