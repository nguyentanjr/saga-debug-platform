package com.example.correlation.liveness;

import com.example.correlation.domain.ServiceLifecycleEvent;
import com.example.correlation.repository.ServiceLifecycleEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

/**
 * Follows the Docker Engine event stream and records when the observed system's services
 * exit and start.
 *
 * <p>Read-only towards the observed system: it asks the runtime what happened and changes
 * nothing, and no service has to emit anything new. Off unless
 * {@code correlation.liveness.docker.enabled} is set.
 *
 * <h2>Observer boundary (ADR 0001)</h2>
 * The engine API can stop, start and delete containers, and a socket mounted read-only still
 * serves all of it. So the intended deployment does not give this service the socket: it
 * talks to a socket proxy that forwards {@code GET /events} and refuses everything else
 * ({@code tcp://...} endpoint). The observer then cannot act on the observed system even if
 * this code were wrong. A {@code unix://} endpoint is accepted for local development only.
 *
 * <p>The request is plain HTTP/1.0 so the engine streams the response as newline-separated
 * JSON until the connection closes; the parser only reads lines that look like JSON objects,
 * so a chunked response would parse too. On reconnect it asks for events since the last one
 * recorded, and duplicates are dropped by {@code (source, external_id)}.
 */
@Component
@ConditionalOnProperty(prefix = "correlation.liveness.docker", name = "enabled", havingValue = "true")
public class DockerLifecycleCollector {

    private static final Logger log = LoggerFactory.getLogger(DockerLifecycleCollector.class);
    private static final String FILTERS =
            "{\"type\":[\"container\"],\"event\":[\"die\",\"start\"]}";

    private final ServiceLifecycleEventRepository repository;
    private final DockerEventParser parser;
    private final String endpoint;
    private final Duration initialLookback;

    public DockerLifecycleCollector(ServiceLifecycleEventRepository repository,
            ObjectMapper objectMapper,
            @Value("${correlation.liveness.docker.endpoint:tcp://docker-events-proxy:2375}") String endpoint,
            @Value("${correlation.liveness.docker.service-label:com.docker.compose.service}") String serviceLabel,
            @Value("${correlation.liveness.docker.initial-lookback:PT10M}") Duration initialLookback) {
        this.repository = repository;
        this.parser = new DockerEventParser(objectMapper, serviceLabel);
        this.endpoint = endpoint;
        this.initialLookback = initialLookback;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        Thread thread = new Thread(this::run, "docker-lifecycle-collector");
        thread.setDaemon(true);
        thread.start();
        log.info("Following Docker container lifecycle on {}", endpoint);
    }

    private void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                follow();
            } catch (Exception e) {
                log.warn("Docker event stream ended ({}); reconnecting in 5s", e.getMessage());
            }
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void follow() throws Exception {
        long since = repository.findTopBySourceOrderByOccurredAtDesc(DockerEventParser.SOURCE)
                .map(e -> e.getOccurredAt().getEpochSecond())
                .orElse(Instant.now().minus(initialLookback).getEpochSecond());
        String path = "/events?since=" + since + "&filters="
                + URLEncoder.encode(FILTERS, StandardCharsets.UTF_8);

        try (Connection connection = open()) {
            String request = "GET " + path + " HTTP/1.0\r\nHost: docker\r\n\r\n";
            connection.out().write(request.getBytes(StandardCharsets.US_ASCII));
            connection.out().flush();
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    connection.in(), StandardCharsets.UTF_8));

            String status = reader.readLine();
            if (status == null || !status.contains(" 200 ")) {
                throw new IllegalStateException("engine answered " + status);
            }
            String line;
            while ((line = reader.readLine()) != null) {
                parser.parse(line).ifPresent(this::record);
            }
        }
    }

    private record Connection(Closeable handle, InputStream in, OutputStream out) implements AutoCloseable {
        @Override public void close() throws IOException { handle.close(); }
    }

    private Connection open() throws IOException {
        URI uri = URI.create(endpoint);
        if ("tcp".equals(uri.getScheme())) {
            Socket socket = new Socket(uri.getHost(), uri.getPort());
            return new Connection(socket, socket.getInputStream(), socket.getOutputStream());
        }
        if ("unix".equals(uri.getScheme())) {
            SocketChannel channel = SocketChannel.open(UnixDomainSocketAddress.of(uri.getPath()));
            return new Connection(channel, Channels.newInputStream(channel), Channels.newOutputStream(channel));
        }
        throw new IllegalStateException("unsupported endpoint " + endpoint);
    }

    void record(ServiceLifecycleEvent event) {
        if (repository.existsBySourceAndExternalId(event.getSource(), event.getExternalId())) return;
        try {
            repository.save(event);
            log.info("Service {} {} at {} ({})", event.getServiceName(), event.getState(),
                    event.getOccurredAt(), event.getDetail());
        } catch (DataIntegrityViolationException duplicate) {
            // Recorded concurrently by a replay; the unique key already holds it.
        }
    }
}
