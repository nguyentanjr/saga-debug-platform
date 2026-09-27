package com.example.correlation.controller;

import com.example.correlation.domain.EventTimeSource;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.service.CorrelationService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class AdminEventTimeSourceTest {
    @Test
    void explicitlySuppliedTimeIsDeclared() {
        assertThat(injected(Map.of("eventType", "ORDER_CREATED", "timestamp", "2026-01-01T00:00:00Z"))
                .eventTimeSource()).isEqualTo(EventTimeSource.DECLARED);
    }

    @Test
    void defaultTimeIsTransportFallback() {
        assertThat(injected(Map.of("eventType", "ORDER_CREATED")).eventTimeSource())
                .isEqualTo(EventTimeSource.TRANSPORT);
    }

    private IngestedEvent injected(Map<String, String> body) {
        CorrelationService correlation = mock(CorrelationService.class);
        new AdminEventController(correlation, null, null).injectEvent(body);
        ArgumentCaptor<IngestedEvent> captor = ArgumentCaptor.forClass(IngestedEvent.class);
        verify(correlation).processEvent(captor.capture());
        return captor.getValue();
    }
}
