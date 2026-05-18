package com.example.correlation.service.alert;

public interface AlertSink {
    void emit(AlertEvent alert);
}
