package com.rohitsharma.tracewatch.common;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Map;

public record TelemetryEvent(
        @NotBlank String tenantId,
        @NotBlank String serviceName,
        @NotBlank String endpoint,
        @NotNull Integer statusCode,
        @Min(0) long latencyMs,
        String traceId,
        String errorMessage,
        Instant timestamp,
        Map<String, String> tags
) {
    public TelemetryEvent {
        if (timestamp == null) {
            timestamp = Instant.now();
        }
    }
}