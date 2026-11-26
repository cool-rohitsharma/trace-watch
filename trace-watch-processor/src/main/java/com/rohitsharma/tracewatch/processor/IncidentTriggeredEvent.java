package com.rohitsharma.tracewatch.processor;

import java.time.Instant;

public record IncidentTriggeredEvent(
        Long incidentId,
        String incidentKey,
        String tenantId,
        String serviceName,
        int errorCountInWindow,
        String sampleTraceId,
        String sampleErrorMessage,
        Instant triggeredAt
) {}