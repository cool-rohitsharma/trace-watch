// TelemetryStreamConsumer.java
package com.rohitsharma.tracewatch.processor;

import com.rohitsharma.tracewatch.common.TelemetryEvent;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TelemetryStreamConsumer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryStreamConsumer.class);

    private final JdbcClient jdbcClient;
    private final SlidingWindowAnomalyDetector anomalyDetector;
    private final KafkaTemplate<String, IncidentTriggeredEvent> incidentKafkaTemplate;
    private final String incidentsTopic;
    private final int errorThreshold;

    public TelemetryStreamConsumer(
            JdbcClient jdbcClient,
            SlidingWindowAnomalyDetector anomalyDetector,
            KafkaTemplate<String, IncidentTriggeredEvent> incidentKafkaTemplate,
            @Value("${tracewatch.kafka.incidents-topic:incidents.triggered}") String incidentsTopic,
            @Value("${tracewatch.anomaly.error-threshold:5}") int errorThreshold) {
        this.jdbcClient = jdbcClient;
        this.anomalyDetector = anomalyDetector;
        this.incidentKafkaTemplate = incidentKafkaTemplate;
        this.incidentsTopic = incidentsTopic;
        this.errorThreshold = errorThreshold;
    }

    @KafkaListener(
            topics = "${tracewatch.kafka.raw-topic:telemetry.raw}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional
    public void onMessage(TelemetryEvent event) {
        if (event.statusCode() == null || event.statusCode() < 100 || event.statusCode() > 599) {
            throw new IllegalArgumentException("Invalid HTTP statusCode: " + event.statusCode());
        }

        Instant ts = event.timestamp() != null ? event.timestamp() : Instant.now();

        jdbcClient.sql("""
                INSERT INTO telemetry_events
                    (tenant_id, service_name, endpoint, status_code, latency_ms, trace_id, error_message, event_timestamp)
                VALUES
                    (:tenantId, :serviceName, :endpoint, :statusCode, :latencyMs, :traceId, :errorMessage, :eventTimestamp)
                """)
                .param("tenantId", event.tenantId())
                .param("serviceName", event.serviceName())
                .param("endpoint", event.endpoint())
                .param("statusCode", event.statusCode())
                .param("latencyMs", event.latencyMs())
                .param("traceId", event.traceId())
                .param("errorMessage", event.errorMessage())
                .param("eventTimestamp", Timestamp.from(ts))
                .update();

        if (event.statusCode() >= 500) {
            boolean shouldTrigger = anomalyDetector.recordErrorAndShouldTrigger(
                    event.tenantId(), event.serviceName(), ts);

            if (shouldTrigger) {
                Instant now = Instant.now();
                String incidentKey = event.tenantId() + ":" + event.serviceName() + ":" + now.getEpochSecond();

                Long incidentId = jdbcClient.sql("""
                        INSERT INTO incidents
                            (incident_key, tenant_id, service_name, status, error_count_in_window, sample_trace_id, sample_error_message, triggered_at)
                        VALUES
                            (:incidentKey, :tenantId, :serviceName, 'OPEN', :errorCount, :traceId, :errorMessage, :triggeredAt)
                        ON CONFLICT (incident_key) DO UPDATE SET updated_at = NOW()
                        RETURNING id
                        """)
                        .param("incidentKey", incidentKey)
                        .param("tenantId", event.tenantId())
                        .param("serviceName", event.serviceName())
                        .param("errorCount", errorThreshold)
                        .param("traceId", event.traceId())
                        .param("errorMessage", event.errorMessage())
                        .param("triggeredAt", Timestamp.from(now))
                        .query(Long.class)
                        .single();

                IncidentTriggeredEvent incidentEvent = new IncidentTriggeredEvent(
                        incidentId,
                        incidentKey,
                        event.tenantId(),
                        event.serviceName(),
                        errorThreshold,
                        event.traceId(),
                        event.errorMessage(),
                        now
                );

                incidentKafkaTemplate.send(incidentsTopic, event.tenantId(), incidentEvent);
                log.warn("Triggered incident id={} key={} for service={}",
                        incidentId, incidentKey, event.serviceName());
            }
        }
    }
}