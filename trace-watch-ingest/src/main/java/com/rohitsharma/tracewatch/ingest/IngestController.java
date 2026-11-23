package com.rohitsharma.tracewatch.ingest;

import com.rohitsharma.tracewatch.common.TelemetryEvent;
import com.rohitsharma.tracewatch.ingest.IngestGuardService.GuardDecision;
import jakarta.validation.Valid;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/events")
public class IngestController {

    private final IngestGuardService guardService;
    private final KafkaTemplate<String, TelemetryEvent> kafkaTemplate;
    private final String rawTopic;

    public IngestController(
            IngestGuardService guardService,
            KafkaTemplate<String, TelemetryEvent> kafkaTemplate,
            @Value("${tracewatch.kafka.raw-topic}") String rawTopic) {
        this.guardService = guardService;
        this.kafkaTemplate = kafkaTemplate;
        this.rawTopic = rawTopic;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> ingest(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody TelemetryEvent event) throws Exception {

        GuardDecision decision = guardService.evaluate(event.tenantId(), idempotencyKey);

        if (decision == GuardDecision.DUPLICATE) {
            return ResponseEntity.ok(Map.of("status", "DUPLICATE_IGNORED", "idempotencyKey", idempotencyKey));
        }
        if (decision == GuardDecision.RATE_LIMITED) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(Map.of("status", "RATE_LIMITED", "tenantId", event.tenantId()));
        }

        // Partition by tenantId so events for a single tenant stay ordered
        kafkaTemplate.send(rawTopic, event.tenantId(), event).get(3, TimeUnit.SECONDS);

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("status", "QUEUED", "idempotencyKey", idempotencyKey));
    }
}