package com.rohitsharma.tracewatch.ingest;

import java.time.Instant;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

@Service
public class IngestGuardService {

    public enum GuardDecision { ALLOWED, DUPLICATE, RATE_LIMITED }

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<String> guardScript;
    private final int rateLimitPerSecond;
    private final int idempotencyTtlSeconds;

    public IngestGuardService(
            StringRedisTemplate redisTemplate,
            @Value("${tracewatch.ingest.rate-limit-per-second:500}") int rateLimitPerSecond,
            @Value("${tracewatch.ingest.idempotency-ttl-seconds:600}") int idempotencyTtlSeconds) {
        this.redisTemplate = redisTemplate;
        this.rateLimitPerSecond = rateLimitPerSecond;
        this.idempotencyTtlSeconds = idempotencyTtlSeconds;
        this.guardScript = new DefaultRedisScript<>();
        this.guardScript.setLocation(new ClassPathResource("scripts/ingest_guard.lua"));
        this.guardScript.setResultType(String.class);
    }

    public GuardDecision evaluate(String tenantId, String idempotencyKey) {
        long currentEpochSecond = Instant.now().getEpochSecond();
        List<String> keys = List.of(
                "idem:" + tenantId + ":" + idempotencyKey,
                "rl:" + tenantId + ":" + currentEpochSecond
        );
        String result = redisTemplate.execute(
                guardScript,
                keys,
                String.valueOf(idempotencyTtlSeconds),
                String.valueOf(rateLimitPerSecond)
        );
        return GuardDecision.valueOf(result);
    }
}