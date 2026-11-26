// SlidingWindowAnomalyDetector.java
package com.rohitsharma.tracewatch.processor;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class SlidingWindowAnomalyDetector {

    private final StringRedisTemplate redisTemplate;
    private final long windowMillis;
    private final int errorThreshold;
    private final Duration cooldownDuration;

    public SlidingWindowAnomalyDetector(
            StringRedisTemplate redisTemplate,
            @Value("${tracewatch.anomaly.window-seconds:60}") long windowSeconds,
            @Value("${tracewatch.anomaly.error-threshold:5}") int errorThreshold,
            @Value("${tracewatch.anomaly.cooldown-seconds:120}") long cooldownSeconds) {
        this.redisTemplate = redisTemplate;
        this.windowMillis = windowSeconds * 1000L;
        this.errorThreshold = errorThreshold;
        this.cooldownDuration = Duration.ofSeconds(cooldownSeconds);
    }

    public boolean recordErrorAndShouldTrigger(String tenantId, String serviceName, Instant eventTime) {
        long nowMs = (eventTime != null ? eventTime : Instant.now()).toEpochMilli();
        long windowStartMs = nowMs - windowMillis;

        String windowKey = "err_win:" + tenantId + ":" + serviceName;
        String cooldownKey = "inc_cd:" + tenantId + ":" + serviceName;

        // Purge expired entries outside the rolling window
        redisTemplate.opsForZSet().removeRangeByScore(windowKey, Double.NEGATIVE_INFINITY, windowStartMs);

        // Add current error event with unique member so concurrent errors in same ms don't overwrite
        String member = nowMs + ":" + UUID.randomUUID();
        redisTemplate.opsForZSet().add(windowKey, member, nowMs);
        redisTemplate.expire(windowKey, Duration.ofMillis(windowMillis + 10_000L));

        Long count = redisTemplate.opsForZSet().zCard(windowKey);
        if (count == null || count < errorThreshold) {
            return false;
        }

        // Atomic cooldown lock: true only if no active cooldown exists for this service
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(cooldownKey, "1", cooldownDuration);
        return Boolean.TRUE.equals(acquired);
    }
}