package com.rohitsharma.tracewatch.processor;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public DefaultErrorHandler dlqErrorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate,
            @Value("${tracewatch.kafka.dlq-topic:telemetry.dlq}") String dlqTopic) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    log.warn("Routing failed record from topic={} offset={} to DLQ={}: {}",
                            record.topic(), record.offset(), dlqTopic, ex.getMessage());
                    return new TopicPartition(dlqTopic, record.partition() % 2);
                }
        );

        // Retry twice with 500ms spacing before sending poison-pill or persistent failure to DLQ
        FixedBackOff backOff = new FixedBackOff(500L, 2L);
        return new DefaultErrorHandler(recoverer, backOff);
    }
}