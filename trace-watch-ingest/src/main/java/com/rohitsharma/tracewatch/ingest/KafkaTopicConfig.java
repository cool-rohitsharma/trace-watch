package com.rohitsharma.tracewatch.ingest;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic rawTelemetryTopic(@Value("${tracewatch.kafka.raw-topic}") String topicName) {
        return TopicBuilder.name(topicName).partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic dlqTopic() {
        return TopicBuilder.name("telemetry.dlq").partitions(2).replicas(1).build();
    }

    @Bean
    public NewTopic incidentsTopic() {
        return TopicBuilder.name("incidents.triggered").partitions(3).replicas(1).build();
    }
}