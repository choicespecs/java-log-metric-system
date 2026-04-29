package com.metrics.collector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the metrics-collector Spring Boot service (port 8081).
 *
 * <p>This service is the ingestion boundary for the distributed metrics system.
 * It exposes two REST controllers:
 * <ul>
 *   <li>{@code MetricsController} — accepts structured JSON {@code MetricEvent} payloads
 *       and publishes them to the {@code metrics.structured} Kafka topic.</li>
 *   <li>{@code LogIngestionController} — accepts raw text log lines and publishes them
 *       as-is to the {@code logs.raw} Kafka topic.</li>
 * </ul>
 *
 * <p>No parsing or enrichment is performed here. The collector is intentionally thin:
 * validate → serialise → publish. Enrichment happens downstream in {@code metrics-processor}.
 */
@SpringBootApplication
public class MetricsCollectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MetricsCollectorApplication.class, args);
    }
}
