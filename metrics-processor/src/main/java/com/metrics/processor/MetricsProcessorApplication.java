package com.metrics.processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the metrics-processor Spring Boot service (port 8082).
 *
 * <p>This service consumes from two Kafka topics and enriches events with service metadata
 * from PostgreSQL before re-publishing to {@code metrics.normalized}:
 * <ul>
 *   <li>{@code StructuredMetricsConsumer} (group {@code metrics-processor}) — consumes
 *       {@code MetricEvent} JSON from {@code metrics.structured}, enriches via
 *       {@code EnrichmentService}, and publishes {@code NormalizedMetric} to
 *       {@code metrics.normalized}.</li>
 *   <li>{@code RawLogConsumer} (group {@code log-processor}) — consumes raw log lines from
 *       {@code logs.raw}, attempts regex parsing via {@code LogParser}, enriches matching
 *       lines, and publishes to {@code metrics.normalized}. Non-matching lines are dropped.</li>
 * </ul>
 *
 * <p>Service metadata is resolved from the {@code service_metadata} PostgreSQL table
 * (managed by Flyway) and cached in Caffeine (500 entries, 10-minute TTL) to avoid
 * repeated database round-trips under high throughput.
 *
 * <p>Kafka consumer concurrency is set to 3 via {@code ConcurrentKafkaListenerContainerFactory}.
 */
@SpringBootApplication
public class MetricsProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MetricsProcessorApplication.class, args);
    }
}
