package com.metrics.collector.controller;

import com.metrics.collector.model.MetricEvent;
import com.metrics.collector.service.MetricsPublisherService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST controller for ingesting structured metric events.
 *
 * <p>Accepts JSON-encoded {@link MetricEvent} payloads and publishes them to the
 * {@code metrics.structured} Kafka topic via {@link MetricsPublisherService}.
 * Bean Validation ({@code @Valid}) enforces that {@code metricName}, {@code serviceId},
 * and {@code timestamp} are non-null/non-blank before any Kafka I/O is attempted.
 *
 * <p>Both endpoints return {@code 202 Accepted} on success. Publishing is fire-and-forget
 * from the HTTP thread — Kafka delivery failures are logged asynchronously but do not
 * affect the HTTP response.
 */
@RestController
@RequestMapping("/api/v1/metrics")
public class MetricsController {

    private static final Logger log = LoggerFactory.getLogger(MetricsController.class);

    private final MetricsPublisherService publisherService;

    public MetricsController(MetricsPublisherService publisherService) {
        this.publisherService = publisherService;
    }

    /**
     * Ingests a single structured metric event.
     *
     * @param event the metric event to ingest; must pass {@code @Valid} constraints
     * @return {@code 202 Accepted} if the event was submitted to Kafka
     */
    @PostMapping
    public ResponseEntity<Void> ingestMetric(@Valid @RequestBody MetricEvent event) {
        log.debug("Received single metric event: metricName={} serviceId={}",
                event.getMetricName(), event.getServiceId());
        publisherService.publishMetric(event);
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }

    /**
     * Ingests a batch of structured metric events.
     *
     * <p>Each element in the list is independently validated and published. A partial
     * failure (e.g., serialisation error on one event) is logged but does not prevent
     * the remaining events from being published.
     *
     * @param events the list of metric events to ingest
     * @return {@code 202 Accepted} once all events have been submitted to Kafka
     */
    @PostMapping("/batch")
    public ResponseEntity<Void> ingestMetricsBatch(@Valid @RequestBody List<MetricEvent> events) {
        log.debug("Received batch of {} metric events", events.size());
        publisherService.publishMetrics(events);
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }
}
