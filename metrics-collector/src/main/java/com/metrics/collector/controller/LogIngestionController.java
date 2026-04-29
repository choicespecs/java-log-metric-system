package com.metrics.collector.controller;

import com.metrics.collector.service.MetricsPublisherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST controller for ingesting raw unstructured log lines.
 *
 * <p>Accepts plain-text or JSON-encoded string payloads and publishes them verbatim
 * to the {@code logs.raw} Kafka topic via {@link MetricsPublisherService}. No parsing
 * or validation is performed beyond trimming whitespace from single-line submissions.
 *
 * <p>The optional {@code source} query parameter becomes the Kafka message key, enabling
 * partition affinity so that all log lines from the same producer land on the same
 * Kafka partition. Defaults to {@code "http-ingestion"} when omitted.
 *
 * <p>Both endpoints return {@code 202 Accepted}. Kafka delivery failures are logged
 * asynchronously via the {@code CompletableFuture} callback.
 */
@RestController
@RequestMapping("/api/v1/logs")
public class LogIngestionController {

    private static final Logger log = LoggerFactory.getLogger(LogIngestionController.class);

    private static final String DEFAULT_SOURCE = "http-ingestion";

    private final MetricsPublisherService publisherService;

    public LogIngestionController(MetricsPublisherService publisherService) {
        this.publisherService = publisherService;
    }

    /**
     * Ingests a single raw log line.
     *
     * @param rawLog the log line to ingest (trimmed before publishing)
     * @param source the Kafka message key / source identifier; defaults to {@code "http-ingestion"}
     * @return {@code 202 Accepted}
     */
    @PostMapping(consumes = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<Void> ingestLog(
            @RequestBody String rawLog,
            @RequestParam(value = "source", defaultValue = DEFAULT_SOURCE) String source) {
        log.debug("Received raw log line from source={}", source);
        publisherService.publishLog(rawLog.trim(), source);
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }

    /**
     * Ingests a batch of raw log lines.
     *
     * <p>Each string in the list is published as a separate Kafka record keyed by {@code source}.
     *
     * @param rawLogs list of raw log lines to ingest
     * @param source  the Kafka message key / source identifier; defaults to {@code "http-ingestion"}
     * @return {@code 202 Accepted}
     */
    @PostMapping(value = "/batch", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> ingestLogsBatch(
            @RequestBody List<String> rawLogs,
            @RequestParam(value = "source", defaultValue = DEFAULT_SOURCE) String source) {
        log.debug("Received batch of {} log lines from source={}", rawLogs.size(), source);
        publisherService.publishLogs(rawLogs, source);
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }
}
