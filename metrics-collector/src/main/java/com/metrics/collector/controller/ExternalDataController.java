package com.metrics.collector.controller;

import com.metrics.collector.service.ExternalDataPublisherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Accepts raw JSON payloads from external APIs whose schemas are unknown or may
 * change at any time. No validation is performed — the payload is forwarded
 * verbatim to the {@code external.data.raw} Kafka topic.
 *
 * <p>Flink Pipeline 4 reads the topic and writes NDJSON files to HDFS partitioned
 * by source API and hour. The {@code ExternalDataProcessor} Spark job then reads
 * those files with schema inference and writes structured Parquet output.
 *
 * <p>The {@code X-Source-API} header identifies the originating API (e.g.
 * {@code payments-api}, {@code crm-api}). It becomes the Kafka partition key and
 * the HDFS partition prefix, enabling per-source Parquet partitioning downstream.
 */
@RestController
@RequestMapping("/api/v1/external")
public class ExternalDataController {

    private static final Logger log = LoggerFactory.getLogger(ExternalDataController.class);

    private final ExternalDataPublisherService publisherService;

    public ExternalDataController(ExternalDataPublisherService publisherService) {
        this.publisherService = publisherService;
    }

    /**
     * Ingest a single JSON payload from an external API.
     *
     * <p>Example:
     * <pre>
     * POST /api/v1/external/ingest
     * X-Source-API: payments-api
     * Content-Type: application/json
     *
     * {"transactionId":"txn-999","amount":42.50,"currency":"USD","userId":"u-123"}
     * </pre>
     */
    @PostMapping(value = "/ingest", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> ingest(
            @RequestHeader(value = "X-Source-API", required = false, defaultValue = "unknown") String sourceApi,
            @RequestBody String rawJson) {

        log.info("POST /api/v1/external/ingest source={} payloadBytes={}", sourceApi, rawJson.length());
        publisherService.publish(sourceApi, rawJson);
        return ResponseEntity.accepted().build();
    }

    /**
     * Ingest a batch of independent JSON payloads (one per element in a JSON array).
     * Each element is published as a separate Kafka message so Flink can process
     * them independently.
     *
     * <p>Example:
     * <pre>
     * POST /api/v1/external/ingest/batch
     * X-Source-API: crm-api
     * Content-Type: application/json
     *
     * [{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]
     * </pre>
     */
    @PostMapping(value = "/ingest/batch", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> ingestBatch(
            @RequestHeader(value = "X-Source-API", required = false, defaultValue = "unknown") String sourceApi,
            @RequestBody java.util.List<com.fasterxml.jackson.databind.JsonNode> records) {

        log.info("POST /api/v1/external/ingest/batch source={} count={}", sourceApi, records.size());
        for (com.fasterxml.jackson.databind.JsonNode node : records) {
            publisherService.publish(sourceApi, node.toString());
        }
        return ResponseEntity.accepted().build();
    }
}
