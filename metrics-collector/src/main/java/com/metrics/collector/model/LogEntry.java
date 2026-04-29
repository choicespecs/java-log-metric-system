package com.metrics.collector.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Immutable record representing a received raw log line with its source and receipt time.
 *
 * <p>This model is used internally for structured representation of log data at the
 * collector boundary. The actual value published to {@code logs.raw} Kafka topic is
 * the plain {@code rawLine} string — this record is not serialised to Kafka directly.
 *
 * @param rawLine    the original unmodified log line as received
 * @param source     the source identifier (e.g. {@code "http-ingestion"} or a client-supplied tag)
 * @param receivedAt the wall-clock instant at which the collector received the line
 */
public record LogEntry(
        @JsonProperty("rawLine") String rawLine,
        @JsonProperty("source") String source,
        @JsonProperty("receivedAt") Instant receivedAt
) {

    @JsonCreator
    public LogEntry(
            @JsonProperty("rawLine") String rawLine,
            @JsonProperty("source") String source,
            @JsonProperty("receivedAt") Instant receivedAt) {
        this.rawLine = rawLine;
        this.source = source;
        this.receivedAt = receivedAt;
    }
}
