package com.metrics.gateway.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record MetricQueryResult(
        @JsonProperty("metricName") String metricName,
        @JsonProperty("serviceId") String serviceId,
        @JsonProperty("avg") double avg,
        @JsonProperty("min") double min,
        @JsonProperty("max") double max,
        @JsonProperty("count") long count,
        @JsonProperty("windowStart") Instant windowStart,
        @JsonProperty("windowEnd") Instant windowEnd
) {
}
