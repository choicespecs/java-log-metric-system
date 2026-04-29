package com.metrics.gateway.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record TimeSeriesPoint(
        @JsonProperty("timestamp") Instant timestamp,
        @JsonProperty("value") double value
) {
}
