package com.metrics.collector.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Map;

/**
 * Represents a single structured metric event submitted by an external client.
 *
 * <p>This is the ingestion payload accepted by {@code POST /api/v1/metrics}. After
 * Bean Validation passes, the event is serialised to JSON and published to the
 * {@code metrics.structured} Kafka topic keyed by {@link #serviceId}.
 *
 * <p>Downstream, {@code metrics-processor} deserialises this class, enriches it with
 * service metadata from PostgreSQL, and produces a {@code NormalizedMetric} to
 * {@code metrics.normalized}.
 *
 * <p>Constraints:
 * <ul>
 *   <li>{@code metricName} — must not be blank (e.g. {@code "cpu.usage"})</li>
 *   <li>{@code serviceId} — must not be blank (e.g. {@code "svc-api"}); used as Kafka key</li>
 *   <li>{@code timestamp} — must not be null; should be the event time, not receipt time</li>
 * </ul>
 */
public class MetricEvent {

    @NotBlank(message = "metricName must not be blank")
    @JsonProperty("metricName")
    private String metricName;

    @JsonProperty("value")
    private double value;

    @JsonProperty("unit")
    private String unit;

    @NotBlank(message = "serviceId must not be blank")
    @JsonProperty("serviceId")
    private String serviceId;

    @JsonProperty("tags")
    private Map<String, String> tags;

    @NotNull(message = "timestamp must not be null")
    @JsonProperty("timestamp")
    private Instant timestamp;

    public MetricEvent() {
    }

    @JsonCreator
    public MetricEvent(
            @JsonProperty("metricName") String metricName,
            @JsonProperty("value") double value,
            @JsonProperty("unit") String unit,
            @JsonProperty("serviceId") String serviceId,
            @JsonProperty("tags") Map<String, String> tags,
            @JsonProperty("timestamp") Instant timestamp) {
        this.metricName = metricName;
        this.value = value;
        this.unit = unit;
        this.serviceId = serviceId;
        this.tags = tags;
        this.timestamp = timestamp;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "MetricEvent{" +
                "metricName='" + metricName + '\'' +
                ", value=" + value +
                ", unit='" + unit + '\'' +
                ", serviceId='" + serviceId + '\'' +
                ", tags=" + tags +
                ", timestamp=" + timestamp +
                '}';
    }
}
