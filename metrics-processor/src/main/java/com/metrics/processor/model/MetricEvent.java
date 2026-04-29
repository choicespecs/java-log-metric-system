package com.metrics.processor.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Map;

/**
 * Processor-side representation of an inbound structured metric event.
 * This mirrors the MetricEvent produced by metrics-collector and consumed
 * from the metrics.structured Kafka topic.
 */
public class MetricEvent {

    @JsonProperty("metricName")
    private String metricName;

    @JsonProperty("value")
    private double value;

    @JsonProperty("unit")
    private String unit;

    @JsonProperty("serviceId")
    private String serviceId;

    @JsonProperty("tags")
    private Map<String, String> tags;

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
                ", timestamp=" + timestamp +
                '}';
    }
}
