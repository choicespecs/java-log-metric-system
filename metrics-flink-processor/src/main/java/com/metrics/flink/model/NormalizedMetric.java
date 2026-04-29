package com.metrics.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Map;

/**
 * Flink-side representation of a normalized metric. Must implement Serializable
 * and have a no-arg constructor with full getters/setters for Flink's serialization.
 * Mirrors the NormalizedMetric produced by metrics-processor.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NormalizedMetric implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("metricName")
    private String metricName;

    @JsonProperty("value")
    private double value;

    @JsonProperty("unit")
    private String unit;

    @JsonProperty("serviceId")
    private String serviceId;

    @JsonProperty("serviceName")
    private String serviceName;

    @JsonProperty("team")
    private String team;

    @JsonProperty("environment")
    private String environment;

    @JsonProperty("region")
    private String region;

    @JsonProperty("tags")
    private Map<String, String> tags;

    @JsonProperty("parsedLevel")
    private String parsedLevel;

    @JsonProperty("logMessage")
    private String logMessage;

    @JsonProperty("timestampMs")
    private long timestampMs;

    public NormalizedMetric() {
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

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getTeam() {
        return team;
    }

    public void setTeam(String team) {
        this.team = team;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public String getParsedLevel() {
        return parsedLevel;
    }

    public void setParsedLevel(String parsedLevel) {
        this.parsedLevel = parsedLevel;
    }

    public String getLogMessage() {
        return logMessage;
    }

    public void setLogMessage(String logMessage) {
        this.logMessage = logMessage;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public void setTimestampMs(long timestampMs) {
        this.timestampMs = timestampMs;
    }

    @Override
    public String toString() {
        return "NormalizedMetric{" +
                "metricName='" + metricName + '\'' +
                ", value=" + value +
                ", serviceId='" + serviceId + '\'' +
                ", environment='" + environment + '\'' +
                ", timestampMs=" + timestampMs +
                '}';
    }
}
