package com.metrics.processor.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Map;

public class NormalizedMetric {

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

    @JsonProperty("timestamp")
    private Instant timestamp;

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

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
        if (timestamp != null) {
            this.timestampMs = timestamp.toEpochMilli();
        }
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
                ", serviceName='" + serviceName + '\'' +
                ", environment='" + environment + '\'' +
                ", parsedLevel='" + parsedLevel + '\'' +
                '}';
    }
}
