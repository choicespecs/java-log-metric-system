package com.metrics.flink.model;

import java.io.Serializable;

/**
 * Represents the windowed aggregation result for a (metricName, serviceId) key.
 * Written to TimescaleDB and Parquet files.
 */
public class AggregatedMetric implements Serializable {

    private static final long serialVersionUID = 1L;

    private String metricName;
    private String serviceId;
    private String serviceName;
    private String environment;
    private double sum;
    private double min;
    private double max;
    private long count;
    private double avg;
    private long windowStartMs;
    private long windowEndMs;

    public AggregatedMetric() {
    }

    public AggregatedMetric(String metricName, String serviceId, String serviceName,
                            String environment, double sum, double min, double max,
                            long count, double avg, long windowStartMs, long windowEndMs) {
        this.metricName = metricName;
        this.serviceId = serviceId;
        this.serviceName = serviceName;
        this.environment = environment;
        this.sum = sum;
        this.min = min;
        this.max = max;
        this.count = count;
        this.avg = avg;
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
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

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    public long getWindowStartMs() {
        return windowStartMs;
    }

    public void setWindowStartMs(long windowStartMs) {
        this.windowStartMs = windowStartMs;
    }

    public long getWindowEndMs() {
        return windowEndMs;
    }

    public void setWindowEndMs(long windowEndMs) {
        this.windowEndMs = windowEndMs;
    }

    @Override
    public String toString() {
        return "AggregatedMetric{" +
                "metricName='" + metricName + '\'' +
                ", serviceId='" + serviceId + '\'' +
                ", count=" + count +
                ", avg=" + avg +
                ", windowStartMs=" + windowStartMs +
                ", windowEndMs=" + windowEndMs +
                '}';
    }
}
