package com.metrics.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Represents a Debezium CDC event from the cdc.public.service_metadata topic.
 * Operations: c (create), u (update), d (delete), r (read/snapshot).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CdcEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Debezium operation: c=create, u=update, d=delete, r=read (snapshot) */
    @JsonProperty("op")
    private String operation;

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

    public CdcEvent() {
    }

    public CdcEvent(String operation, String serviceId, String serviceName,
                    String team, String environment, String region) {
        this.operation = operation;
        this.serviceId = serviceId;
        this.serviceName = serviceName;
        this.team = team;
        this.environment = environment;
        this.region = region;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
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

    @Override
    public String toString() {
        return "CdcEvent{" +
                "operation='" + operation + '\'' +
                ", serviceId='" + serviceId + '\'' +
                ", serviceName='" + serviceName + '\'' +
                ", environment='" + environment + '\'' +
                '}';
    }
}
