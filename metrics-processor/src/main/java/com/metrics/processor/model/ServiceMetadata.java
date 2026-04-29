package com.metrics.processor.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "service_metadata")
public class ServiceMetadata {

    @Id
    @Column(name = "service_id", length = 128)
    private String serviceId;

    @Column(name = "service_name", length = 256, nullable = false)
    private String serviceName;

    @Column(name = "team", length = 128)
    private String team;

    @Column(name = "environment", length = 64)
    private String environment;

    @Column(name = "region", length = 64)
    private String region;

    public ServiceMetadata() {
    }

    public ServiceMetadata(String serviceId, String serviceName, String team,
                           String environment, String region) {
        this.serviceId = serviceId;
        this.serviceName = serviceName;
        this.team = team;
        this.environment = environment;
        this.region = region;
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
        return "ServiceMetadata{" +
                "serviceId='" + serviceId + '\'' +
                ", serviceName='" + serviceName + '\'' +
                ", team='" + team + '\'' +
                ", environment='" + environment + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
