package com.metrics.processor.service;

import com.metrics.processor.model.MetricEvent;
import com.metrics.processor.model.NormalizedMetric;
import com.metrics.processor.model.ServiceMetadata;
import com.metrics.processor.repository.ServiceMetadataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class EnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentService.class);

    private static final String DEFAULT_SERVICE_NAME = "Unknown Service";
    private static final String DEFAULT_TEAM = "unknown";
    private static final String DEFAULT_ENVIRONMENT = "unknown";
    private static final String DEFAULT_REGION = "unknown";

    private final ServiceMetadataRepository serviceMetadataRepository;

    public EnrichmentService(ServiceMetadataRepository serviceMetadataRepository) {
        this.serviceMetadataRepository = serviceMetadataRepository;
    }

    /**
     * Enriches a MetricEvent with service metadata from the database.
     * The service metadata lookup is cached by serviceId to avoid repeated DB queries
     * for the same service.
     */
    public NormalizedMetric enrich(MetricEvent event) {
        ServiceMetadata metadata = resolveMetadata(event.getServiceId());

        NormalizedMetric normalized = new NormalizedMetric();
        normalized.setMetricName(event.getMetricName());
        normalized.setValue(event.getValue());
        normalized.setUnit(event.getUnit());
        normalized.setServiceId(event.getServiceId());
        normalized.setTags(event.getTags());

        Instant timestamp = event.getTimestamp() != null ? event.getTimestamp() : Instant.now();
        normalized.setTimestamp(timestamp);
        normalized.setTimestampMs(timestamp.toEpochMilli());

        normalized.setServiceName(metadata.getServiceName());
        normalized.setTeam(metadata.getTeam());
        normalized.setEnvironment(metadata.getEnvironment());
        normalized.setRegion(metadata.getRegion());

        return normalized;
    }

    /**
     * Enriches a MetricEvent and also sets log-specific fields (parsedLevel, logMessage).
     */
    public NormalizedMetric enrichWithLogData(MetricEvent event, String parsedLevel, String logMessage) {
        NormalizedMetric normalized = enrich(event);
        normalized.setParsedLevel(parsedLevel);
        normalized.setLogMessage(logMessage);
        return normalized;
    }

    /**
     * Looks up service metadata by serviceId. The result is cached by serviceId
     * to avoid repeated DB round-trips for high-volume services.
     *
     * @param serviceId the identifier to look up
     * @return resolved ServiceMetadata, or a default placeholder if not found
     */
    @Cacheable(value = "serviceMetadata", key = "#serviceId")
    public ServiceMetadata resolveMetadata(String serviceId) {
        return serviceMetadataRepository
                .findById(serviceId)
                .orElseGet(() -> {
                    log.warn("No service metadata found for serviceId={}, using defaults", serviceId);
                    return buildDefaultMetadata(serviceId);
                });
    }

    private ServiceMetadata buildDefaultMetadata(String serviceId) {
        return new ServiceMetadata(
                serviceId,
                DEFAULT_SERVICE_NAME,
                DEFAULT_TEAM,
                DEFAULT_ENVIRONMENT,
                DEFAULT_REGION
        );
    }
}
