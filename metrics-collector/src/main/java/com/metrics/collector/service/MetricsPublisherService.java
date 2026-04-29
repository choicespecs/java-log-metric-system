package com.metrics.collector.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.metrics.collector.model.MetricEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
public class MetricsPublisherService {

    private static final Logger log = LoggerFactory.getLogger(MetricsPublisherService.class);

    private static final String METRICS_TOPIC = "metrics.structured";
    private static final String LOGS_TOPIC = "logs.raw";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public MetricsPublisherService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public void publishMetric(MetricEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            String key = event.getServiceId();
            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(METRICS_TOPIC, key, payload);
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish metric event for serviceId={} metricName={}: {}",
                            event.getServiceId(), event.getMetricName(), ex.getMessage(), ex);
                } else {
                    log.debug("Published metric event for serviceId={} metricName={} to partition={} offset={}",
                            event.getServiceId(), event.getMetricName(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize MetricEvent: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to serialize MetricEvent", e);
        }
    }

    public void publishMetrics(List<MetricEvent> events) {
        for (MetricEvent event : events) {
            publishMetric(event);
        }
    }

    public void publishLog(String rawLine, String source) {
        String key = source != null ? source : "unknown";
        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(LOGS_TOPIC, key, rawLine);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish log line from source={}: {}",
                        source, ex.getMessage(), ex);
            } else {
                log.debug("Published log line from source={} to partition={} offset={}",
                        source,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    public void publishLogs(List<String> rawLines, String source) {
        for (String line : rawLines) {
            publishLog(line, source);
        }
    }
}
