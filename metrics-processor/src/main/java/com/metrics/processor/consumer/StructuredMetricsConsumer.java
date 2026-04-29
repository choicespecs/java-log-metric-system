package com.metrics.processor.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.metrics.processor.model.MetricEvent;
import com.metrics.processor.model.NormalizedMetric;
import com.metrics.processor.service.EnrichmentService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that enriches structured metric events and forwards them downstream.
 *
 * <p>Listens on the {@code metrics.structured} topic (consumer group {@code metrics-processor}).
 * For each record:
 * <ol>
 *   <li>Deserialises the JSON payload into a {@link MetricEvent}.</li>
 *   <li>Calls {@link EnrichmentService#enrich(MetricEvent)} to attach service metadata
 *       ({@code serviceName}, {@code team}, {@code environment}, {@code region}) sourced
 *       from PostgreSQL via a Caffeine cache.</li>
 *   <li>Serialises the resulting {@link NormalizedMetric} to JSON and publishes it to
 *       {@code metrics.normalized} keyed by {@code serviceId}.</li>
 * </ol>
 *
 * <p>On deserialisation or enrichment failure the record is logged at ERROR level and
 * silently dropped — there is no dead-letter topic. The Kafka offset is committed
 * automatically (see {@code KafkaConfig}).
 */
@Component
public class StructuredMetricsConsumer {

    private static final Logger log = LoggerFactory.getLogger(StructuredMetricsConsumer.class);

    private static final String OUTPUT_TOPIC = "metrics.normalized";

    private final EnrichmentService enrichmentService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public StructuredMetricsConsumer(EnrichmentService enrichmentService,
                                     KafkaTemplate<String, String> kafkaTemplate) {
        this.enrichmentService = enrichmentService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    /**
     * Processes a single record from {@code metrics.structured}.
     *
     * @param record the Kafka consumer record containing a JSON-serialised {@link MetricEvent}
     */
    @KafkaListener(topics = "metrics.structured", groupId = "metrics-processor")
    public void consume(ConsumerRecord<String, String> record) {
        String payload = record.value();
        log.debug("Received structured metric from partition={} offset={}", record.partition(), record.offset());

        try {
            MetricEvent event = objectMapper.readValue(payload, MetricEvent.class);
            NormalizedMetric normalized = enrichmentService.enrich(event);
            String normalizedJson = objectMapper.writeValueAsString(normalized);

            kafkaTemplate.send(OUTPUT_TOPIC, normalized.getServiceId(), normalizedJson)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish normalized metric for serviceId={}: {}",
                                    normalized.getServiceId(), ex.getMessage(), ex);
                        } else {
                            log.debug("Published normalized metric for serviceId={} to partition={} offset={}",
                                    normalized.getServiceId(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        }
                    });
        } catch (Exception e) {
            log.error("Error processing structured metric record at partition={} offset={}: {}",
                    record.partition(), record.offset(), e.getMessage(), e);
        }
    }
}
