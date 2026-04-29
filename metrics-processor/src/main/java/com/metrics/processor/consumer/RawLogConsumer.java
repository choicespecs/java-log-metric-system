package com.metrics.processor.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.metrics.processor.model.MetricEvent;
import com.metrics.processor.model.NormalizedMetric;
import com.metrics.processor.parser.LogParser;
import com.metrics.processor.service.EnrichmentService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;

/**
 * Kafka consumer that parses raw log lines and forwards matching events downstream.
 *
 * <p>Listens on the {@code logs.raw} topic (consumer group {@code log-processor}).
 * For each record:
 * <ol>
 *   <li>Attempts regex parsing via {@link LogParser}. Lines that do not match the
 *       standard log pattern are silently dropped.</li>
 *   <li>For matching lines, constructs a synthetic {@link MetricEvent} with
 *       {@code metricName="log.event"}, {@code value=1.0}, and the parsed
 *       {@code serviceId} and {@code timestamp}.</li>
 *   <li>Calls {@link EnrichmentService#enrichWithLogData} to attach service metadata
 *       and set {@code parsedLevel} and {@code logMessage} on the {@link NormalizedMetric}.</li>
 *   <li>Publishes the enriched {@link NormalizedMetric} to {@code metrics.normalized}.</li>
 * </ol>
 *
 * <p>Note: {@code logs.raw} is also consumed by Flink Pipeline 2 (different consumer group)
 * for HDFS archival. These two paths are independent — this consumer handles near-real-time
 * metric extraction; Flink handles durable long-term archival.
 */
@Component
public class RawLogConsumer {

    private static final Logger log = LoggerFactory.getLogger(RawLogConsumer.class);

    private static final String OUTPUT_TOPIC = "metrics.normalized";

    private final LogParser logParser;
    private final EnrichmentService enrichmentService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public RawLogConsumer(LogParser logParser,
                          EnrichmentService enrichmentService,
                          KafkaTemplate<String, String> kafkaTemplate) {
        this.logParser = logParser;
        this.enrichmentService = enrichmentService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    /**
     * Processes a single raw log record from {@code logs.raw}.
     *
     * @param record the Kafka consumer record containing a raw log line string
     */
    @KafkaListener(topics = "logs.raw", groupId = "log-processor")
    public void consume(ConsumerRecord<String, String> record) {
        String rawLine = record.value();
        log.debug("Received raw log from partition={} offset={}", record.partition(), record.offset());

        try {
            Optional<NormalizedMetric> parsedOptional = logParser.parse(rawLine);

            if (parsedOptional.isEmpty()) {
                log.debug("Log line did not match any pattern, skipping: {}", rawLine);
                return;
            }

            NormalizedMetric parsed = parsedOptional.get();

            // Build a synthetic MetricEvent to reuse the enrichment service cache
            MetricEvent syntheticEvent = new MetricEvent(
                    parsed.getMetricName(),
                    parsed.getValue(),
                    parsed.getUnit(),
                    parsed.getServiceId(),
                    parsed.getTags(),
                    parsed.getTimestamp() != null ? parsed.getTimestamp() : Instant.now()
            );

            // Enrich with metadata, then merge log-specific fields back
            NormalizedMetric enriched = enrichmentService.enrichWithLogData(
                    syntheticEvent,
                    parsed.getParsedLevel(),
                    parsed.getLogMessage()
            );

            String enrichedJson = objectMapper.writeValueAsString(enriched);

            kafkaTemplate.send(OUTPUT_TOPIC, enriched.getServiceId(), enrichedJson)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish enriched log metric for serviceId={}: {}",
                                    enriched.getServiceId(), ex.getMessage(), ex);
                        } else {
                            log.debug("Published enriched log metric for serviceId={} to partition={} offset={}",
                                    enriched.getServiceId(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        }
                    });
        } catch (Exception e) {
            log.error("Error processing raw log record at partition={} offset={}: {}",
                    record.partition(), record.offset(), e.getMessage(), e);
        }
    }
}
