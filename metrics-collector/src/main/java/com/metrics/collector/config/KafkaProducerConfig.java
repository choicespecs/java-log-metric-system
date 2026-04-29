package com.metrics.collector.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer configuration for metrics-collector.
 *
 * <p>Key producer settings:
 * <ul>
 *   <li>{@code acks=all} — leader waits for all in-sync replicas to acknowledge before
 *       confirming the write, maximising durability.</li>
 *   <li>{@code enable.idempotence=true} — prevents duplicate records on producer retry,
 *       required for exactly-once producer semantics.</li>
 *   <li>{@code linger.ms=5} — allows micro-batching of up to 5ms to improve throughput
 *       at the cost of a small latency increase.</li>
 *   <li>{@code batch.size=65536} (64 KB) — maximum size of a single batch before it is
 *       sent regardless of {@code linger.ms}.</li>
 *   <li>{@code retries=3} — automatically retries transient errors up to 3 times.</li>
 * </ul>
 *
 * <p>Both key and value are serialised as UTF-8 strings. Structured events are JSON;
 * raw log lines are published verbatim.
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Creates the Kafka {@link ProducerFactory} with durability-oriented settings.
     *
     * @return a {@link DefaultKafkaProducerFactory} configured with {@code acks=all}
     *         and idempotence enabled
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Creates the {@link KafkaTemplate} used by all publisher services.
     *
     * @return a {@link KafkaTemplate} backed by the configured {@link ProducerFactory}
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
