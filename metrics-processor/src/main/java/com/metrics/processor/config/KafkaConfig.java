package com.metrics.processor.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Kafka consumer/producer configuration and Caffeine cache setup for metrics-processor.
 *
 * <p><b>Consumer settings:</b>
 * <ul>
 *   <li>Concurrency: 3 listener threads per {@code @KafkaListener} container.</li>
 *   <li>Auto-offset-reset: {@code earliest} — replays from the beginning on first start.</li>
 *   <li>Auto-commit: enabled with a 1-second commit interval.
 *       <b>Note:</b> auto-commit risks message loss on crash before downstream publish completes.
 *       Consider switching to manual offset commit ({@code AckMode.MANUAL}) for stronger guarantees.</li>
 * </ul>
 *
 * <p><b>Producer settings:</b> mirrors the collector's producer — {@code acks=all},
 * {@code retries=3}, {@code linger.ms=5}, {@code batch.size=65536}.
 *
 * <p><b>Cache ({@code serviceMetadata}):</b> Caffeine cache used by {@code EnrichmentService}
 * to avoid repeated PostgreSQL lookups for the same {@code serviceId}.
 * Maximum 500 entries, entries expire 10 minutes after write.
 */
@Configuration
@EnableCaching
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:metrics-processor}")
    private String groupId;

    /**
     * Consumer factory used by both {@code StructuredMetricsConsumer} and {@code RawLogConsumer}.
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Listener container factory with concurrency=3, allowing up to 3 partitions
     * to be consumed in parallel per listener.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        return factory;
    }

    /**
     * Producer factory for forwarding enriched events to {@code metrics.normalized}.
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /** KafkaTemplate used by both consumer components to re-publish enriched events. */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Caffeine-backed cache manager for service metadata lookups.
     *
     * <p>Cache name: {@code "serviceMetadata"}. Max size: 500 entries.
     * Entries expire 10 minutes after write, after which the next lookup will
     * hit PostgreSQL and repopulate the cache.
     */
    @Bean
    public CacheManager cacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager("serviceMetadata");
        cacheManager.setCaffeine(
                Caffeine.newBuilder()
                        .maximumSize(500)
                        .expireAfterWrite(10, TimeUnit.MINUTES)
                        .recordStats()
        );
        return cacheManager;
    }
}
