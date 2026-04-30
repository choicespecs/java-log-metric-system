package com.metrics.collector.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class ExternalDataPublisherService {

    private static final Logger log = LoggerFactory.getLogger(ExternalDataPublisherService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topics.external-data:external.data.raw}")
    private String externalTopic;

    public ExternalDataPublisherService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes a raw JSON payload to the external data topic without any schema
     * validation or parsing. The sourceApi key partitions messages by origin so
     * the Flink sink can group files per API source on HDFS.
     */
    public void publish(String sourceApi, String rawJson) {
        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(externalTopic, sourceApi, rawJson);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish external data from source={}: {}", sourceApi, ex.getMessage(), ex);
            } else {
                log.debug("Published external data from source={} to partition={} offset={}",
                        sourceApi,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
