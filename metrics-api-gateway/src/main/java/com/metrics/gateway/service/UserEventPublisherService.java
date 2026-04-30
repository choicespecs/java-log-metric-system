package com.metrics.gateway.service;

import com.metrics.gateway.model.UserEventRequest;
import com.metrics.proto.UserEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;

@Service
public class UserEventPublisherService {

    private static final Logger log = LoggerFactory.getLogger(UserEventPublisherService.class);

    private final KafkaTemplate<String, UserEvent> kafkaTemplate;

    @Value("${kafka.topics.user-events:users.events}")
    private String userEventsTopic;

    public UserEventPublisherService(KafkaTemplate<String, UserEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(UserEventRequest request) {
        UserEvent event = UserEvent.newBuilder()
                .setUserId(request.getUserId())
                .setEventType(request.getEventType())
                .setTimestampMs(request.getTimestampMs() != null
                        ? request.getTimestampMs()
                        : System.currentTimeMillis())
                .setServiceId(request.getServiceId())
                .setSessionId(request.getSessionId() != null ? request.getSessionId() : "")
                .setEnvironment(request.getEnvironment() != null ? request.getEnvironment() : "")
                .putAllProperties(request.getProperties() != null
                        ? request.getProperties()
                        : Collections.emptyMap())
                .build();

        // Key by userId so all events for a given user land on the same partition
        kafkaTemplate.send(userEventsTopic, event.getUserId(), event);
        log.info("Published UserEvent userId={} type={} to {}", event.getUserId(), event.getEventType(), userEventsTopic);
    }
}
