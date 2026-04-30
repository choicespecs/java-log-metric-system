package com.metrics.gateway.controller;

import com.metrics.gateway.model.UserEventRequest;
import com.metrics.gateway.service.UserEventPublisherService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/users")
public class UserEventController {

    private static final Logger log = LoggerFactory.getLogger(UserEventController.class);

    private final UserEventPublisherService publisherService;

    public UserEventController(UserEventPublisherService publisherService) {
        this.publisherService = publisherService;
    }

    /**
     * Accepts a structured user event, serializes it as Protobuf, and publishes
     * to the {@code users.events} Kafka topic via Confluent Schema Registry.
     *
     * <p>The Confluent {@code KafkaProtobufSerializer} auto-registers the schema
     * on first use and prepends a 5-byte Confluent header (magic byte + schema ID)
     * to every message — Flink reads the schema ID to fetch and cache the descriptor.
     *
     * <p>Example:
     * {@code POST /api/v1/users/events}
     * <pre>
     * {
     *   "userId": "u-123",
     *   "eventType": "LOGIN",
     *   "serviceId": "svc-auth",
     *   "environment": "production",
     *   "properties": {"ip": "1.2.3.4", "device": "mobile"}
     * }
     * </pre>
     */
    @PostMapping("/events")
    public ResponseEntity<Void> publishEvent(@RequestBody @Valid UserEventRequest request) {
        log.info("POST /api/v1/users/events userId={} eventType={}", request.getUserId(), request.getEventType());
        publisherService.publish(request);
        return ResponseEntity.accepted().build();
    }
}
