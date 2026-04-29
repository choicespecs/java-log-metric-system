package com.metrics.collector.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.metrics.collector.model.MetricEvent;
import com.metrics.collector.service.MetricsPublisherService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(MetricsController.class)
class MetricsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private MetricsPublisherService publisherService;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        doNothing().when(publisherService).publishMetric(any(MetricEvent.class));
        doNothing().when(publisherService).publishMetrics(anyList());
    }

    @Test
    void ingestSingleMetric_returnsAccepted() throws Exception {
        MetricEvent event = new MetricEvent(
                "cpu.usage",
                75.5,
                "percent",
                "svc-api",
                Map.of("host", "host-01", "region", "us-east-1"),
                Instant.now()
        );

        mockMvc.perform(post("/api/v1/metrics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(event)))
                .andExpect(status().isAccepted());

        verify(publisherService, times(1)).publishMetric(any(MetricEvent.class));
    }

    @Test
    void ingestMetricsBatch_returnsAccepted() throws Exception {
        List<MetricEvent> events = List.of(
                new MetricEvent("cpu.usage", 60.0, "percent", "svc-api",
                        Map.of("host", "host-01"), Instant.now()),
                new MetricEvent("memory.usage", 80.0, "percent", "svc-worker",
                        Map.of("host", "host-02"), Instant.now())
        );

        mockMvc.perform(post("/api/v1/metrics/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(events)))
                .andExpect(status().isAccepted());

        verify(publisherService, times(1)).publishMetrics(anyList());
    }

    @Test
    void ingestMetric_withMissingMetricName_returnsBadRequest() throws Exception {
        String invalidPayload = "{\"value\": 50.0, \"serviceId\": \"svc-api\", \"timestamp\": \"2024-01-01T00:00:00Z\"}";

        mockMvc.perform(post("/api/v1/metrics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(invalidPayload))
                .andExpect(status().isBadRequest());

        verify(publisherService, never()).publishMetric(any());
    }

    @Test
    void ingestMetric_withMissingServiceId_returnsBadRequest() throws Exception {
        String invalidPayload = "{\"metricName\": \"cpu.usage\", \"value\": 50.0, \"timestamp\": \"2024-01-01T00:00:00Z\"}";

        mockMvc.perform(post("/api/v1/metrics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(invalidPayload))
                .andExpect(status().isBadRequest());

        verify(publisherService, never()).publishMetric(any());
    }
}
