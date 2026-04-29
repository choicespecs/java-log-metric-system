package com.metrics.gateway.controller;

import com.metrics.gateway.model.MetricQueryResult;
import com.metrics.gateway.model.TimeSeriesPoint;
import com.metrics.gateway.service.ParquetQueryService;
import com.metrics.gateway.service.TimeSeriesQueryService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(MetricsQueryController.class)
class MetricsQueryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private TimeSeriesQueryService timeSeriesQueryService;

    @MockBean
    private ParquetQueryService parquetQueryService;

    @Test
    void queryTimeSeries_returnsOkWithResults() throws Exception {
        Instant windowStart = Instant.parse("2024-01-15T10:00:00Z");
        Instant windowEnd = Instant.parse("2024-01-15T10:01:00Z");

        List<MetricQueryResult> mockResults = List.of(
                new MetricQueryResult("cpu.usage", "svc-api", 72.5, 60.0, 90.0, 60L,
                        windowStart, windowEnd),
                new MetricQueryResult("cpu.usage", "svc-api", 68.0, 55.0, 85.0, 60L,
                        windowEnd, Instant.parse("2024-01-15T10:02:00Z"))
        );

        when(timeSeriesQueryService.queryAggregates(
                eq("cpu.usage"), eq("svc-api"), any(Instant.class), any(Instant.class)))
                .thenReturn(mockResults);

        mockMvc.perform(get("/api/v1/query/timeseries")
                        .param("metricName", "cpu.usage")
                        .param("serviceId", "svc-api")
                        .param("from", "2024-01-15T00:00:00Z")
                        .param("to", "2024-01-16T00:00:00Z")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[0].metricName", is("cpu.usage")))
                .andExpect(jsonPath("$[0].serviceId", is("svc-api")))
                .andExpect(jsonPath("$[0].avg", is(72.5)))
                .andExpect(jsonPath("$[0].min", is(60.0)))
                .andExpect(jsonPath("$[0].max", is(90.0)))
                .andExpect(jsonPath("$[0].count", is(60)));
    }

    @Test
    void queryTimeSeries_emptyResult_returnsEmptyArray() throws Exception {
        when(timeSeriesQueryService.queryAggregates(
                any(), any(), any(Instant.class), any(Instant.class)))
                .thenReturn(List.of());

        mockMvc.perform(get("/api/v1/query/timeseries")
                        .param("metricName", "nonexistent.metric")
                        .param("serviceId", "svc-unknown")
                        .param("from", "2024-01-01T00:00:00Z")
                        .param("to", "2024-01-02T00:00:00Z")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(0)));
    }

    @Test
    void querySeries_returnsTimeSeriesPoints() throws Exception {
        List<TimeSeriesPoint> mockPoints = List.of(
                new TimeSeriesPoint(Instant.parse("2024-01-15T10:00:00Z"), 72.5),
                new TimeSeriesPoint(Instant.parse("2024-01-15T10:01:00Z"), 68.0),
                new TimeSeriesPoint(Instant.parse("2024-01-15T10:02:00Z"), 75.0)
        );

        when(timeSeriesQueryService.queryTimeSeries(
                eq("cpu.usage"), eq("svc-api"), any(Instant.class), any(Instant.class)))
                .thenReturn(mockPoints);

        mockMvc.perform(get("/api/v1/query/series")
                        .param("metricName", "cpu.usage")
                        .param("serviceId", "svc-api")
                        .param("from", "2024-01-15T00:00:00Z")
                        .param("to", "2024-01-16T00:00:00Z")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(3)))
                .andExpect(jsonPath("$[0].value", is(72.5)))
                .andExpect(jsonPath("$[1].value", is(68.0)))
                .andExpect(jsonPath("$[2].value", is(75.0)));
    }

    @Test
    void listParquetFiles_returnsFileList() throws Exception {
        List<String> mockFiles = List.of(
                "aggregated/production/2024-01-15/part-0.snappy.parquet",
                "aggregated/production/2024-01-15/part-1.snappy.parquet"
        );

        when(parquetQueryService.listRecentFiles(eq("aggregated/production/2024-01-15")))
                .thenReturn(mockFiles);

        mockMvc.perform(get("/api/v1/query/files")
                        .param("prefix", "aggregated/production/2024-01-15")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[0]", containsString("part-0.snappy.parquet")));
    }

    @Test
    void queryTimeSeries_invalidTimeRange_returnsBadRequest() throws Exception {
        when(timeSeriesQueryService.queryAggregates(any(), any(), any(Instant.class), any(Instant.class)))
                .thenThrow(new IllegalArgumentException("'from' must be strictly before 'to'"));

        mockMvc.perform(get("/api/v1/query/timeseries")
                        .param("metricName", "cpu.usage")
                        .param("serviceId", "svc-api")
                        .param("from", "2024-01-16T00:00:00Z")
                        .param("to", "2024-01-15T00:00:00Z")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }
}
