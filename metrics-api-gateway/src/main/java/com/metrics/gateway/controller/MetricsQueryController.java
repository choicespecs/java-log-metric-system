package com.metrics.gateway.controller;

import com.metrics.gateway.model.MetricQueryResult;
import com.metrics.gateway.model.TimeSeriesPoint;
import com.metrics.gateway.service.ParquetQueryService;
import com.metrics.gateway.service.TimeSeriesQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/v1/query")
public class MetricsQueryController {

    private static final Logger log = LoggerFactory.getLogger(MetricsQueryController.class);

    private final TimeSeriesQueryService timeSeriesQueryService;
    private final ParquetQueryService parquetQueryService;

    public MetricsQueryController(TimeSeriesQueryService timeSeriesQueryService,
                                   ParquetQueryService parquetQueryService) {
        this.timeSeriesQueryService = timeSeriesQueryService;
        this.parquetQueryService = parquetQueryService;
    }

    /**
     * Returns full aggregated metric rows (avg/min/max/count per window) for a metric + service
     * within the specified time range.
     *
     * <p>Example:
     * {@code GET /api/v1/query/timeseries?metricName=cpu.usage&serviceId=svc-api&from=2024-01-01T00:00:00Z&to=2024-01-02T00:00:00Z}
     */
    @GetMapping("/timeseries")
    public ResponseEntity<List<MetricQueryResult>> queryTimeSeries(
            @RequestParam String metricName,
            @RequestParam String serviceId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to) {

        log.info("GET /api/v1/query/timeseries metricName={} serviceId={} from={} to={}",
                metricName, serviceId, from, to);

        List<MetricQueryResult> results = timeSeriesQueryService.queryAggregates(
                metricName, serviceId, from, to);

        return ResponseEntity.ok(results);
    }

    /**
     * Returns a lightweight time-series (timestamp → avg_value) suitable for charting.
     *
     * <p>Example:
     * {@code GET /api/v1/query/series?metricName=cpu.usage&serviceId=svc-api&from=2024-01-01T00:00:00Z&to=2024-01-02T00:00:00Z}
     */
    @GetMapping("/series")
    public ResponseEntity<List<TimeSeriesPoint>> querySeries(
            @RequestParam String metricName,
            @RequestParam String serviceId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to) {

        log.info("GET /api/v1/query/series metricName={} serviceId={} from={} to={}",
                metricName, serviceId, from, to);

        List<TimeSeriesPoint> points = timeSeriesQueryService.queryTimeSeries(
                metricName, serviceId, from, to);

        return ResponseEntity.ok(points);
    }

    /**
     * Lists Parquet files in MinIO under the given prefix.
     *
     * <p>Example:
     * {@code GET /api/v1/query/files?prefix=aggregated/production/2024-01-15}
     */
    @GetMapping("/files")
    public ResponseEntity<List<String>> listParquetFiles(
            @RequestParam(required = false, defaultValue = "") String prefix) {

        log.info("GET /api/v1/query/files prefix={}", prefix);

        List<String> files = parquetQueryService.listRecentFiles(prefix);

        return ResponseEntity.ok(files);
    }

    /**
     * Global exception handler for IllegalArgumentException (e.g. invalid time range).
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleIllegalArgument(IllegalArgumentException ex) {
        log.warn("Bad request: {}", ex.getMessage());
        return ResponseEntity.badRequest().body(ex.getMessage());
    }
}
