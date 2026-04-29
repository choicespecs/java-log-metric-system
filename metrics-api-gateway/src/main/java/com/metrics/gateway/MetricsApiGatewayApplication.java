package com.metrics.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the metrics-api-gateway Spring Boot service (port 8083).
 *
 * <p>This service is the query boundary for the distributed metrics system.
 * It exposes three read-only REST endpoints via {@code MetricsQueryController}:
 * <ul>
 *   <li>{@code GET /api/v1/query/timeseries} — returns full aggregated rows
 *       (avg/min/max/count per 1-minute window) from TimescaleDB for a given
 *       metric + service + time range.</li>
 *   <li>{@code GET /api/v1/query/series} — returns lightweight (timestamp, avg_value)
 *       time-series points from TimescaleDB, suitable for charting.</li>
 *   <li>{@code GET /api/v1/query/files} — lists Parquet file object names in MinIO
 *       under an optional prefix, for data discovery and export workflows.</li>
 * </ul>
 *
 * <p>TimescaleDB is queried via {@code JdbcTemplate} (raw SQL to leverage hypertable
 * index performance). MinIO is accessed via the MinIO Java SDK ({@code MinioClient}).
 * Neither service is available for write operations through this gateway.
 */
@SpringBootApplication
public class MetricsApiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(MetricsApiGatewayApplication.class, args);
    }
}
