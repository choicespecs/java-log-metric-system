package com.metrics.gateway.repository;

import com.metrics.gateway.model.MetricQueryResult;
import com.metrics.gateway.model.TimeSeriesPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/**
 * Repository for querying aggregated metrics from TimescaleDB using raw SQL via JdbcTemplate.
 * Raw SQL is preferred over JPA here to take full advantage of TimescaleDB time-series functions.
 */
@Repository
public class AggregatedMetricRepository {

    private static final Logger log = LoggerFactory.getLogger(AggregatedMetricRepository.class);

    private final JdbcTemplate jdbcTemplate;

    public AggregatedMetricRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Returns full aggregated rows for a given metric + service over a time range.
     */
    public List<MetricQueryResult> findByMetricNameAndServiceId(
            String metricName, String serviceId, Instant from, Instant to) {

        String sql =
                "SELECT metric_name, service_id, avg_value, min_value, max_value, " +
                "       count_value, window_start, window_end " +
                "FROM metric_aggregates " +
                "WHERE metric_name = ? " +
                "  AND service_id = ? " +
                "  AND window_start >= ? " +
                "  AND window_end <= ? " +
                "ORDER BY window_start ASC";

        log.debug("Querying metric_aggregates: metricName={} serviceId={} from={} to={}",
                metricName, serviceId, from, to);

        return jdbcTemplate.query(sql,
                (rs, rowNum) -> mapRowToMetricQueryResult(rs),
                metricName,
                serviceId,
                Timestamp.from(from),
                Timestamp.from(to));
    }

    /**
     * Returns a lightweight time-series (timestamp, avg_value) for a given metric + service.
     */
    public List<TimeSeriesPoint> findTimeSeries(
            String metricName, String serviceId, Instant from, Instant to) {

        String sql =
                "SELECT window_start, avg_value " +
                "FROM metric_aggregates " +
                "WHERE metric_name = ? " +
                "  AND service_id = ? " +
                "  AND window_start >= ? " +
                "  AND window_end <= ? " +
                "ORDER BY window_start ASC";

        log.debug("Querying time series: metricName={} serviceId={} from={} to={}",
                metricName, serviceId, from, to);

        return jdbcTemplate.query(sql,
                (rs, rowNum) -> new TimeSeriesPoint(
                        rs.getTimestamp("window_start").toInstant(),
                        rs.getDouble("avg_value")
                ),
                metricName,
                serviceId,
                Timestamp.from(from),
                Timestamp.from(to));
    }

    private MetricQueryResult mapRowToMetricQueryResult(ResultSet rs) throws SQLException {
        return new MetricQueryResult(
                rs.getString("metric_name"),
                rs.getString("service_id"),
                rs.getDouble("avg_value"),
                rs.getDouble("min_value"),
                rs.getDouble("max_value"),
                rs.getLong("count_value"),
                rs.getTimestamp("window_start").toInstant(),
                rs.getTimestamp("window_end").toInstant()
        );
    }
}
