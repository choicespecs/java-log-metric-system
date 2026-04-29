package com.metrics.flink.sink;

import com.metrics.flink.model.AggregatedMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;

/**
 * Flink SinkFunction that upserts AggregatedMetric records into TimescaleDB.
 *
 * <p>Target table DDL (run this on TimescaleDB before submitting the job):
 * <pre>
 * CREATE TABLE IF NOT EXISTS metric_aggregates (
 *     metric_name   TEXT NOT NULL,
 *     service_id    TEXT NOT NULL,
 *     service_name  TEXT,
 *     environment   TEXT,
 *     sum_value     DOUBLE PRECISION,
 *     min_value     DOUBLE PRECISION,
 *     max_value     DOUBLE PRECISION,
 *     count_value   BIGINT,
 *     avg_value     DOUBLE PRECISION,
 *     window_start  TIMESTAMPTZ NOT NULL,
 *     window_end    TIMESTAMPTZ NOT NULL,
 *     PRIMARY KEY (metric_name, service_id, window_start)
 * );
 * SELECT create_hypertable('metric_aggregates', 'window_start', if_not_exists => TRUE);
 * </pre>
 */
public class TimescaleDbSink extends RichSinkFunction<AggregatedMetric> {

    private static final Logger log = LoggerFactory.getLogger(TimescaleDbSink.class);

    private static final String UPSERT_SQL =
            "INSERT INTO metric_aggregates " +
            "(metric_name, service_id, service_name, environment, " +
            " sum_value, min_value, max_value, count_value, avg_value, " +
            " window_start, window_end) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (metric_name, service_id, window_start) DO UPDATE SET " +
            "  service_name  = EXCLUDED.service_name, " +
            "  environment   = EXCLUDED.environment, " +
            "  sum_value     = EXCLUDED.sum_value, " +
            "  min_value     = EXCLUDED.min_value, " +
            "  max_value     = EXCLUDED.max_value, " +
            "  count_value   = EXCLUDED.count_value, " +
            "  avg_value     = EXCLUDED.avg_value, " +
            "  window_end    = EXCLUDED.window_end";

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private transient Connection connection;
    private transient PreparedStatement preparedStatement;

    public TimescaleDbSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(true);
        preparedStatement = connection.prepareStatement(UPSERT_SQL);
        log.info("TimescaleDbSink opened JDBC connection to {}", jdbcUrl);
    }

    @Override
    public void invoke(AggregatedMetric metric, Context context) throws Exception {
        try {
            preparedStatement.setString(1, metric.getMetricName());
            preparedStatement.setString(2, metric.getServiceId());
            preparedStatement.setString(3, metric.getServiceName());
            preparedStatement.setString(4, metric.getEnvironment());
            preparedStatement.setDouble(5, metric.getSum());
            preparedStatement.setDouble(6, metric.getMin());
            preparedStatement.setDouble(7, metric.getMax());
            preparedStatement.setLong(8, metric.getCount());
            preparedStatement.setDouble(9, metric.getAvg());
            preparedStatement.setTimestamp(10, Timestamp.from(Instant.ofEpochMilli(metric.getWindowStartMs())));
            preparedStatement.setTimestamp(11, Timestamp.from(Instant.ofEpochMilli(metric.getWindowEndMs())));

            preparedStatement.executeUpdate();

            log.debug("Upserted AggregatedMetric: metricName={} serviceId={} windowStart={}",
                    metric.getMetricName(), metric.getServiceId(), metric.getWindowStartMs());
        } catch (SQLException e) {
            log.error("Failed to upsert AggregatedMetric for metricName={} serviceId={}: {}",
                    metric.getMetricName(), metric.getServiceId(), e.getMessage(), e);
            // Re-throw to trigger Flink's fault tolerance / retry mechanism
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                log.warn("Error closing PreparedStatement: {}", e.getMessage());
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn("Error closing JDBC connection: {}", e.getMessage());
            }
        }
        log.info("TimescaleDbSink closed JDBC connection");
        super.close();
    }
}
