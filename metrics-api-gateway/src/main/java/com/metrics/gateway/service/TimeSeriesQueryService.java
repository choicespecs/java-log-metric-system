package com.metrics.gateway.service;

import com.metrics.gateway.model.MetricQueryResult;
import com.metrics.gateway.model.TimeSeriesPoint;
import com.metrics.gateway.repository.AggregatedMetricRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
public class TimeSeriesQueryService {

    private static final Logger log = LoggerFactory.getLogger(TimeSeriesQueryService.class);

    private final AggregatedMetricRepository repository;

    public TimeSeriesQueryService(AggregatedMetricRepository repository) {
        this.repository = repository;
    }

    /**
     * Queries aggregated metric rows from TimescaleDB for the given time range.
     *
     * @param metricName name of the metric (e.g. "cpu.usage")
     * @param serviceId  service identifier (e.g. "svc-api")
     * @param from       start of the query window (inclusive)
     * @param to         end of the query window (inclusive)
     * @return list of aggregated metric results ordered by window_start ascending
     * @throws IllegalArgumentException if {@code from} is not before {@code to}
     */
    public List<MetricQueryResult> queryAggregates(
            String metricName, String serviceId, Instant from, Instant to) {
        validateTimeRange(from, to);
        log.info("Querying aggregates: metricName={} serviceId={} from={} to={}",
                metricName, serviceId, from, to);
        return repository.findByMetricNameAndServiceId(metricName, serviceId, from, to);
    }

    /**
     * Returns a lightweight time-series (timestamp → avg) for charting purposes.
     *
     * @param metricName name of the metric
     * @param serviceId  service identifier
     * @param from       start of the query window (inclusive)
     * @param to         end of the query window (inclusive)
     * @return list of (timestamp, avg_value) points ordered by timestamp ascending
     * @throws IllegalArgumentException if {@code from} is not before {@code to}
     */
    public List<TimeSeriesPoint> queryTimeSeries(
            String metricName, String serviceId, Instant from, Instant to) {
        validateTimeRange(from, to);
        log.info("Querying time series: metricName={} serviceId={} from={} to={}",
                metricName, serviceId, from, to);
        return repository.findTimeSeries(metricName, serviceId, from, to);
    }

    private void validateTimeRange(Instant from, Instant to) {
        if (from == null || to == null) {
            throw new IllegalArgumentException("'from' and 'to' parameters must not be null");
        }
        if (!from.isBefore(to)) {
            throw new IllegalArgumentException(
                    "'from' (" + from + ") must be strictly before 'to' (" + to + ")");
        }
    }
}
