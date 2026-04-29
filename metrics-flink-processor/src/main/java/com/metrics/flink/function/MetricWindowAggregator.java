package com.metrics.flink.function;

import com.metrics.flink.model.AggregatedMetric;
import com.metrics.flink.model.NormalizedMetric;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Flink AggregateFunction that incrementally computes sum, min, max, and count
 * for a stream of NormalizedMetric events within a tumbling window.
 * getResult() finalises the avg = sum / count.
 */
public class MetricWindowAggregator
        implements AggregateFunction<NormalizedMetric, AggregatedMetric, AggregatedMetric> {

    @Override
    public AggregatedMetric createAccumulator() {
        AggregatedMetric acc = new AggregatedMetric();
        acc.setSum(0.0);
        acc.setMin(Double.MAX_VALUE);
        acc.setMax(Double.MIN_VALUE);
        acc.setCount(0L);
        acc.setAvg(0.0);
        return acc;
    }

    @Override
    public AggregatedMetric add(NormalizedMetric metric, AggregatedMetric accumulator) {
        double value = metric.getValue();

        accumulator.setSum(accumulator.getSum() + value);
        accumulator.setMin(Math.min(accumulator.getMin(), value));
        accumulator.setMax(Math.max(accumulator.getMax(), value));
        accumulator.setCount(accumulator.getCount() + 1);

        // Carry forward metadata from the most recent event (stable within a key-window)
        accumulator.setMetricName(metric.getMetricName());
        accumulator.setServiceId(metric.getServiceId());
        accumulator.setServiceName(metric.getServiceName());
        accumulator.setEnvironment(metric.getEnvironment());

        return accumulator;
    }

    @Override
    public AggregatedMetric getResult(AggregatedMetric accumulator) {
        long count = accumulator.getCount();
        if (count > 0) {
            accumulator.setAvg(accumulator.getSum() / count);
        } else {
            accumulator.setAvg(0.0);
            // Reset sentinels if no data was received
            accumulator.setMin(0.0);
            accumulator.setMax(0.0);
        }
        return accumulator;
    }

    @Override
    public AggregatedMetric merge(AggregatedMetric a, AggregatedMetric b) {
        AggregatedMetric merged = new AggregatedMetric();
        merged.setSum(a.getSum() + b.getSum());
        merged.setMin(Math.min(a.getMin(), b.getMin()));
        merged.setMax(Math.max(a.getMax(), b.getMax()));
        merged.setCount(a.getCount() + b.getCount());
        merged.setMetricName(a.getMetricName() != null ? a.getMetricName() : b.getMetricName());
        merged.setServiceId(a.getServiceId() != null ? a.getServiceId() : b.getServiceId());
        merged.setServiceName(a.getServiceName() != null ? a.getServiceName() : b.getServiceName());
        merged.setEnvironment(a.getEnvironment() != null ? a.getEnvironment() : b.getEnvironment());
        return merged;
    }
}
