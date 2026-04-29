package com.metrics.flink.function;

import com.metrics.flink.model.AggregatedMetric;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * ProcessWindowFunction that enriches the pre-aggregated AggregatedMetric
 * with the actual window start and end timestamps (in milliseconds).
 * This function is chained after MetricWindowAggregator in a combined window operation.
 */
public class MetricWindowProcessFunction
        extends ProcessWindowFunction<AggregatedMetric, AggregatedMetric, String, TimeWindow> {

    @Override
    public void process(
            String key,
            Context context,
            Iterable<AggregatedMetric> elements,
            Collector<AggregatedMetric> out) {

        TimeWindow window = context.window();
        long windowStartMs = window.getStart();
        long windowEndMs = window.getEnd();

        for (AggregatedMetric aggregated : elements) {
            aggregated.setWindowStartMs(windowStartMs);
            aggregated.setWindowEndMs(windowEndMs);
            out.collect(aggregated);
        }
    }
}
