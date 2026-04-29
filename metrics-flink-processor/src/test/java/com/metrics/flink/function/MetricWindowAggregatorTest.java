package com.metrics.flink.function;

import com.metrics.flink.model.AggregatedMetric;
import com.metrics.flink.model.NormalizedMetric;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

/**
 * JUnit 4 unit tests for MetricWindowAggregator.
 * Flink's test utilities still rely on JUnit 4, so we use it here.
 */
public class MetricWindowAggregatorTest {

    private MetricWindowAggregator aggregator;

    @Before
    public void setUp() {
        aggregator = new MetricWindowAggregator();
    }

    private NormalizedMetric buildMetric(String metricName, String serviceId, double value) {
        NormalizedMetric m = new NormalizedMetric();
        m.setMetricName(metricName);
        m.setServiceId(serviceId);
        m.setServiceName("Test Service");
        m.setEnvironment("test");
        m.setValue(value);
        m.setTimestampMs(System.currentTimeMillis());
        return m;
    }

    @Test
    public void createAccumulator_returnsZeroedState() {
        AggregatedMetric acc = aggregator.createAccumulator();
        assertEquals(0.0, acc.getSum(), 0.001);
        assertEquals(0L, acc.getCount());
        assertEquals(Double.MAX_VALUE, acc.getMin(), 0.001);
        assertEquals(Double.MIN_VALUE, acc.getMax(), 0.001);
    }

    @Test
    public void add_singleMetric_correctSumMinMaxCount() {
        AggregatedMetric acc = aggregator.createAccumulator();
        NormalizedMetric metric = buildMetric("cpu.usage", "svc-api", 75.0);

        acc = aggregator.add(metric, acc);

        assertEquals(75.0, acc.getSum(), 0.001);
        assertEquals(75.0, acc.getMin(), 0.001);
        assertEquals(75.0, acc.getMax(), 0.001);
        assertEquals(1L, acc.getCount());
        assertEquals("cpu.usage", acc.getMetricName());
        assertEquals("svc-api", acc.getServiceId());
    }

    @Test
    public void add_multipleMetrics_correctAggregates() {
        AggregatedMetric acc = aggregator.createAccumulator();

        acc = aggregator.add(buildMetric("cpu.usage", "svc-api", 10.0), acc);
        acc = aggregator.add(buildMetric("cpu.usage", "svc-api", 50.0), acc);
        acc = aggregator.add(buildMetric("cpu.usage", "svc-api", 90.0), acc);

        assertEquals(150.0, acc.getSum(), 0.001);
        assertEquals(10.0, acc.getMin(), 0.001);
        assertEquals(90.0, acc.getMax(), 0.001);
        assertEquals(3L, acc.getCount());
    }

    @Test
    public void getResult_computesAverageCorrectly() {
        AggregatedMetric acc = aggregator.createAccumulator();

        acc = aggregator.add(buildMetric("memory.usage", "svc-worker", 200.0), acc);
        acc = aggregator.add(buildMetric("memory.usage", "svc-worker", 400.0), acc);
        acc = aggregator.add(buildMetric("memory.usage", "svc-worker", 600.0), acc);

        AggregatedMetric result = aggregator.getResult(acc);

        assertEquals(1200.0, result.getSum(), 0.001);
        assertEquals(200.0, result.getMin(), 0.001);
        assertEquals(600.0, result.getMax(), 0.001);
        assertEquals(3L, result.getCount());
        assertEquals(400.0, result.getAvg(), 0.001); // (200 + 400 + 600) / 3
    }

    @Test
    public void getResult_withSingleValue_avgEqualsValue() {
        AggregatedMetric acc = aggregator.createAccumulator();
        acc = aggregator.add(buildMetric("disk.io", "svc-api", 42.5), acc);

        AggregatedMetric result = aggregator.getResult(acc);

        assertEquals(42.5, result.getAvg(), 0.001);
        assertEquals(42.5, result.getMin(), 0.001);
        assertEquals(42.5, result.getMax(), 0.001);
    }

    @Test
    public void getResult_emptyAccumulator_doesNotThrow() {
        AggregatedMetric acc = aggregator.createAccumulator();

        AggregatedMetric result = aggregator.getResult(acc);

        assertEquals(0L, result.getCount());
        assertEquals(0.0, result.getAvg(), 0.001);
        assertEquals(0.0, result.getSum(), 0.001);
    }

    @Test
    public void merge_combinesTwoAccumulators() {
        AggregatedMetric acc1 = aggregator.createAccumulator();
        acc1 = aggregator.add(buildMetric("cpu.usage", "svc-a", 10.0), acc1);
        acc1 = aggregator.add(buildMetric("cpu.usage", "svc-a", 20.0), acc1);

        AggregatedMetric acc2 = aggregator.createAccumulator();
        acc2 = aggregator.add(buildMetric("cpu.usage", "svc-a", 30.0), acc2);
        acc2 = aggregator.add(buildMetric("cpu.usage", "svc-a", 40.0), acc2);

        AggregatedMetric merged = aggregator.merge(acc1, acc2);

        assertEquals(100.0, merged.getSum(), 0.001);
        assertEquals(10.0, merged.getMin(), 0.001);
        assertEquals(40.0, merged.getMax(), 0.001);
        assertEquals(4L, merged.getCount());
    }

    @Test
    public void add_preservesMetricMetadata() {
        AggregatedMetric acc = aggregator.createAccumulator();
        NormalizedMetric metric = buildMetric("network.bytes", "svc-edge", 1000.0);
        metric.setServiceName("Edge Service");
        metric.setEnvironment("production");

        acc = aggregator.add(metric, acc);

        assertEquals("network.bytes", acc.getMetricName());
        assertEquals("svc-edge", acc.getServiceId());
        assertEquals("Edge Service", acc.getServiceName());
        assertEquals("production", acc.getEnvironment());
    }
}
