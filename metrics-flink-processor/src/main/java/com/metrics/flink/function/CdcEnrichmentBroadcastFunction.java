package com.metrics.flink.function;

import com.metrics.flink.model.CdcEvent;
import com.metrics.flink.model.NormalizedMetric;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BroadcastProcessFunction that enriches NormalizedMetric events with the latest
 * service metadata from a CDC (Debezium) broadcast stream.
 *
 * <p>The CDC stream is broadcast to all parallel instances. When a metric arrives,
 * this function looks up the matching CdcEvent in broadcast state and overwrites
 * the service metadata fields (serviceName, team, environment, region) if found.
 */
public class CdcEnrichmentBroadcastFunction
        extends BroadcastProcessFunction<NormalizedMetric, CdcEvent, NormalizedMetric> {

    private static final Logger log = LoggerFactory.getLogger(CdcEnrichmentBroadcastFunction.class);

    private final MapStateDescriptor<String, CdcEvent> cdcStateDescriptor;

    public CdcEnrichmentBroadcastFunction(MapStateDescriptor<String, CdcEvent> cdcStateDescriptor) {
        this.cdcStateDescriptor = cdcStateDescriptor;
    }

    @Override
    public void processElement(
            NormalizedMetric metric,
            ReadOnlyContext ctx,
            Collector<NormalizedMetric> out) throws Exception {

        ReadOnlyBroadcastState<String, CdcEvent> broadcastState =
                ctx.getBroadcastState(cdcStateDescriptor);

        String serviceId = metric.getServiceId();
        if (serviceId != null) {
            CdcEvent cdcEvent = broadcastState.get(serviceId);
            if (cdcEvent != null) {
                // Overwrite metric metadata with the latest CDC data
                if (cdcEvent.getServiceName() != null) {
                    metric.setServiceName(cdcEvent.getServiceName());
                }
                if (cdcEvent.getTeam() != null) {
                    metric.setTeam(cdcEvent.getTeam());
                }
                if (cdcEvent.getEnvironment() != null) {
                    metric.setEnvironment(cdcEvent.getEnvironment());
                }
                if (cdcEvent.getRegion() != null) {
                    metric.setRegion(cdcEvent.getRegion());
                }
                log.debug("Enriched metric serviceId={} with CDC data", serviceId);
            } else {
                log.debug("No CDC state found for serviceId={}, using original metadata", serviceId);
            }
        }

        out.collect(metric);
    }

    @Override
    public void processBroadcastElement(
            CdcEvent cdcEvent,
            Context ctx,
            Collector<NormalizedMetric> out) throws Exception {

        BroadcastState<String, CdcEvent> broadcastState = ctx.getBroadcastState(cdcStateDescriptor);
        String serviceId = cdcEvent.getServiceId();

        if (serviceId == null || serviceId.isBlank()) {
            log.warn("Received CDC event with null/blank serviceId, skipping: {}", cdcEvent);
            return;
        }

        String operation = cdcEvent.getOperation();
        if ("d".equalsIgnoreCase(operation)) {
            // Delete operation — remove from broadcast state
            broadcastState.remove(serviceId);
            log.info("Removed CDC state for deleted serviceId={}", serviceId);
        } else {
            // Create (c), Update (u), or Read/Snapshot (r) — upsert into broadcast state
            broadcastState.put(serviceId, cdcEvent);
            log.info("Updated CDC broadcast state for serviceId={} operation={}", serviceId, operation);
        }
    }
}
