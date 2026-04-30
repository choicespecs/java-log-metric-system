package com.metrics.flink.serialization;

import com.metrics.flink.model.ExternalDataRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serial;
import java.nio.charset.StandardCharsets;

/**
 * Captures the Kafka record key (source-api identifier) alongside the raw JSON
 * payload so Flink can partition HDFS output by source API without any schema
 * assumptions about the payload structure.
 */
public class ExternalDataDeserializer
        implements KafkaRecordDeserializationSchema<ExternalDataRecord> {

    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            Collector<ExternalDataRecord> out) {
        if (record.value() == null) return;

        String sourceApi = record.key() != null
                ? new String(record.key(), StandardCharsets.UTF_8)
                : "unknown";
        String payload = new String(record.value(), StandardCharsets.UTF_8).strip();

        if (payload.isEmpty()) return;

        out.collect(new ExternalDataRecord(sourceApi, payload, System.currentTimeMillis()));
    }

    @Override
    public TypeInformation<ExternalDataRecord> getProducedType() {
        return TypeInformation.of(ExternalDataRecord.class);
    }
}
