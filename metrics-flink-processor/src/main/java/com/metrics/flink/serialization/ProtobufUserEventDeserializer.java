package com.metrics.flink.serialization;

import com.metrics.proto.UserEvent;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serial;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Flink deserializer for Protobuf-encoded {@link UserEvent} messages published
 * with the Confluent Schema Registry wire format:
 *
 * <pre>
 *   [0x00][schema-id 4B][message-indexes varint...][protobuf bytes]
 * </pre>
 *
 * Schema IDs are resolved via {@link CachedSchemaRegistryClient} and cached in
 * operator memory ({@code schemaCache}) so the registry is only called once per
 * schema version seen by this task slot — subsequent messages skip the HTTP call.
 */
public class ProtobufUserEventDeserializer
        implements KafkaRecordDeserializationSchema<UserEvent> {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory.getLogger(ProtobufUserEventDeserializer.class);
    private static final byte CONFLUENT_MAGIC_BYTE = 0x00;

    private final String schemaRegistryUrl;
    private final int schemaCacheCapacity;

    // Transient — not serialized; initialized in open() when the operator starts.
    // CachedSchemaRegistryClient handles the HTTP layer with its own internal cache.
    private transient CachedSchemaRegistryClient registryClient;

    // Operator-level cache: schema ID → ParsedSchema to avoid hitting registryClient
    // for every message after the first occurrence of a given schema ID.
    private transient Map<Integer, ParsedSchema> schemaCache;

    public ProtobufUserEventDeserializer(String schemaRegistryUrl, int schemaCacheCapacity) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.schemaCacheCapacity = schemaCacheCapacity;
    }

    @Override
    public void open(org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext context) {
        registryClient = new CachedSchemaRegistryClient(
                Collections.singletonList(schemaRegistryUrl),
                schemaCacheCapacity
        );
        schemaCache = new HashMap<>();
        log.info("ProtobufUserEventDeserializer initialized, schema registry: {}", schemaRegistryUrl);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            Collector<UserEvent> out) throws IOException {
        byte[] bytes = record.value();
        if (bytes == null || bytes.length == 0) return;

        ByteBuffer buf = ByteBuffer.wrap(bytes);

        byte magic = buf.get();
        if (magic != CONFLUENT_MAGIC_BYTE) {
            throw new IOException("Unexpected Confluent magic byte: 0x" + Integer.toHexString(magic & 0xFF));
        }

        int schemaId = buf.getInt();

        // Warm the operator-level cache on first encounter of this schema ID.
        // CachedSchemaRegistryClient makes an HTTP call on cache miss and caches
        // the result; subsequent calls for the same ID are served from memory.
        schemaCache.computeIfAbsent(schemaId, id -> {
            try {
                ParsedSchema schema = registryClient.getSchemaById(id);
                log.debug("Cached schema id={} type={}", id, schema.schemaType());
                return schema;
            } catch (Exception e) {
                throw new RuntimeException("Schema id=" + id + " not found in registry at " + schemaRegistryUrl, e);
            }
        });

        // Skip the Confluent Protobuf message-index array (varint-encoded count + indexes).
        // For a .proto with a single top-level message this encodes as [0x00] (count=0).
        int indexCount = (int) readVarint(buf);
        for (int i = 0; i < indexCount; i++) {
            readVarint(buf);
        }

        // Remaining bytes are the raw Protobuf-serialized UserEvent.
        byte[] protoBytes = new byte[buf.remaining()];
        buf.get(protoBytes);

        out.collect(UserEvent.parseFrom(protoBytes));
    }

    private long readVarint(ByteBuffer buf) {
        long result = 0;
        int shift = 0;
        byte b;
        do {
            b = buf.get();
            result |= (long) (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }

    @Override
    public TypeInformation<UserEvent> getProducedType() {
        return TypeInformation.of(UserEvent.class);
    }
}
