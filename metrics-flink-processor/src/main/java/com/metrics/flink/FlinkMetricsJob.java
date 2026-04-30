package com.metrics.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metrics.flink.function.CdcEnrichmentBroadcastFunction;
import com.metrics.flink.function.MetricWindowAggregator;
import com.metrics.flink.function.MetricWindowProcessFunction;
import com.metrics.flink.model.AggregatedMetric;
import com.metrics.flink.model.CdcEvent;
import com.metrics.flink.model.NormalizedMetric;
import com.metrics.flink.serialization.ProtobufUserEventDeserializer;
import com.metrics.flink.sink.TimescaleDbSink;
import com.metrics.proto.UserEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Properties;

/**
 * Main Flink streaming job. Runs three independent pipelines:
 *
 * <p><b>Pipeline 1 — Structured metrics aggregation:</b>
 * metrics.normalized → CDC enrichment → 1-min tumbling window → TimescaleDB + MinIO Parquet
 *
 * <p><b>Pipeline 2 — Unstructured log archival:</b>
 * logs.raw → rolling text files on HDFS (partitioned by hour) → consumed by Spark batch job
 *
 * <p><b>Pipeline 3 — User events (Protobuf via Schema Registry):</b>
 * users.events → {@link ProtobufUserEventDeserializer} (schema cached per task slot) → log sink
 */
public class FlinkMetricsJob {

    private static final Logger log = LoggerFactory.getLogger(FlinkMetricsJob.class);

    private static final String AGGREGATED_METRIC_AVRO_SCHEMA =
            "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"AggregatedMetric\",\n" +
            "  \"namespace\": \"com.metrics.flink.model\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"metricName\",   \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "    {\"name\": \"serviceId\",    \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "    {\"name\": \"serviceName\",  \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "    {\"name\": \"environment\",  \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "    {\"name\": \"sum\",          \"type\": \"double\", \"default\": 0.0},\n" +
            "    {\"name\": \"min\",          \"type\": \"double\", \"default\": 0.0},\n" +
            "    {\"name\": \"max\",          \"type\": \"double\", \"default\": 0.0},\n" +
            "    {\"name\": \"count\",        \"type\": \"long\",   \"default\": 0},\n" +
            "    {\"name\": \"avg\",          \"type\": \"double\", \"default\": 0.0},\n" +
            "    {\"name\": \"windowStartMs\",\"type\": \"long\",   \"default\": 0},\n" +
            "    {\"name\": \"windowEndMs\",  \"type\": \"long\",   \"default\": 0}\n" +
            "  ]\n" +
            "}";

    public static void main(String[] args) throws Exception {
        Properties props = loadProperties();

        String kafkaBootstrapServers = props.getProperty("kafka.bootstrap.servers", "localhost:9092");
        String kafkaGroupId         = props.getProperty("kafka.consumer.group.id", "flink-metrics-processor");
        String timescaleUrl         = props.getProperty("timescaledb.url", "jdbc:postgresql://localhost:5433/metricsdb");
        String timescaleUser        = props.getProperty("timescaledb.username", "metrics");
        String timescalePass        = props.getProperty("timescaledb.password", "metrics");
        String parquetOutputPath    = props.getProperty("parquet.output.path", "s3://metrics-parquet/aggregated");
        String hdfsLogsOutputPath   = props.getProperty("hdfs.logs.output.path", "hdfs://namenode:9000/metrics/logs/raw");
        String schemaRegistryUrl    = props.getProperty("schema.registry.url", "http://localhost:8081");
        String userEventsTopic      = props.getProperty("users.events.topic", "users.events");
        int parallelism             = Integer.parseInt(props.getProperty("flink.parallelism", "2"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(60_000);

        ObjectMapper objectMapper = new ObjectMapper();

        // -----------------------------------------------------------------------
        // Pipeline 1: Structured metrics  →  windowed aggregation  →  sinks
        // -----------------------------------------------------------------------

        KafkaSource<String> metricsKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics("metrics.normalized")
                .setGroupId(kafkaGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> cdcKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics("cdc.public.service_metadata")
                .setGroupId(kafkaGroupId + "-cdc")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<NormalizedMetric> metricsStream = env
                .fromSource(metricsKafkaSource, WatermarkStrategy.noWatermarks(), "metrics-normalized-source")
                .flatMap((String json, org.apache.flink.util.Collector<NormalizedMetric> out) -> {
                    try {
                        out.collect(objectMapper.readValue(json, NormalizedMetric.class));
                    } catch (Exception e) {
                        log.warn("Failed to deserialize NormalizedMetric: {}", e.getMessage());
                    }
                })
                .returns(NormalizedMetric.class)
                .name("parse-normalized-metrics");

        DataStream<CdcEvent> cdcStream = env
                .fromSource(cdcKafkaSource, WatermarkStrategy.noWatermarks(), "cdc-service-metadata-source")
                .flatMap((String json, org.apache.flink.util.Collector<CdcEvent> out) -> {
                    try {
                        out.collect(objectMapper.readValue(json, CdcEvent.class));
                    } catch (Exception e) {
                        log.warn("Failed to deserialize CdcEvent: {}", e.getMessage());
                    }
                })
                .returns(CdcEvent.class)
                .name("parse-cdc-events");

        DataStream<NormalizedMetric> timedMetricsStream = metricsStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<NormalizedMetric>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((metric, ts) -> metric.getTimestampMs())
                )
                .name("assign-watermarks");

        MapStateDescriptor<String, CdcEvent> cdcStateDescriptor = new MapStateDescriptor<>(
                "cdc-service-metadata-state",
                Types.STRING,
                Types.POJO(CdcEvent.class)
        );

        BroadcastStream<CdcEvent> broadcastCdcStream = cdcStream.broadcast(cdcStateDescriptor);

        DataStream<NormalizedMetric> enrichedStream = timedMetricsStream
                .connect(broadcastCdcStream)
                .process(new CdcEnrichmentBroadcastFunction(cdcStateDescriptor))
                .name("cdc-enrichment");

        SingleOutputStreamOperator<AggregatedMetric> aggregatedStream = enrichedStream
                .keyBy(metric -> metric.getServiceId() + ":" + metric.getMetricName())
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new MetricWindowAggregator(), new MetricWindowProcessFunction())
                .name("windowed-aggregation");

        // Fork 1a: TimescaleDB
        aggregatedStream
                .addSink(new TimescaleDbSink(timescaleUrl, timescaleUser, timescalePass))
                .name("timescaledb-sink");

        // Fork 1b: Parquet to MinIO/S3, partitioned by environment/date
        BulkWriter.Factory<AggregatedMetric> parquetWriterFactory =
                AvroParquetWriters.forReflectRecord(AggregatedMetric.class);

        FileSink<AggregatedMetric> parquetSink = FileSink
                .forBulkFormat(new Path(parquetOutputPath), parquetWriterFactory)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner<AggregatedMetric, String>() {
                    @Override
                    public String getBucketId(AggregatedMetric element, Context context) {
                        String environment = element.getEnvironment() != null ? element.getEnvironment() : "unknown";
                        java.time.LocalDate date = java.time.Instant.ofEpochMilli(element.getWindowStartMs())
                                .atZone(ZoneOffset.UTC).toLocalDate();
                        return environment + "/" + date.toString();
                    }

                    @Override
                    public org.apache.flink.core.io.SimpleVersionedSerializer<String> getSerializer() {
                        return org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners
                                .SimpleVersionedStringSerializer.INSTANCE;
                    }
                })
                .build();

        aggregatedStream
                .sinkTo(parquetSink)
                .name("parquet-minio-sink");

        // -----------------------------------------------------------------------
        // Pipeline 2: Raw unstructured logs  →  rolling text files on HDFS
        //
        // Files are partitioned by year/month/day/hour so the Spark batch job can
        // process one hour's worth of logs at a time:
        //   hdfs://namenode:9000/metrics/logs/raw/year=2024/month=01/day=15/hour=10/logs-*.txt
        //
        // The Spark job (metrics-spark-processor) reads these text files, applies
        // regex parsing, and writes structured Parquet to /metrics/logs/parquet/.
        // -----------------------------------------------------------------------

        KafkaSource<String> rawLogsKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics("logs.raw")
                .setGroupId(kafkaGroupId + "-raw-logs")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawLogsStream = env
                .fromSource(rawLogsKafkaSource, WatermarkStrategy.noWatermarks(), "logs-raw-source")
                .name("raw-logs-stream");

        // Roll every 5 minutes or 128 MB — whichever comes first.
        FileSink<String> hdfsTextSink = FileSink
                .forRowFormat(new Path(hdfsLogsOutputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withInactivityInterval(Duration.ofMinutes(1))
                                .withMaxPartSize(MemorySize.ofMebiBytes(128))
                                .build())
                .withBucketAssigner(
                        new DateTimeBucketAssigner<>("'year='yyyy/'month='MM/'day='dd/'hour='HH", ZoneOffset.UTC))
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("logs")
                                .withPartSuffix(".txt")
                                .build())
                .build();

        rawLogsStream
                .sinkTo(hdfsTextSink)
                .name("hdfs-raw-logs-sink");

        // -----------------------------------------------------------------------
        // Pipeline 3: User events (Protobuf via Schema Registry)
        //
        // The API Gateway publishes Protobuf-encoded UserEvent messages to the
        // users.events topic using KafkaProtobufSerializer, which:
        //   1. Auto-registers the schema in Confluent Schema Registry on first use.
        //   2. Prepends a 5-byte Confluent header (magic byte 0x00 + schema ID).
        //
        // ProtobufUserEventDeserializer reads the schema ID from that header and
        // caches the resolved ParsedSchema per task slot via CachedSchemaRegistryClient.
        // After the first message for a given schema version, all subsequent lookups
        // are served from operator memory — no extra registry calls per message.
        // -----------------------------------------------------------------------

        KafkaSource<UserEvent> userEventsKafkaSource = KafkaSource.<UserEvent>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(userEventsTopic)
                .setGroupId(kafkaGroupId + "-user-events")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new ProtobufUserEventDeserializer(schemaRegistryUrl, 100))
                .build();

        DataStream<UserEvent> userEventsStream = env
                .fromSource(userEventsKafkaSource, WatermarkStrategy.noWatermarks(), "user-events-protobuf-source")
                .name("user-events-stream");

        userEventsStream
                .map(e -> String.format("[UserEvent] userId=%s type=%s service=%s env=%s ts=%d",
                        e.getUserId(), e.getEventType(), e.getServiceId(), e.getEnvironment(), e.getTimestampMs()))
                .returns(String.class)
                .addSink(new org.apache.flink.streaming.api.functions.sink.PrintSinkFunction<>())
                .name("user-events-log-sink");

        env.execute("Metrics Aggregation Job");
    }

    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream is = FlinkMetricsJob.class.getClassLoader()
                .getResourceAsStream("flink-job.properties")) {
            if (is != null) {
                props.load(is);
                log.info("Loaded flink-job.properties from classpath");
            } else {
                log.warn("flink-job.properties not found on classpath, using defaults");
            }
        } catch (Exception e) {
            log.error("Error loading flink-job.properties: {}", e.getMessage(), e);
        }
        return props;
    }
}
