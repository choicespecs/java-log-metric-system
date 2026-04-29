# Data Flow

## End-to-End Pipeline Overview

The system operates two parallel pipelines that diverge at ingestion and converge only at query time:

- **Structured metrics pipeline** — JSON metric events flow from collector → processor (enrichment) → Flink (windowed aggregation) → TimescaleDB + MinIO Parquet.
- **Unstructured log pipeline** — raw text log lines flow from collector → Flink (file archival to HDFS) → Spark (regex structuring → Parquet).

A third, orthogonal CDC stream keeps enrichment metadata in sync across both the Spring-based processor and the Flink broadcast state.

## Structured Metrics Flow

```mermaid
sequenceDiagram
    participant Client
    participant Collector as metrics-collector :8081
    participant KMS as Kafka: metrics.structured
    participant Processor as metrics-processor :8082
    participant PG as PostgreSQL :5432
    participant KMN as Kafka: metrics.normalized
    participant Flink as Flink: Pipeline 1
    participant TS as TimescaleDB :5433
    participant MN as MinIO :9000

    Client->>Collector: POST /api/v1/metrics (MetricEvent JSON)
    Collector->>Collector: @Valid Bean Validation
    Collector->>KMS: produce(key=serviceId, value=JSON)
    KMS-->>Processor: poll (group: metrics-processor)
    Processor->>PG: SELECT service_metadata WHERE service_id=?
    PG-->>Processor: ServiceMetadata (cached via Caffeine 10min)
    Processor->>Processor: EnrichmentService.enrich() to NormalizedMetric
    Processor->>KMN: produce(key=serviceId, value=JSON)
    KMN-->>Flink: poll (group: flink-metrics-processor)
    Flink->>Flink: assignTimestampsAndWatermarks (5s out-of-order)
    Flink->>Flink: connect() BroadcastStream (CDC enrichment)
    Flink->>Flink: keyBy(serviceId:metricName)
    Flink->>Flink: TumblingEventTimeWindows(1 min)
    Flink->>Flink: MetricWindowAggregator to AggregatedMetric
    Flink->>Flink: MetricWindowProcessFunction: attach windowStart/windowEnd
    Flink->>TS: UPSERT metric_aggregates ON CONFLICT (metric_name, service_id, window_start)
    Flink->>MN: FileSink Parquet bucket=environment/date (OnCheckpointRollingPolicy)
```

## Unstructured Log Flow

```mermaid
sequenceDiagram
    participant Client
    participant Collector as metrics-collector :8081
    participant KLR as Kafka: logs.raw
    participant Flink as Flink: Pipeline 2
    participant HDFS as HDFS /metrics/logs/raw
    participant Spark as Spark SparkLogProcessor
    participant HDFSPq as HDFS /metrics/logs/parquet

    Client->>Collector: POST /api/v1/logs (text/plain, ?source=...)
    Collector->>KLR: produce(key=source, value=rawLine)
    KLR-->>Flink: poll (group: flink-metrics-processor-raw-logs)
    Flink->>Flink: DataStream of String (no watermarks)
    Flink->>HDFS: FileSink rolling .txt year=YYYY/month=MM/day=DD/hour=HH/logs-*.txt roll every 5min OR 128MB OR 1min inactivity
    Note over Spark: Batch job (periodic schedule)
    Spark->>HDFS: spark.read().option("recursiveFileLookup","true").text(inputPath)
    Spark->>Spark: regexp_extract: timestamp, level, service_id, message, raw_line
    Spark->>Spark: filter(timestamp != "")
    Spark->>Spark: withColumn("date", to_date(timestamp))
    Spark->>HDFSPq: write Parquet partitionBy("level","date") SaveMode.Append
```

## CDC Enrichment Flow

```mermaid
sequenceDiagram
    participant PG as PostgreSQL :5432 (wal_level=logical)
    participant DZ as Debezium/kafka-connect :8083
    participant KCDC as Kafka: cdc.public.service_metadata
    participant Flink as Flink: CdcEnrichmentBroadcastFunction
    participant BS as BroadcastState MapState serviceId to CdcEvent

    PG->>DZ: WAL replication slot (logical decoding)
    DZ->>KCDC: CDC event {op, serviceId, serviceName, team, environment, region}
    KCDC-->>Flink: processBroadcastElement()
    alt op = "c" or "u" or "r"
        Flink->>BS: broadcastState.put(serviceId, cdcEvent)
    else op = "d"
        Flink->>BS: broadcastState.remove(serviceId)
    end
    Note over Flink: On each NormalizedMetric event
    Flink->>BS: broadcastState.get(serviceId)
    BS-->>Flink: CdcEvent (or null)
    Flink->>Flink: overwrite serviceName/team/environment/region if found
```

## Query Flow

```mermaid
sequenceDiagram
    participant Client
    participant GW as metrics-api-gateway :8083
    participant TS as TimescaleDB :5433
    participant MN as MinIO :9000

    alt Time-series query (recent aggregates)
        Client->>GW: GET /api/v1/query/timeseries?metricName=&serviceId=&from=&to=
        GW->>GW: TimeSeriesQueryService.validateTimeRange()
        GW->>TS: SELECT metric_name,service_id,avg_value,... FROM metric_aggregates WHERE window_start>=? AND window_end<=? ORDER BY window_start ASC
        TS-->>GW: List of MetricQueryResult
        GW-->>Client: 200 JSON array

        Client->>GW: GET /api/v1/query/series?metricName=&serviceId=&from=&to=
        GW->>TS: SELECT window_start, avg_value FROM metric_aggregates WHERE ...
        TS-->>GW: List of TimeSeriesPoint
        GW-->>Client: 200 JSON array
    else Parquet file listing (long-term analytics)
        Client->>GW: GET /api/v1/query/files?prefix=aggregated/production/2024-01-15
        GW->>MN: ListObjectsArgs(bucket=metrics-parquet, prefix=..., recursive=true)
        MN-->>GW: Iterable of Result Item
        GW->>GW: filter .parquet / .snappy.parquet
        GW-->>Client: 200 JSON array of object names
    end
```

## Data Models

### MetricEvent (ingestion payload)

```mermaid
erDiagram
    MetricEvent {
        String metricName "NotBlank"
        double value
        String unit
        String serviceId "NotBlank"
        Map_String_String tags "nullable"
        Instant timestamp "NotNull"
    }
```

### NormalizedMetric (enriched event on metrics.normalized)

```mermaid
erDiagram
    NormalizedMetric {
        String metricName
        double value
        String unit
        String serviceId
        String serviceName "from ServiceMetadata"
        String team "from ServiceMetadata"
        String environment "from ServiceMetadata"
        String region "from ServiceMetadata"
        Map_String_String tags
        String parsedLevel "log events only"
        String logMessage "log events only"
        Instant timestamp
        long timestampMs "epoch ms for Flink watermarks"
    }
```

### AggregatedMetric (Flink window output)

```mermaid
erDiagram
    AggregatedMetric {
        String metricName
        String serviceId
        String serviceName
        String environment
        double sum
        double min
        double max
        long count
        double avg
        long windowStartMs "epoch ms"
        long windowEndMs "epoch ms"
    }
```

### ServiceMetadata (PostgreSQL, Flyway-managed)

```mermaid
erDiagram
    service_metadata {
        VARCHAR_128 service_id PK
        VARCHAR_256 service_name "NOT NULL"
        VARCHAR_128 team
        VARCHAR_64 environment
        VARCHAR_64 region
    }
```

### metric_aggregates (TimescaleDB hypertable)

```mermaid
erDiagram
    metric_aggregates {
        TEXT metric_name PK
        TEXT service_id PK
        TIMESTAMPTZ window_start PK
        TEXT service_name
        TEXT environment
        DOUBLE_PRECISION sum_value
        DOUBLE_PRECISION min_value
        DOUBLE_PRECISION max_value
        BIGINT count_value
        DOUBLE_PRECISION avg_value
        TIMESTAMPTZ window_end
    }
```

The `metric_aggregates` table is converted to a TimescaleDB hypertable on `window_start` via `SELECT create_hypertable('metric_aggregates', 'window_start', if_not_exists => TRUE)`. This DDL must be run manually before submitting the Flink job (not managed by Flyway).

### CdcEvent (cdc.public.service_metadata topic payload)

```mermaid
erDiagram
    CdcEvent {
        String op "c=create u=update d=delete r=snapshot"
        String serviceId
        String serviceName
        String team
        String environment
        String region
    }
```

## Log Parsing Pattern

Both `LogParser` (metrics-processor) and `SparkLogProcessor` (metrics-spark-processor) use the same regex pattern:

```
(\d{4}-\d{2}-\d{2}T[\d:.]+Z)\s+\[(\w+)]\s+\[([^\]]+)]\s+(.*)
```

Group mapping:
1. ISO-8601 timestamp (e.g. `2024-01-15T10:30:00.123Z`)
2. Log level (e.g. `INFO`, `ERROR`)
3. Service ID (e.g. `svc-api`)
4. Message text

Lines that do not match are silently dropped. In `LogParser` they return `Optional.empty()`; in Spark they are filtered out via `filter(timestamp != "")`.

## HDFS Partition Layout

Raw log files written by Flink (Pipeline 2):
```
hdfs://namenode:9000/metrics/logs/raw/
  year=2024/month=01/day=15/hour=10/
    logs-<uuid>.txt
    logs-<uuid>.txt.inprogress
```

Structured Parquet files written by Spark:
```
hdfs://namenode:9000/metrics/logs/parquet/
  level=INFO/date=2024-01-15/
    part-00000-*.parquet
  level=ERROR/date=2024-01-15/
    part-00000-*.parquet
```

## MinIO Object Layout

Aggregated Parquet files written by Flink (Pipeline 1):
```
s3://metrics-parquet/aggregated/
  production/2024-01-15/
    part-0-<uuid>.parquet
  staging/2024-01-15/
    part-0-<uuid>.parquet
```

Bucket `metrics-parquet` is created with public read policy by the `minio-init` container at startup. Parquet files roll on every Flink checkpoint (`OnCheckpointRollingPolicy`).
