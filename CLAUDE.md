# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Distributed metrics logging and aggregation system using Apache Kafka, Apache Flink, and Spring Boot. Ingests both structured (JSON, Protobuf, Avro) and unstructured (raw text logs, binary) data at high volume, processes it through streaming pipelines, and lands analytical output as Parquet files in object storage.

**Prerequisites:** JDK 17+, Docker & Docker Compose, Apache Maven 3.8+, 8GB+ RAM

## Build & Run Commands

```bash
# Start all infrastructure (Kafka, Flink, Hadoop, Spark, Debezium, MinIO, TimescaleDB, etc.)
docker-compose up -d

# Build all modules
mvn clean package

# Build a single module
mvn clean package -pl metrics-collector

# Run a Spring Boot service
cd metrics-collector && mvn spring-boot:run
cd metrics-processor && mvn spring-boot:run
cd metrics-api-gateway && mvn spring-boot:run

# Run all tests
mvn test

# Run tests for a single module
mvn test -pl metrics-collector

# Run a single test class
mvn test -pl metrics-collector -Dtest=MetricsCollectorServiceTest

# Submit Flink job (runs all three pipelines: structured metrics, raw log archival, Protobuf user events)
cd metrics-flink-processor
mvn clean package
flink run -c com.metrics.flink.FlinkMetricsJob target/flink-processor-1.0.0.jar

# Submit Spark batch job (converts HDFS raw log text files to Parquet)
cd metrics-spark-processor
mvn clean package
spark-submit \
  --master spark://localhost:7077 \
  --class com.metrics.spark.SparkLogProcessor \
  target/metrics-spark-processor-1.0.0-SNAPSHOT.jar \
  hdfs://namenode:9000/metrics/logs/raw \
  hdfs://namenode:9000/metrics/logs/parquet
```

## TimescaleDB DDL (run once before submitting the Flink job)

The `metric_aggregates` hypertable must be created manually — it is not managed by Flyway:

```sql
CREATE TABLE IF NOT EXISTS metric_aggregates (
    metric_name   TEXT NOT NULL,
    service_id    TEXT NOT NULL,
    service_name  TEXT,
    environment   TEXT,
    sum_value     DOUBLE PRECISION,
    min_value     DOUBLE PRECISION,
    max_value     DOUBLE PRECISION,
    count_value   BIGINT,
    avg_value     DOUBLE PRECISION,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_name, service_id, window_start)
);
SELECT create_hypertable('metric_aggregates', 'window_start', if_not_exists => TRUE);
```

Connect to TimescaleDB: `psql -h localhost -p 5433 -U metrics -d metricsdb`

## System Design

### Full Data Flow

See `docs/FLOW.md` for sequence diagrams. Summary:

```
Structured Events (JSON/Avro)  -->  metrics.structured
                                        |
                                  metrics-processor  -->  metrics.normalized  -->  Flink Pipeline 1  -->  TimescaleDB
                                        |                                                               -->  MinIO (Parquet)
                                   PostgreSQL
                                   (enrichment)

Unstructured Logs / Text  -->  logs.raw  -->  Flink Pipeline 2  -->  HDFS rolling .txt files
                                                                           |
                                                                    Spark batch job
                                                                           |
                                                                    HDFS Parquet

CDC Events (Debezium)  -->  cdc.public.service_metadata  -->  Flink broadcast state (enrichment)

User Events (Protobuf)  -->  API Gateway  -->  Schema Registry (auto-register)
                                 |
                          users.events (Kafka, Confluent wire format: 5-byte header + Protobuf)
                                 |
                          Flink Pipeline 3  -->  ProtobufUserEventDeserializer
                                                  (CachedSchemaRegistryClient + operator Map cache)
```

### Modules

| Module | Role |
|--------|------|
| `metrics-proto` | Shared Protobuf schema module — compiles `user_event.proto` to Java via `protobuf-maven-plugin`; both gateway and Flink depend on this |
| `metrics-collector` | Spring Boot ingestion service — `POST /api/v1/metrics` for structured events, `POST /api/v1/logs` for raw text; publishes to Kafka |
| `metrics-processor` | Spring Boot Kafka consumer — enriches structured events from PostgreSQL and forwards to `metrics.normalized` |
| `metrics-flink-processor` | Apache Flink job — three pipelines (see below); deployed via `flink run`, not a Spring Boot app |
| `metrics-spark-processor` | Spark batch job — reads HDFS text files, parses with regex, writes Parquet; submitted via `spark-submit` |
| `metrics-api-gateway` | Spring Boot query layer + Protobuf producer — time-series queries on TimescaleDB, Parquet file listing from MinIO, `POST /api/v1/users/events` publishes Protobuf to `users.events` |

The Flink and Spark modules are fat JARs submitted to their respective clusters. All infrastructure is managed via `docker-compose`.

### Flink Job — Three Pipelines

**Pipeline 1 (structured metrics):**
`metrics.normalized` → CDC broadcast enrichment → 1-min tumbling event-time window → TimescaleDB upsert + MinIO Parquet

**Pipeline 2 (unstructured log archival):**
`logs.raw` → `FileSink` with `DefaultRollingPolicy` (5-min roll / 128 MB) → HDFS path `hdfs://namenode:9000/metrics/logs/raw/year=.../month=.../day=.../hour=.../logs-*.txt`

Files roll every 5 minutes or 128 MB so the Spark job processes bounded hourly batches.

**Pipeline 3 (Protobuf user events via Schema Registry):**
`users.events` → `ProtobufUserEventDeserializer` → log sink

The deserializer reads the Confluent wire-format header (5 bytes: magic + schema ID), fetches the schema from Confluent Schema Registry via `CachedSchemaRegistryClient` (one HTTP call per schema version), and caches `ParsedSchema` objects in a per-task-slot `Map<Integer, ParsedSchema>`. All subsequent messages for the same schema ID are deserialized from the operator's in-memory cache — no registry calls in the hot path.

### Key Design Concerns

**High-volume ingestion**
Kafka is the primary buffer. Topics should be partitioned by metric type or source ID to enable parallel consumer groups. Backpressure from Flink is handled via Kafka consumer lag — do not block the collector thread.

**Unstructured data (text logs)**
Raw log lines arrive at `metrics-collector` and are published as-is to `logs.raw` — no parsing at ingest time. Flink Pipeline 2 consumes `logs.raw` and writes rolling `.txt` files to HDFS under a `year/month/day/hour` partition layout. The Spark batch job (`SparkLogProcessor`) then reads those files, applies regex parsing, and writes structured Parquet to `hdfs://namenode:9000/metrics/logs/parquet/` partitioned by `level/date`.

**Time-series and windowed aggregations**
Flink handles all windowing — tumbling and sliding windows over event time with watermarks for late-arrival tolerance. Aggregated results are written to TimescaleDB for operational queries (short retention) and to Parquet files for long-term analytical queries.

**Analytical data sink (Parquet)**
The Flink `FileSink` writes Parquet-encoded output to MinIO (S3-compatible). Files are partitioned by `environment/date`. Use Apache Iceberg or Hive metastore for schema evolution if the analytical layer grows.

**CDC with enrichment**
Debezium captures row-level changes from the PostgreSQL operational database and publishes them to Kafka CDC topics. The Flink job maintains a broadcast state of enrichment tables (e.g., service metadata, user dimensions) and joins incoming metric events against that state before aggregation.

**Protobuf + Schema Registry (Pipeline 3)**
User events flow from the API Gateway as Protobuf — binary encoding with no field names on the wire. The Confluent Schema Registry sits between the Gateway (`KafkaProtobufSerializer`, auto-registers on first publish) and Flink (`ProtobufUserEventDeserializer`, fetches schema by ID and caches per task slot). The shared `UserEvent` proto is defined in `metrics-proto/src/main/proto/user_event.proto` and compiled to Java by `protobuf-maven-plugin` during `mvn package`.

**Databases and storage**
- **PostgreSQL** — operational store; source for CDC via Debezium; holds enrichment/reference data (`service_metadata`)
- **TimescaleDB** — time-series store for pre-aggregated metrics; `metric_aggregates` is a hypertable; used by the API gateway for low-latency range queries
- **MinIO** — S3-compatible object storage for Parquet files from the structured metrics pipeline
- **HDFS** — stores raw log text files (from Flink) and structured log Parquet files (from Spark); NameNode web UI at `localhost:9870`; Spark Web UI at `localhost:8888`
- **Confluent Schema Registry** — stores Protobuf schema for `users.events`; accessed by the API Gateway to register and by Flink to fetch/cache

## Documentation

Full documentation lives in `docs/`:

| File | Contents |
|------|----------|
| `docs/ARCHITECTURE.md` | Module map, system diagram, infrastructure services, Kafka topics |
| `docs/FLOW.md` | Sequence diagrams for all four data flows, data models, storage layouts |
| `docs/DESIGN.md` | Eight design decisions with rationale/trade-offs, technical debt inventory |
| `docs/SECURITY.md` | Auth gaps, hardcoded secrets inventory, network exposure, recommendations |

## Web UIs (after `docker-compose up -d`)

| UI | URL |
|----|-----|
| Flink JobManager | http://localhost:8080 |
| Kafka UI | http://localhost:8090 |
| MinIO Console | http://localhost:9001 |
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8888 |
| Schema Registry | http://localhost:8081 |
