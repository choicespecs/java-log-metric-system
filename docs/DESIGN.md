# Design Decisions

## Decision 1: Dual-path ingestion (structured vs. unstructured)

**Decision:** Accept structured metrics (JSON `MetricEvent`) and raw text logs through separate REST endpoints (`/api/v1/metrics` vs. `/api/v1/logs`) that publish to separate Kafka topics (`metrics.structured` vs. `logs.raw`).

**Rationale:** Structured and unstructured data have fundamentally different latency and fidelity requirements. Structured events are enriched immediately in `metrics-processor` and produce a `NormalizedMetric` within milliseconds. Raw log lines are too high-volume and heterogeneous to parse at ingest time; they are archived as-is and parsed offline. Separating topics allows independent scaling, independent consumer group management, and clear operational boundaries between streaming and batch.

**Trade-off:** A log line that matches the regex in `LogParser` travels through two separate paths (the `RawLogConsumer` in `metrics-processor` publishes to `metrics.normalized` and Flink Pipeline 2 also reads `logs.raw` for HDFS archival). This means a matching log line is processed twice — once for real-time enrichment and once for archival. This is acceptable because the paths serve different purposes (low-latency metric visibility vs. durable long-term storage), but operators should be aware that `logs.raw` has two consumer groups with separate offsets.

---

## Decision 2: Two-stage enrichment (Spring cache + Flink broadcast state)

**Decision:** Enrichment metadata is applied at two stages: once in `metrics-processor` using a Caffeine cache backed by PostgreSQL, and again in Flink using broadcast state populated by Debezium CDC events.

**Rationale:** The Spring-layer enrichment handles the common case cheaply — the Caffeine cache (500 entries, 10-min TTL) absorbs repeated lookups for the same `serviceId` without touching the database. The Flink-layer CDC enrichment handles the update case — if `service_metadata` changes in PostgreSQL, Debezium propagates the change as a CDC event to Flink's broadcast state within seconds, so windowed aggregations pick up fresh metadata without waiting for a Spring cache expiry.

**Trade-off:** There is a brief inconsistency window where `metrics-processor` may use stale cached metadata (up to 10 minutes) while Flink broadcast state has already received a CDC update (or vice versa). For operational dashboards this window is acceptable. If strict consistency is required, the Spring cache TTL should be reduced or CDC should become the sole enrichment path.

---

## Decision 3: Flink for streaming, Spark for batch log structuring

**Decision:** Raw log parsing and Parquet structuring are handled by a Spark batch job (`SparkLogProcessor`) rather than inline in Flink Pipeline 2.

**Rationale:** Log volumes are bursty and irregular. Parsing in Flink would add per-record CPU overhead on the streaming critical path and complicate checkpointing. The Flink FileSink writes bounded hourly partitions (`year/month/day/hour`) to HDFS, giving Spark well-defined input boundaries. Spark's columnar execution engine and parallel partition reads are better suited to the bulk regex-plus-write workload. The 5-minute rolling policy ensures hourly Spark jobs can process recent-but-complete data.

**Trade-off:** Log-level Parquet data is available with a lag equal to the Spark job's schedule interval (typically 1–6 hours, depending on operator configuration). Real-time log queries are not possible against the Parquet store. If sub-hour latency is required, the regex parsing should be moved into a Flink `flatMap` operator and the Parquet output written directly from Flink.

---

## Decision 4: TimescaleDB for recent operational queries, Parquet for long-term analytics

**Decision:** Flink Pipeline 1 fans out aggregated metrics to two sinks: TimescaleDB (via `TimescaleDbSink` JDBC) and MinIO Parquet (via `FileSink`).

**Rationale:** TimescaleDB's hypertable on `window_start` provides sub-second range queries for dashboards covering hours or days. The table receives upserts (`ON CONFLICT DO UPDATE`) so re-submitted Flink jobs do not produce duplicate rows. Parquet files in MinIO are immutable, compressible, and queryable by Spark or any Hive-compatible engine for analytical workloads spanning weeks or months where TimescaleDB retention policies would have purged the data.

**Trade-off:** Flink holds open a single JDBC connection per task slot in `TimescaleDbSink` (`connection.setAutoCommit(true)`). Under high parallelism this creates many connections to TimescaleDB. A connection pool (e.g., HikariCP) or a batched JDBC sink (using Flink's `JdbcSink`) would reduce connection pressure and increase write throughput at the cost of increased per-record latency.

---

## Decision 5: Kafka producer key strategy

**Decision:** `metrics.structured` and `metrics.normalized` use `serviceId` as the Kafka message key. `logs.raw` uses `source` as the key.

**Rationale:** Keying by `serviceId` ensures all events for a given service land on the same partition, preserving per-service ordering and enabling Flink to key by `serviceId:metricName` without cross-partition shuffles. Using `source` for raw logs groups log lines from the same producer together, making HDFS file contents homogeneous within a partition.

**Trade-off:** Hot services with high metric volume will overload the partition assigned to their `serviceId`. Partitioning by a composite key (e.g., `serviceId + metricName` hash) or increasing topic partition count would reduce hot-spot risk.

---

## Decision 6: Fat JARs for Flink and Spark modules

**Decision:** `metrics-flink-processor` and `metrics-spark-processor` are packaged as fat JARs (Maven Shade / Spring Boot repackage) rather than thin JARs with separate dependency resolution.

**Rationale:** Flink and Spark cluster environments do not have Maven available at runtime. Fat JARs are self-contained and submit cleanly with `flink run -c` and `spark-submit --class`. This avoids classpath management between the cluster's built-in libraries and application dependencies.

**Trade-off:** Fat JARs are large (100MB+ for Spark due to bundled Hadoop and Spark core). Builds are slower. The standard mitigation — `--conf spark.jars.packages` or marking Spark/Flink dependencies as `provided` scope — was not applied here, meaning the JAR bundles libraries already present on the cluster. This works but wastes bandwidth and risks version conflicts.

---

## Decision 7: No schema enforcement at Kafka layer

**Decision:** All Kafka messages are serialised as plain JSON strings (`StringSerializer` / `StringDeserializer`). Schema Registry is deployed but not used for topic schema enforcement.

**Rationale:** Plain JSON lowers the barrier to entry for producers. The Schema Registry container is present for future Avro/Protobuf schema management, and the Flink module already bundles `flink-parquet` and `avro` for Parquet output.

**Trade-off:** Without schema enforcement, a malformed or schema-breaking `MetricEvent` published to `metrics.structured` will cause a `JsonProcessingException` in `StructuredMetricsConsumer` and be silently dropped (logged as an error but not dead-lettered). Adding a dead-letter topic and wiring the Schema Registry for topic validation would improve data quality and observability.

---

## Decision 8: Raw SQL in AggregatedMetricRepository

**Decision:** `AggregatedMetricRepository` uses `JdbcTemplate` with hand-written SQL rather than Spring Data JPA.

**Rationale:** TimescaleDB-specific features (`time_bucket`, continuous aggregates, compression policies) require SQL constructs that JPA's JPQL cannot express. Using `JdbcTemplate` allows direct use of `TIMESTAMPTZ` comparison operators and sets the pattern for future adoption of `time_bucket` functions without having to escape JPA's abstraction layer.

**Trade-off:** Raw SQL is harder to refactor when column names change. The mapping in `mapRowToMetricQueryResult` must be kept in sync with the DDL manually.

---

## Identified Technical Debt

| Item | Severity | Location | Suggested Resolution |
|---|---|---|---|
| `TimescaleDbSink` opens one bare JDBC connection per task slot with no pooling | Medium | `metrics-flink-processor/TimescaleDbSink.java` | Replace with `JdbcSink.exactlyOnceSink()` or add HikariCP pooling |
| `metrics-structured` and `metrics-normalized` use `serviceId` as partition key — hot-service overloading risk | Medium | `metrics-collector/MetricsPublisherService.java` | Hash `serviceId + metricName` or increase partition count |
| Spark and Flink fat JARs bundle `provided`-scope cluster libraries | Low | `metrics-flink-processor/pom.xml`, `metrics-spark-processor/pom.xml` | Mark `flink-streaming-java`, `spark-core` etc. as `<scope>provided</scope>` |
| `MetricWindowAggregator.createAccumulator()` initialises `min` to `Double.MAX_VALUE` and `max` to `Double.MIN_VALUE` — sentinel values leak through if `getResult()` receives a zero-count accumulator | Low | `metrics-flink-processor/MetricWindowAggregator.java` | The `getResult()` reset to `0.0` guards against this — downstream consumers should treat `count=0` as no-data |
| No dead-letter topic for malformed Kafka messages | Medium | `metrics-processor/StructuredMetricsConsumer.java`, `RawLogConsumer.java` | Route parse failures to a `metrics.dlq` topic |
| `ParquetQueryService` provides only file listing, not Parquet content queries | Low | `metrics-api-gateway/ParquetQueryService.java` | Integrate Apache Arrow Flight or DuckDB for in-process Parquet reads |
| `minio-init` sets `public` policy on `metrics-parquet` bucket | Medium | `docker-compose.yml` | Remove public bucket policy; use IAM roles or pre-signed URLs |
| `TimescaleDB` DDL (create hypertable) must be run manually | Low | `metrics-flink-processor/TimescaleDbSink.java` (Javadoc) | Add a Flyway migration module or a dedicated init container for TimescaleDB |
| Watermark strategy on `metricsStream` is set to `noWatermarks()` initially then overwritten — correct but confusing | Low | `metrics-flink-processor/FlinkMetricsJob.java` | Set `WatermarkStrategy.forBoundedOutOfOrderness(...)` directly on the `fromSource` call |
| `ENABLE_AUTO_COMMIT_CONFIG: true` in `metrics-processor/KafkaConfig` — offsets committed before successful downstream publish | Medium | `metrics-processor/KafkaConfig.java` | Switch to manual offset commit (`AckMode.MANUAL`) after successful `kafkaTemplate.send()` |
