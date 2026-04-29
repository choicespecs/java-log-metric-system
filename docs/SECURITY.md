# Security

## Authentication and Authorisation

### Current State

The system has **no application-level authentication or authorisation** implemented. All HTTP endpoints in all three Spring Boot services (`metrics-collector`, `metrics-processor`, `metrics-api-gateway`) are open without any authentication mechanism.

There are no Spring Security dependencies in any module's `pom.xml`. No JWT validation, API key checks, OAuth2 scopes, or mutual TLS are configured.

**Severity: HIGH** — In any environment reachable by untrusted clients, all endpoints must be protected.

### Kafka Authentication

Kafka is configured with `PLAINTEXT` listeners only:

```
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
```

There is no SASL, SSL/TLS, or ACL configuration. Any client that can reach port 9092 can produce to or consume from any topic.

**Severity: HIGH** — In production, Kafka should use `SASL_SSL` with ACLs per consumer group and topic.

### MinIO Authentication

MinIO uses default root credentials (`minioadmin` / `minioadmin`) stored in `docker-compose.yml` and `flink-job.properties`. The `metrics-parquet` bucket is created with a **public read policy** by `minio-init`.

```yaml
environment:
  MINIO_ROOT_USER: minioadmin
  MINIO_ROOT_PASSWORD: minioadmin
```

**Severity: CRITICAL** — Default credentials and a public bucket policy must not be used in any environment accessible beyond localhost. Rotate credentials and replace the bucket policy with IAM-scoped access.

### PostgreSQL / TimescaleDB Authentication

Both databases use `metrics` / `metrics` credentials defined in `docker-compose.yml` and Spring Boot `application.yml`. PostgreSQL is exposed on `localhost:5432`, TimescaleDB on `localhost:5433`.

**Severity: HIGH** — Credentials should be injected via environment variables or a secrets manager (Vault, AWS SSM) rather than hardcoded in property files.

### Debezium / Kafka Connect

Kafka Connect (Debezium) is exposed on port 8083. The REST API accepts connector configuration without authentication. The PostgreSQL replication user is the same `metrics` user.

**Severity: MEDIUM** — The Kafka Connect REST API should be restricted to internal network access only and, in production, should require basic auth or mTLS.

---

## Hardcoded Secrets Inventory

| File | Secret | Value | Severity |
|---|---|---|---|
| `docker-compose.yml` | MinIO root user | `minioadmin` | CRITICAL |
| `docker-compose.yml` | MinIO root password | `minioadmin` | CRITICAL |
| `docker-compose.yml` | PostgreSQL password | `metrics` | HIGH |
| `docker-compose.yml` | TimescaleDB password | `metrics` | HIGH |
| `metrics-processor/src/main/resources/application.yml` | PostgreSQL password | `metrics` | HIGH |
| `metrics-api-gateway/src/main/resources/application.yml` | PostgreSQL (TimescaleDB) password | `metrics` | HIGH |
| `metrics-api-gateway/src/main/resources/application.yml` | MinIO access key | `minioadmin` | CRITICAL |
| `metrics-api-gateway/src/main/resources/application.yml` | MinIO secret key | `minioadmin` | CRITICAL |
| `metrics-flink-processor/src/main/resources/flink-job.properties` | TimescaleDB password | `metrics` | HIGH |
| `metrics-flink-processor/src/main/resources/flink-job.properties` | MinIO access key | `minioadmin` | CRITICAL |
| `metrics-flink-processor/src/main/resources/flink-job.properties` | MinIO secret key | `minioadmin` | CRITICAL |

**Total hardcoded secret occurrences: 11**

All secrets in `application.yml` and `.properties` files should be replaced with `${ENV_VAR_NAME}` references and supplied at runtime via environment variables, Docker secrets, or a secrets manager.

---

## Input Entry Points and Validation

### metrics-collector

| Endpoint | Method | Content-Type | Validation |
|---|---|---|---|
| `/api/v1/metrics` | POST | `application/json` | `@Valid` on `MetricEvent`: `@NotBlank metricName`, `@NotBlank serviceId`, `@NotNull timestamp` |
| `/api/v1/metrics/batch` | POST | `application/json` | `@Valid` on `List<MetricEvent>` — each element validated |
| `/api/v1/logs` | POST | `text/plain` | No validation — raw string accepted as-is, trimmed |
| `/api/v1/logs/batch` | POST | `application/json` | No validation — `List<String>` accepted as-is |

**Issue:** Log endpoints have no size limits. A client can submit arbitrarily large payloads. Spring Boot's default `max-http-request-header-size` applies but no `spring.servlet.multipart.max-request-size` or body-size limit is configured.

**Severity: MEDIUM** — Add `spring.mvc.servlet.max-request-size` or a `ContentCachingRequestWrapper` filter with a maximum payload size.

### metrics-api-gateway

| Endpoint | Method | Validation |
|---|---|---|
| `/api/v1/query/timeseries` | GET | `@DateTimeFormat(iso=DATE_TIME)` on `from`/`to`; `validateTimeRange()` checks `from.isBefore(to)` |
| `/api/v1/query/series` | GET | Same as above |
| `/api/v1/query/files` | GET | `prefix` is optional, defaults to `""`. Passed directly to MinIO `ListObjectsArgs`. No sanitisation. |

**Issue:** The `prefix` parameter in `/api/v1/query/files` is passed unsanitised to the MinIO SDK. A path-traversal-style prefix (e.g., `../`) could be used to enumerate unexpected bucket paths. While MinIO resolves these to safe object keys, the parameter should be validated to only contain safe characters.

**Severity: LOW** — Add a regex whitelist (alphanumeric, `/`, `-`, `.`, `_`) on the `prefix` parameter.

---

## Network Exposure

The `docker-compose.yml` exposes the following ports to the host:

| Port | Service | Risk |
|---|---|---|
| 9092 | Kafka | Plaintext access to all topics — HIGH |
| 8081 | Schema Registry | Unauthenticated schema API — MEDIUM |
| 8083 | Kafka Connect (Debezium) | Unauthenticated connector management — MEDIUM |
| 5432 | PostgreSQL | Direct DB access — HIGH |
| 5433 | TimescaleDB | Direct DB access — HIGH |
| 9000 | MinIO API | Public bucket, default creds — CRITICAL |
| 9001 | MinIO Console | Web console with default creds — CRITICAL |
| 9870 | HDFS NameNode Web UI | Unauthenticated read access to HDFS metadata — MEDIUM |
| 8888 | Spark Web UI | Unauthenticated — LOW |
| 7077 | Spark Master | Unauthenticated job submission — HIGH |
| 8080 | Flink JobManager Web UI | Unauthenticated — LOW |
| 8090 | Kafka UI | Unauthenticated full topic access — HIGH |

In a production deployment, all ports except the application ports (8081, 8082, 8083) should be restricted to internal network access or bound to localhost only (`127.0.0.1:PORT:PORT`).

---

## Known Security Issues by Severity

### CRITICAL

1. **Default MinIO credentials** (`minioadmin`/`minioadmin`) in all config files. Replace with strong, randomly generated credentials injected via secrets manager.
2. **Public MinIO bucket policy** — `metrics-parquet` is set to world-readable by `minio-init`. Remove the public policy; use pre-signed URLs for external access.

### HIGH

3. **No HTTP authentication** on any of the three Spring Boot services. All endpoints are open.
4. **Kafka plaintext transport** — all producers and consumers communicate without TLS.
5. **Hardcoded database passwords** in `application.yml` and `flink-job.properties`.
6. **Spark master port 7077 exposed** — allows unauthenticated job submission that can execute arbitrary code on the Spark cluster.
7. **Kafka UI exposed on 8090** — provides full topic read/write access without authentication.

### MEDIUM

8. **No request body size limit** on log ingestion endpoints.
9. **Debezium Kafka Connect REST API unauthenticated** — allows adding/removing connectors without access control.
10. **Kafka Connect and Schema Registry on public ports** — should be internal-only.
11. **`ENABLE_AUTO_COMMIT_CONFIG: true`** in `metrics-processor/KafkaConfig` — auto-commit means a message is marked consumed before it is successfully processed, risking message loss on crash.

### LOW

12. **Unsanitised `prefix` query parameter** in `/api/v1/query/files`.
13. **`flink-job.properties` and `spark-job.properties` bundled in the fat JAR** — secrets inside the JAR are readable by anyone with access to the artifact.
14. **No structured error responses** — stack traces may be leaked in Spring's default error response body in development mode.

---

## Recommendations

1. Add Spring Security to all three Spring Boot modules. Use API key authentication (simple) or OAuth2 resource server (production-grade).
2. Replace all hardcoded credentials with `${ENV_VAR}` references. Use Docker secrets or Kubernetes secrets at deployment time.
3. Enable Kafka SASL/SSL and configure ACLs: collector can only produce to `metrics.structured` and `logs.raw`; processor can only consume those and produce to `metrics.normalized`.
4. Remove the public MinIO bucket policy. Use pre-signed URLs for any external download needs.
5. Bind infrastructure ports (5432, 5433, 9092, 7077, 8090, 9001) to `127.0.0.1` in `docker-compose.yml` for non-production deployments, and remove them entirely for production (use an ingress or service mesh).
6. Switch `ENABLE_AUTO_COMMIT_CONFIG` to `false` in `metrics-processor` and use manual offset commit after successful downstream publish.
7. Add a `Content-Length` or body-size filter to `metrics-collector` to cap log payload sizes.
