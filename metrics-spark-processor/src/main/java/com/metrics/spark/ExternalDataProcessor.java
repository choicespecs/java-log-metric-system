package com.metrics.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Spark batch job: reads NDJSON files written by Flink Pipeline 4 from HDFS,
 * infers schema from the raw JSON payloads, flattens nested structures, normalises
 * column names, and writes structured output to one or more sinks.
 *
 * <p>Input file format (one JSON object per line):
 * <pre>
 * {"_source_api":"payments-api","_received_at":1714435200000,"_data":{"id":1,"amount":42.50}}
 * {"_source_api":"payments-api","_received_at":1714435260000,"_data":{"id":2,"amount":18.00}}
 * </pre>
 *
 * <p>Spark reads these files with {@code spark.read.json()}, which infers the schema
 * automatically — no Protobuf, Avro, or any pre-declared schema is required.
 *
 * <p>Typical invocation:
 * <pre>
 * spark-submit \
 *   --master spark://spark-master:7077 \
 *   --class com.metrics.spark.ExternalDataProcessor \
 *   target/metrics-spark-processor-1.0.0-SNAPSHOT.jar \
 *   hdfs://namenode:9000/metrics/external/raw \
 *   hdfs://namenode:9000/metrics/external/parquet \
 *   [--output-s3 s3a://metrics-bucket/external/parquet] \
 *   [--output-jdbc jdbc:postgresql://timescale:5433/metricsdb \
 *    --jdbc-user metrics --jdbc-password metrics \
 *    --jdbc-table external_events --jdbc-mode append]
 * </pre>
 */
public class ExternalDataProcessor {

    public static void main(String[] args) throws Exception {
        Properties props = loadProperties();

        if (args.length < 2) {
            throw new IllegalArgumentException(
                    "Usage: ExternalDataProcessor <hdfs-input-path> <hdfs-parquet-output-path> [options]");
        }

        String hdfsInput  = args[0];
        String hdfsOutput = args[1];

        // Optional sinks parsed from remaining args
        String s3Output      = null;
        String jdbcUrl       = null;
        String jdbcUser      = null;
        String jdbcPassword  = null;
        String jdbcTable     = null;
        String jdbcMode      = "append";

        for (int i = 2; i < args.length; i++) {
            switch (args[i]) {
                case "--output-s3"      -> s3Output     = args[++i];
                case "--output-jdbc"    -> jdbcUrl      = args[++i];
                case "--jdbc-user"      -> jdbcUser     = args[++i];
                case "--jdbc-password"  -> jdbcPassword = args[++i];
                case "--jdbc-table"     -> jdbcTable    = args[++i];
                case "--jdbc-mode"      -> jdbcMode     = args[++i];
                default -> System.err.println("Unknown option: " + args[i]);
            }
        }

        SparkSession spark = SparkSession.builder()
                .appName("External Data Processor")
                // mergeSchema=true allows Parquet reads to handle schema evolution
                // across runs where the external API added or removed fields.
                .config("spark.sql.parquet.mergeSchema", "true")
                .getOrCreate();

        // Read all NDJSON files recursively from the HDFS source tree.
        Dataset<Row> raw = spark.read()
                .option("recursiveFileLookup", "true")
                .option("multiLine", "false")  // one JSON object per line (NDJSON)
                .json(hdfsInput);

        Dataset<Row> transformed = transform(spark, raw);

        // ── Sink 1: HDFS Parquet (always) ───────────────────────────────────────
        transformed.write()
                .mode(SaveMode.Append)
                .option("mergeSchema", "true")
                .partitionBy("_source_api", "_date")
                .parquet(hdfsOutput);

        System.out.println("Written Parquet to HDFS: " + hdfsOutput);

        // ── Sink 2: S3 / MinIO (optional) ───────────────────────────────────────
        // Requires the Spark cluster to have hadoop-aws configured with:
        //   spark.hadoop.fs.s3a.endpoint, fs.s3a.access.key, fs.s3a.secret.key
        //   spark.hadoop.fs.s3a.path.style.access=true  (for MinIO)
        if (s3Output != null) {
            transformed.write()
                    .mode(SaveMode.Append)
                    .option("mergeSchema", "true")
                    .partitionBy("_source_api", "_date")
                    .parquet(s3Output);
            System.out.println("Written Parquet to S3: " + s3Output);
        }

        // ── Sink 3: JDBC data warehouse (optional) ───────────────────────────────
        // Flattens any remaining struct/array columns to JSON strings before writing
        // because JDBC does not support complex Spark types.
        if (jdbcUrl != null && jdbcTable != null) {
            Dataset<Row> jdbcReady = flattenForJdbc(transformed);
            Properties jdbcProps = new Properties();
            if (jdbcUser     != null) jdbcProps.setProperty("user",     jdbcUser);
            if (jdbcPassword != null) jdbcProps.setProperty("password", jdbcPassword);
            jdbcProps.setProperty("driver", "org.postgresql.Driver");

            jdbcReady.write()
                    .mode(SaveMode.valueOf(jdbcMode.substring(0, 1).toUpperCase() + jdbcMode.substring(1)))
                    .jdbc(jdbcUrl, jdbcTable, jdbcProps);
            System.out.println("Written to JDBC table: " + jdbcTable + " at " + jdbcUrl);
        }

        spark.stop();
    }

    /**
     * Core transformation pipeline — extracted for testability.
     *
     * <ol>
     *   <li>Verify the {@code _data} column exists and is a struct (it will be if
     *       the source sent JSON objects; it will be a string for arrays/primitives).</li>
     *   <li>Flatten {@code _data.*} fields into top-level columns (one level deep).</li>
     *   <li>Recursively flatten any nested struct fields within {@code _data}.</li>
     *   <li>Normalise all column names to lowercase with underscores.</li>
     *   <li>Add {@code _ingested_at} (current job timestamp) and
     *       {@code _record_hash} (SHA-256 of the full row as JSON) for audit.</li>
     *   <li>Derive {@code _date} for Parquet partition pruning.</li>
     * </ol>
     */
    static Dataset<Row> transform(SparkSession spark, Dataset<Row> raw) {
        if (raw.columns().length != 0 && hasColumn(raw, "_data")) {
            StructType dataType = raw.schema();
            StructField dataField = dataType.apply("_data");

            if (dataField.dataType() instanceof StructType dataStruct) {
                // Flatten _data.* fields into the top-level schema
                List<Column> cols = new ArrayList<>();
                cols.add(functions.col("_source_api"));
                cols.add(functions.col("_received_at"));

                for (StructField field : dataStruct.fields()) {
                    if (field.dataType() instanceof StructType nestedStruct) {
                        // One more level of flattening for nested objects
                        for (StructField nested : nestedStruct.fields()) {
                            String alias = normalizeColName(field.name() + "_" + nested.name());
                            cols.add(functions.col("_data." + field.name() + "." + nested.name()).alias(alias));
                        }
                    } else {
                        cols.add(functions.col("_data." + field.name()).alias(normalizeColName(field.name())));
                    }
                }

                raw = raw.select(cols.toArray(new Column[0]));
            } else {
                // _data is not a struct (could be array, string, or null) — convert to JSON string
                raw = raw.withColumn("data_payload", functions.to_json(functions.col("_data")))
                         .drop("_data");
            }
        }

        // Normalise any remaining column names that came from schema inference
        for (String colName : raw.columns()) {
            String normalized = normalizeColName(colName);
            if (!normalized.equals(colName)) {
                raw = raw.withColumnRenamed(colName, normalized);
            }
        }

        // Metadata audit columns
        Dataset<Row> enriched = raw
                .withColumn("_ingested_at", functions.current_timestamp())
                .withColumn("_record_hash",
                        functions.sha2(functions.to_json(functions.struct(
                                java.util.Arrays.stream(raw.columns())
                                        .map(functions::col)
                                        .toArray(Column[]::new)
                        )), 256))
                .withColumn("_date",
                        functions.to_date(functions.from_unixtime(
                                functions.col("_received_at").divide(1000))));

        return enriched;
    }

    /**
     * Converts any remaining struct or array columns to JSON strings so the
     * DataFrame can be written to a JDBC target that expects scalar types.
     */
    static Dataset<Row> flattenForJdbc(Dataset<Row> df) {
        for (StructField field : df.schema().fields()) {
            if (field.dataType() instanceof StructType || field.dataType() instanceof ArrayType) {
                df = df.withColumn(field.name(), functions.to_json(functions.col(field.name())));
            }
        }
        return df;
    }

    /**
     * Lowercases a column name and replaces any character that is not a letter,
     * digit, or underscore with {@code _}, so Parquet partition paths and SQL
     * column names are always safe regardless of what field names the API sends.
     */
    static String normalizeColName(String name) {
        return name.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }

    private static boolean hasColumn(Dataset<Row> df, String colName) {
        for (String col : df.columns()) {
            if (col.equals(colName)) return true;
        }
        return false;
    }

    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream is = ExternalDataProcessor.class.getClassLoader()
                .getResourceAsStream("spark-job.properties")) {
            if (is != null) props.load(is);
        } catch (Exception ignored) {}
        return props;
    }
}
