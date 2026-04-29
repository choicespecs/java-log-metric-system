package com.metrics.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.InputStream;
import java.util.Properties;

/**
 * Spark batch job: reads raw unstructured log text files from HDFS, parses each
 * line with a regex, and writes structured Parquet output partitioned by level/date.
 *
 * <p>Expected input format (one log line per text record):
 * <pre>2024-01-15T10:30:00Z [INFO] [svc-api] Request processed successfully</pre>
 *
 * <p>Typical invocation:
 * <pre>
 * spark-submit \
 *   --master spark://spark-master:7077 \
 *   --class com.metrics.spark.SparkLogProcessor \
 *   target/spark-processor-1.0.0.jar \
 *   hdfs://namenode:9000/metrics/logs/raw \
 *   hdfs://namenode:9000/metrics/logs/parquet
 * </pre>
 *
 * <p>If no arguments are provided, paths are read from {@code spark-job.properties}.
 */
public class SparkLogProcessor {

    // Matches: 2024-01-15T10:30:00Z [INFO] [svc-api] message text here
    static final String LOG_PATTERN =
            "(\\d{4}-\\d{2}-\\d{2}T[\\d:.]+Z)\\s+\\[(\\w+)]\\s+\\[([^\\]]+)]\\s+(.*)";

    public static void main(String[] args) throws Exception {
        Properties props = loadProperties();

        String inputPath = args.length > 0
                ? args[0]
                : props.getProperty("hdfs.logs.input.path", "hdfs://namenode:9000/metrics/logs/raw");
        String outputPath = args.length > 1
                ? args[1]
                : props.getProperty("hdfs.logs.output.path", "hdfs://namenode:9000/metrics/logs/parquet");

        SparkSession spark = SparkSession.builder()
                .appName("Metrics Log Processor")
                .getOrCreate();

        process(spark, inputPath, outputPath);

        spark.stop();
    }

    /**
     * Core transformation — extracted so tests can call it with a local SparkSession.
     *
     * @param spark      active SparkSession
     * @param inputPath  HDFS path to raw text log files produced by the Flink job
     * @param outputPath HDFS path for structured Parquet output
     */
    public static void process(SparkSession spark, String inputPath, String outputPath) {
        // Read all .txt files written by Flink's FileSink under inputPath recursively.
        Dataset<Row> rawLogs = spark.read()
                .option("recursiveFileLookup", "true")
                .text(inputPath);

        Dataset<Row> parsed = rawLogs
                .select(
                        functions.regexp_extract(functions.col("value"), LOG_PATTERN, 1).alias("timestamp"),
                        functions.regexp_extract(functions.col("value"), LOG_PATTERN, 2).alias("level"),
                        functions.regexp_extract(functions.col("value"), LOG_PATTERN, 3).alias("service_id"),
                        functions.regexp_extract(functions.col("value"), LOG_PATTERN, 4).alias("message"),
                        functions.col("value").alias("raw_line")
                )
                // Derive a date column for partitioning; null-safe — unparsed lines get null timestamp.
                .withColumn("date", functions.to_date(functions.col("timestamp")))
                // Drop lines that did not match the pattern (empty timestamp after regex extraction).
                .filter(functions.col("timestamp").notEqual(""));

        // Write Parquet partitioned by log level and date so downstream queries can
        // prune partitions efficiently (e.g. WHERE level='ERROR' AND date='2024-01-15').
        parsed.write()
                .mode(SaveMode.Append)
                .partitionBy("level", "date")
                .parquet(outputPath);
    }

    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream is = SparkLogProcessor.class.getClassLoader()
                .getResourceAsStream("spark-job.properties")) {
            if (is != null) {
                props.load(is);
            }
        } catch (Exception e) {
            // Non-fatal — defaults are baked into the property lookups above.
        }
        return props;
    }
}
