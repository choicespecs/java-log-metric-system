package com.metrics.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SparkLogProcessorTest {

    private static SparkSession spark;

    @BeforeClass
    public static void setup() {
        spark = SparkSession.builder()
                .appName("SparkLogProcessorTest")
                .master("local[1]")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterClass
    public static void teardown() {
        if (spark != null) {
            spark.stop();
        }
    }

    // Helper: apply the same parse logic used in SparkLogProcessor.
    private Dataset<Row> parse(List<String> lines) {
        Dataset<Row> input = spark.createDataset(lines, Encoders.STRING()).toDF("value");
        return input.select(
                functions.regexp_extract(functions.col("value"), SparkLogProcessor.LOG_PATTERN, 1).alias("timestamp"),
                functions.regexp_extract(functions.col("value"), SparkLogProcessor.LOG_PATTERN, 2).alias("level"),
                functions.regexp_extract(functions.col("value"), SparkLogProcessor.LOG_PATTERN, 3).alias("service_id"),
                functions.regexp_extract(functions.col("value"), SparkLogProcessor.LOG_PATTERN, 4).alias("message"),
                functions.col("value").alias("raw_line")
        )
        .withColumn("date", functions.to_date(functions.col("timestamp")))
        .filter(functions.col("timestamp").notEqual(""));
    }

    @Test
    public void validInfoLine_isParsedCorrectly() {
        List<Row> rows = parse(Arrays.asList(
                "2024-01-15T10:30:00Z [INFO] [svc-api] Request processed successfully"
        )).collectAsList();

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals("2024-01-15T10:30:00Z", row.getAs("timestamp"));
        assertEquals("INFO", row.getAs("level"));
        assertEquals("svc-api", row.getAs("service_id"));
        assertEquals("Request processed successfully", row.getAs("message"));
    }

    @Test
    public void validErrorLine_isParsedCorrectly() {
        List<Row> rows = parse(Arrays.asList(
                "2024-01-15T10:30:01Z [ERROR] [svc-worker] NullPointerException in PaymentService"
        )).collectAsList();

        assertEquals(1, rows.size());
        assertEquals("ERROR", rows.get(0).getAs("level"));
        assertEquals("svc-worker", rows.get(0).getAs("service_id"));
    }

    @Test
    public void unparsableLine_isFiltered() {
        assertEquals(0, parse(Arrays.asList("this is not a structured log line")).count());
    }

    @Test
    public void emptyLine_isFiltered() {
        assertEquals(0, parse(Arrays.asList("")).count());
    }

    @Test
    public void mixedBatch_onlyValidLinesPass() {
        List<Row> rows = parse(Arrays.asList(
                "2024-01-15T10:30:00Z [INFO] [svc-api] Request processed",
                "2024-01-15T10:30:01Z [ERROR] [svc-worker] Processing failed",
                "not a log line",
                "",
                "2024-01-15T10:30:02Z [WARN] [svc-api] High latency detected"
        )).collectAsList();

        assertEquals(3, rows.size());
    }

    @Test
    public void datePartitionColumn_derivedFromTimestamp() {
        List<Row> rows = parse(Arrays.asList(
                "2024-03-20T08:00:00Z [INFO] [svc-api] startup complete"
        )).collectAsList();

        assertEquals(1, rows.size());
        // date column is a java.sql.Date — toString() yields "2024-03-20"
        assertTrue(rows.get(0).getAs("date").toString().startsWith("2024-03-20"));
    }

    @Test
    public void rawLine_preservedVerbatim() {
        String originalLine = "2024-01-15T10:30:00Z [DEBUG] [svc-api] payload={\"key\":\"value\"}";
        List<Row> rows = parse(Arrays.asList(originalLine)).collectAsList();

        assertEquals(1, rows.size());
        assertEquals(originalLine, rows.get(0).getAs("raw_line"));
    }
}
