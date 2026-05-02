package com.metrics.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class ExternalDataProcessorTest {

    private static SparkSession spark;

    @BeforeClass
    public static void setup() {
        spark = SparkSession.builder()
                .appName("ExternalDataProcessorTest")
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

    private Dataset<Row> fromNdjson(String... lines) {
        return spark.read()
                .option("multiLine", "false")
                .json(spark.createDataset(Arrays.asList(lines), Encoders.STRING()));
    }

    // ── normalizeColName ────────────────────────────────────────────────────

    @Test
    public void normalizeColName_lowercasesInput() {
        assertEquals("my_column", ExternalDataProcessor.normalizeColName("My_Column"));
    }

    @Test
    public void normalizeColName_replacesSpecialCharsWithUnderscore() {
        assertEquals("field_name_1", ExternalDataProcessor.normalizeColName("field-name.1"));
    }

    @Test
    public void normalizeColName_preservesAlreadyNormalizedName() {
        assertEquals("amount", ExternalDataProcessor.normalizeColName("amount"));
    }

    @Test
    public void normalizeColName_handlesSpaces() {
        assertEquals("my_field", ExternalDataProcessor.normalizeColName("my field"));
    }

    // ── transform — struct _data ────────────────────────────────────────────

    @Test
    public void transform_flattensDataStructIntoTopLevelColumns() {
        Dataset<Row> raw = fromNdjson(
                "{\"_source_api\":\"payments-api\",\"_received_at\":1714435200000,\"_data\":{\"id\":1,\"amount\":42.50}}"
        );

        Dataset<Row> result = ExternalDataProcessor.transform(spark, raw);

        List<String> cols = Arrays.asList(result.columns());
        assertTrue("id column should be promoted", cols.contains("id"));
        assertTrue("amount column should be promoted", cols.contains("amount"));
        assertFalse("_data should be removed after flattening", cols.contains("_data"));
    }

    @Test
    public void transform_addsAuditColumns() {
        Dataset<Row> raw = fromNdjson(
                "{\"_source_api\":\"payments-api\",\"_received_at\":1714435200000,\"_data\":{\"id\":1}}"
        );

        Dataset<Row> result = ExternalDataProcessor.transform(spark, raw);
        List<String> cols = Arrays.asList(result.columns());

        assertTrue("_ingested_at must be present", cols.contains("_ingested_at"));
        assertTrue("_record_hash must be present", cols.contains("_record_hash"));
        assertTrue("_date must be present", cols.contains("_date"));
    }

    @Test
    public void transform_derivesDateFromReceivedAt() {
        // 1714478400000 ms = 2024-04-30T12:00:00Z — midday UTC so local-timezone
        // date arithmetic lands on 2024-04-30 in any timezone from UTC-11 to UTC+11.
        Dataset<Row> raw = fromNdjson(
                "{\"_source_api\":\"payments-api\",\"_received_at\":1714478400000,\"_data\":{\"id\":1}}"
        );

        Dataset<Row> result = ExternalDataProcessor.transform(spark, raw);
        Row row = result.collectAsList().get(0);

        assertNotNull(row.getAs("_date"));
        assertEquals("2024-04-30", row.getAs("_date").toString());
    }

    @Test
    public void transform_normalizesColumnNamesFromInference() {
        // Spark may infer a column name with a dash; transform must sanitize it.
        Dataset<Row> raw = fromNdjson(
                "{\"_source_api\":\"orders-api\",\"_received_at\":1714435200000,\"_data\":{\"order_id\":99,\"total\":100}}"
        );

        Dataset<Row> result = ExternalDataProcessor.transform(spark, raw);
        for (String col : result.columns()) {
            assertTrue("Column name must be lowercase and safe: " + col,
                    col.matches("[a-z0-9_]+"));
        }
    }

    @Test
    public void transform_multipleRows_allProcessed() {
        Dataset<Row> raw = fromNdjson(
                "{\"_source_api\":\"payments-api\",\"_received_at\":1714435200000,\"_data\":{\"id\":1,\"amount\":10}}",
                "{\"_source_api\":\"payments-api\",\"_received_at\":1714435260000,\"_data\":{\"id\":2,\"amount\":20}}"
        );

        Dataset<Row> result = ExternalDataProcessor.transform(spark, raw);
        assertEquals(2, result.count());
    }

    @Test
    public void transform_recordHashDiffersForDifferentRows() {
        Dataset<Row> raw = fromNdjson(
                "{\"_source_api\":\"payments-api\",\"_received_at\":1714435200000,\"_data\":{\"id\":1,\"amount\":10}}",
                "{\"_source_api\":\"payments-api\",\"_received_at\":1714435260000,\"_data\":{\"id\":2,\"amount\":20}}"
        );

        List<Row> rows = ExternalDataProcessor.transform(spark, raw).collectAsList();
        String hash1 = rows.get(0).getAs("_record_hash");
        String hash2 = rows.get(1).getAs("_record_hash");
        assertNotEquals("Rows with different data must have different hashes", hash1, hash2);
    }

    // ── transform — non-struct _data ────────────────────────────────────────

    @Test
    public void transform_nonStructData_convertedToDataPayload() {
        // When _data is a JSON array, Spark infers it as ArrayType, not StructType.
        Dataset<Row> raw = fromNdjson(
                "{\"_source_api\":\"list-api\",\"_received_at\":1714435200000,\"_data\":[1,2,3]}"
        );

        Dataset<Row> result = ExternalDataProcessor.transform(spark, raw);
        List<String> cols = Arrays.asList(result.columns());

        assertTrue("data_payload column should be added for non-struct _data",
                cols.contains("data_payload"));
        assertFalse("_data should be dropped for non-struct _data", cols.contains("_data"));
    }

    // ── flattenForJdbc ──────────────────────────────────────────────────────

    @Test
    public void flattenForJdbc_convertsStructColumnsToJsonString() {
        // Build a DF that has a struct column after schema inference
        Dataset<Row> raw = fromNdjson(
                "{\"_source_api\":\"payments-api\",\"_received_at\":1714435200000,\"_data\":{\"nested\":{\"k\":\"v\"}}}"
        );
        Dataset<Row> transformed = ExternalDataProcessor.transform(spark, raw);
        Dataset<Row> forJdbc = ExternalDataProcessor.flattenForJdbc(transformed);

        // All columns must be scalar (no StructType / ArrayType)
        for (org.apache.spark.sql.types.StructField field : forJdbc.schema().fields()) {
            assertFalse("Column " + field.name() + " must not be a StructType after flattenForJdbc",
                    field.dataType() instanceof org.apache.spark.sql.types.StructType);
            assertFalse("Column " + field.name() + " must not be an ArrayType after flattenForJdbc",
                    field.dataType() instanceof org.apache.spark.sql.types.ArrayType);
        }
    }
}
