package com.metrics.processor.parser;

import com.metrics.processor.model.NormalizedMetric;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class LogParserTest {

    private LogParser logParser;

    @BeforeEach
    void setUp() {
        logParser = new LogParser();
    }

    @Test
    void parse_withValidStandardLogLine_returnsNormalizedMetric() {
        String validLogLine = "2024-01-15T10:30:00.123Z [INFO] [svc-api] Request processed successfully in 45ms";

        Optional<NormalizedMetric> result = logParser.parse(validLogLine);

        assertTrue(result.isPresent(), "Expected a parsed NormalizedMetric but got empty");
        NormalizedMetric metric = result.get();

        assertEquals("log.event", metric.getMetricName());
        assertEquals(1.0, metric.getValue(), 0.001);
        assertEquals("count", metric.getUnit());
        assertEquals("svc-api", metric.getServiceId());
        assertEquals("INFO", metric.getParsedLevel());
        assertEquals("Request processed successfully in 45ms", metric.getLogMessage());
        assertNotNull(metric.getTimestamp());
        assertEquals(1705314600123L, metric.getTimestampMs());
    }

    @Test
    void parse_withWarnLevel_returnsCorrectLevel() {
        String warnLogLine = "2024-06-01T08:00:00.000Z [WARN] [svc-worker] High memory usage detected";

        Optional<NormalizedMetric> result = logParser.parse(warnLogLine);

        assertTrue(result.isPresent());
        assertEquals("WARN", result.get().getParsedLevel());
        assertEquals("svc-worker", result.get().getServiceId());
        assertEquals("High memory usage detected", result.get().getLogMessage());
    }

    @Test
    void parse_withErrorLevel_returnsCorrectLevel() {
        String errorLogLine = "2024-06-01T09:15:30.500Z [ERROR] [svc-api] NullPointerException at line 42";

        Optional<NormalizedMetric> result = logParser.parse(errorLogLine);

        assertTrue(result.isPresent());
        assertEquals("ERROR", result.get().getParsedLevel());
        assertEquals("NullPointerException at line 42", result.get().getLogMessage());
    }

    @Test
    void parse_withUnrecognizedLine_returnsEmpty() {
        String unrecognizedLine = "this is not a structured log line at all";

        Optional<NormalizedMetric> result = logParser.parse(unrecognizedLine);

        assertFalse(result.isPresent(), "Expected empty Optional for unrecognized log line");
    }

    @Test
    void parse_withPlainTextNoTimestamp_returnsEmpty() {
        String plainText = "ERROR something went wrong";

        Optional<NormalizedMetric> result = logParser.parse(plainText);

        assertFalse(result.isPresent());
    }

    @Test
    void parse_withNullInput_returnsEmpty() {
        Optional<NormalizedMetric> result = logParser.parse(null);

        assertFalse(result.isPresent(), "Expected empty Optional for null input");
    }

    @Test
    void parse_withBlankInput_returnsEmpty() {
        Optional<NormalizedMetric> result = logParser.parse("   ");

        assertFalse(result.isPresent(), "Expected empty Optional for blank input");
    }

    @Test
    void parse_withMissingServiceIdBrackets_returnsEmpty() {
        String malformedLine = "2024-01-15T10:30:00.000Z [INFO] svc-api Message without brackets";

        Optional<NormalizedMetric> result = logParser.parse(malformedLine);

        assertFalse(result.isPresent());
    }
}
