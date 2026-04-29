package com.metrics.processor.parser;

import com.metrics.processor.model.NormalizedMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses raw log lines conforming to the standard structured log format.
 *
 * <p>Expected format:
 * <pre>
 * 2024-01-15T10:30:00.123Z [INFO] [svc-api] Some message here
 * </pre>
 *
 * <p>The regex extracts four capture groups:
 * <ol>
 *   <li>ISO-8601 UTC timestamp (e.g. {@code 2024-01-15T10:30:00.123Z})</li>
 *   <li>Log level (e.g. {@code INFO}, {@code ERROR}, {@code WARN})</li>
 *   <li>Service ID (e.g. {@code svc-api})</li>
 *   <li>Free-form message text</li>
 * </ol>
 *
 * <p>Lines that do not match the pattern return {@code Optional.empty()}. This includes
 * blank lines, plain-text messages without a timestamp, and log lines using a different
 * format. Non-matching lines are not an error — they are expected in a heterogeneous
 * log stream and are handled by Flink Pipeline 2 for raw archival.
 *
 * <p>This parser mirrors the {@code LOG_PATTERN} used in {@code SparkLogProcessor} so that
 * the same lines are accepted/rejected across both the streaming and batch paths.
 */
@Component
public class LogParser {

    private static final Logger log = LoggerFactory.getLogger(LogParser.class);

    // Matches: 2024-01-15T10:30:00.123Z [INFO] [svc-api] Some message here
    private static final Pattern STANDARD_LOG_PATTERN = Pattern.compile(
            "(\\d{4}-\\d{2}-\\d{2}T[\\d:.]+Z)\\s+\\[(\\w+)]\\s+\\[([^\\]]+)]\\s+(.*)"
    );

    /**
     * Attempts to parse a single raw log line.
     *
     * <p>On a successful match, returns a partially-populated {@link NormalizedMetric}
     * with {@code metricName="log.event"}, {@code value=1.0}, {@code unit="count"},
     * and the extracted {@code serviceId}, {@code parsedLevel}, {@code logMessage},
     * and {@code timestamp}. The enrichment fields ({@code serviceName}, {@code team},
     * {@code environment}, {@code region}) are set to placeholder defaults and are
     * expected to be overwritten by {@code EnrichmentService}.
     *
     * @param rawLine the raw log line to parse; null or blank returns empty
     * @return an {@code Optional} containing the parsed {@link NormalizedMetric},
     *         or {@code Optional.empty()} if the line does not match the pattern
     */
    public Optional<NormalizedMetric> parse(String rawLine) {
        if (rawLine == null || rawLine.isBlank()) {
            return Optional.empty();
        }

        Matcher matcher = STANDARD_LOG_PATTERN.matcher(rawLine.trim());
        if (!matcher.matches()) {
            log.debug("Log line did not match standard pattern: {}", rawLine);
            return Optional.empty();
        }

        String timestampStr = matcher.group(1);
        String level = matcher.group(2);
        String serviceId = matcher.group(3);
        String message = matcher.group(4);

        Instant timestamp;
        try {
            timestamp = Instant.parse(timestampStr);
        } catch (DateTimeParseException e) {
            log.warn("Failed to parse timestamp '{}' from log line: {}", timestampStr, rawLine);
            timestamp = Instant.now();
        }

        NormalizedMetric metric = new NormalizedMetric();
        metric.setMetricName("log.event");
        metric.setValue(1.0);
        metric.setUnit("count");
        metric.setServiceId(serviceId);
        metric.setParsedLevel(level.toUpperCase());
        metric.setLogMessage(message);
        metric.setTimestamp(timestamp);
        metric.setTimestampMs(timestamp.toEpochMilli());

        // Default enrichment fields — will be overwritten by EnrichmentService if metadata is found
        metric.setServiceName(serviceId);
        metric.setTeam("unknown");
        metric.setEnvironment("unknown");
        metric.setRegion("unknown");

        return Optional.of(metric);
    }
}
