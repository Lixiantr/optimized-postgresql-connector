package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;

import java.util.Map;
import java.util.Set;

/** Controls the native path response to a failure while materializing one WAL event. */
enum EventProcessingFailureHandlingMode {
    FAIL("fail"),
    WARN("warn"),
    SKIP("skip");

    static final String PROPERTY = "event.processing.failure.handling.mode";
    private static final Set<String> ALLOWED = Set.of("fail", "warn", "skip");

    private final String value;

    EventProcessingFailureHandlingMode(String value) {
        this.value = value;
    }

    static EventProcessingFailureHandlingMode from(Map<String, String> properties) {
        String value = properties.getOrDefault(PROPERTY, "fail");
        return switch (value) {
            case "fail" -> FAIL;
            case "warn" -> WARN;
            case "skip" -> SKIP;
            default -> throw new ConfigException(PROPERTY, value, "must be one of " + ALLOWED);
        };
    }

    String value() {
        return value;
    }

    boolean skipsFailedEvents() {
        return this != FAIL;
    }
}
