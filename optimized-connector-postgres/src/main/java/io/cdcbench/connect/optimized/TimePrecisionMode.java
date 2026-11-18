package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;

import java.util.Map;
import java.util.Set;

/** Validates the PostgreSQL temporal precision contract exposed by Debezium. */
final class TimePrecisionMode {
    static final String PROPERTY = "time.precision.mode";
    private static final Set<String> ALLOWED = Set.of(
            "adaptive", "adaptive_time_microseconds", "connect",
            "isostring", "microseconds", "nanoseconds");

    private final String value;

    TimePrecisionMode(Map<String, String> properties) {
        value = properties.getOrDefault(PROPERTY, "adaptive");
        if (!ALLOWED.contains(value)) {
            throw new ConfigException(PROPERTY, value, "must be one of " + ALLOWED);
        }
    }

    String value() {
        return value;
    }
}
