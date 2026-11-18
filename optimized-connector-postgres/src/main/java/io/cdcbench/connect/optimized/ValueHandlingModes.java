package io.cdcbench.connect.optimized;

import io.cdcbench.connect.common.PostgresValueConverter;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;
import java.util.Set;

/** Validates the PostgreSQL value representation modes exposed by Debezium. */
final class ValueHandlingModes {
    static final String DECIMAL = "decimal.handling.mode";
    static final String BINARY = "binary.handling.mode";
    static final String HSTORE = "hstore.handling.mode";
    static final String INTERVAL = "interval.handling.mode";

    private final String decimal;
    private final String binary;
    private final String hstore;
    private final String interval;

    ValueHandlingModes(Map<String, String> properties) {
        decimal = value(properties, DECIMAL, "precise", Set.of("precise", "string", "double"));
        binary = value(properties, BINARY, "bytes",
                Set.of("bytes", "base64", "base64-url-safe", "hex"));
        hstore = value(properties, HSTORE, "json", Set.of("json", "map"));
        interval = value(properties, INTERVAL, "numeric", Set.of("numeric", "string"));
    }

    String decimal() { return decimal; }
    String binary() { return binary; }
    String hstore() { return hstore; }
    String interval() { return interval; }

    PostgresValueConverter converter() {
        return converter(new TimePrecisionMode(Map.of()));
    }

    PostgresValueConverter converter(TimePrecisionMode timePrecisionMode) {
        return new PostgresValueConverter(
                decimal, binary, hstore, interval, timePrecisionMode.value());
    }

    private static String value(
            Map<String, String> properties, String key, String defaultValue,
            Set<String> allowed) {
        String configured = properties.getOrDefault(key, defaultValue);
        if (!allowed.contains(configured)) {
            throw new ConfigException(key, configured, "must be one of " + allowed);
        }
        return configured;
    }
}
