package io.cdcbench.connect.optimized;

import io.cdcbench.connect.common.ColumnMetadata;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Adds PostgreSQL source-column metadata to the emitted Connect schema. */
final class SourceTypePropagator {
    static final String COLUMN_PROPERTY = "column.propagate.source.type";
    static final String DATATYPE_PROPERTY = "datatype.propagate.source.type";
    static final String SOURCE_TYPE = "__debezium.source.column.type";
    static final String SOURCE_NAME = "__debezium.source.column.name";
    static final String SOURCE_LENGTH = "__debezium.source.column.length";
    static final String SOURCE_SCALE = "__debezium.source.column.scale";

    private final String database;
    private final List<Pattern> columnPatterns;
    private final List<Pattern> datatypePatterns;

    SourceTypePropagator(String database, Map<String, String> properties) {
        this.database = database;
        columnPatterns = patterns(properties, COLUMN_PROPERTY);
        datatypePatterns = patterns(properties, DATATYPE_PROPERTY);
    }

    boolean enabled() {
        return !columnPatterns.isEmpty() || !datatypePatterns.isEmpty();
    }

    Map<String, String> parameters(
            String schema, String table, String column, ColumnMetadata metadata,
            JdbcMetadata jdbcMetadata) {
        String typeName = metadata.typeName();
        if (typeName == null) {
            return Map.of();
        }
        String prefix = database + "." + schema + "." + table + ".";
        boolean selected = matches(columnPatterns, prefix + column)
                || matches(datatypePatterns, prefix + typeName);
        if (!selected) {
            return Map.of();
        }

        Map<String, String> parameters = new LinkedHashMap<>();
        parameters.put(SOURCE_TYPE, typeName.toUpperCase(Locale.ROOT));
        parameters.put(SOURCE_NAME, column);
        if (jdbcMetadata != null && jdbcMetadata.length() != null) {
            parameters.put(SOURCE_LENGTH, Integer.toString(jdbcMetadata.length()));
        }
        if (jdbcMetadata != null && jdbcMetadata.scale() != null) {
            parameters.put(SOURCE_SCALE, Integer.toString(jdbcMetadata.scale()));
        }
        return Map.copyOf(parameters);
    }

    private static boolean matches(List<Pattern> patterns, String value) {
        return patterns.stream().anyMatch(pattern -> pattern.matcher(value).matches());
    }

    private static List<Pattern> patterns(Map<String, String> properties, String key) {
        String configured = properties.get(key);
        if (configured == null || configured.isBlank()) {
            return List.of();
        }
        List<Pattern> patterns = new ArrayList<>();
        for (String expression : configured.split(",", -1)) {
            String normalized = expression.trim();
            if (normalized.isEmpty()) {
                continue;
            }
            try {
                patterns.add(Pattern.compile(normalized));
            } catch (PatternSyntaxException error) {
                throw new ConfigException(key, expression, "must contain valid regular expressions");
            }
        }
        return List.copyOf(patterns);
    }

    record JdbcMetadata(Integer length, Integer scale) {}
}
