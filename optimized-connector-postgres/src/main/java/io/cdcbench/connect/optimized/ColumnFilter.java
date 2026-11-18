package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Anchored schema.table.column filters for emitted record values. */
final class ColumnFilter {
    static final String COLUMN_INCLUDE_LIST = "column.include.list";
    static final String COLUMN_EXCLUDE_LIST = "column.exclude.list";

    private final List<Pattern> includes;
    private final List<Pattern> excludes;

    ColumnFilter(Map<String, String> properties) {
        includes = patterns(properties, COLUMN_INCLUDE_LIST);
        excludes = patterns(properties, COLUMN_EXCLUDE_LIST);
        if (!includes.isEmpty() && !excludes.isEmpty()) {
            throw new ConfigException(
                    COLUMN_INCLUDE_LIST + "/" + COLUMN_EXCLUDE_LIST,
                    "both configured", "include and exclude lists are mutually exclusive");
        }
    }

    boolean includes(String schema, String table, String column) {
        String qualifiedName = schema + "." + table + "." + column;
        if (!includes.isEmpty()) {
            return includes.stream().anyMatch(pattern -> pattern.matcher(qualifiedName).matches());
        }
        return excludes.stream().noneMatch(pattern -> pattern.matcher(qualifiedName).matches());
    }

    private static List<Pattern> patterns(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            return List.of();
        }
        List<Pattern> patterns = new ArrayList<>();
        for (String expression : value.split(",")) {
            String normalized = expression.trim();
            if (normalized.isEmpty()) {
                continue;
            }
            try {
                patterns.add(Pattern.compile(normalized));
            } catch (PatternSyntaxException error) {
                throw new ConfigException(
                        key, expression, "must contain valid Java regular expressions");
            }
        }
        return List.copyOf(patterns);
    }
}
