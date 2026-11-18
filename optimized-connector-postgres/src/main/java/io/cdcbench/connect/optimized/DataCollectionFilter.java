package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Version-pinned Debezium-style anchored filters for PostgreSQL data collections. */
final class DataCollectionFilter {
    static final String TABLE_INCLUDE_LIST = "table.include.list";
    static final String TABLE_EXCLUDE_LIST = "table.exclude.list";
    static final String SCHEMA_INCLUDE_LIST = "schema.include.list";
    static final String SCHEMA_EXCLUDE_LIST = "schema.exclude.list";

    private final List<Pattern> tableIncludes;
    private final List<Pattern> tableExcludes;
    private final List<Pattern> schemaIncludes;
    private final List<Pattern> schemaExcludes;

    DataCollectionFilter(Map<String, String> properties) {
        tableIncludes = patterns(properties, TABLE_INCLUDE_LIST);
        tableExcludes = patterns(properties, TABLE_EXCLUDE_LIST);
        schemaIncludes = patterns(properties, SCHEMA_INCLUDE_LIST);
        schemaExcludes = patterns(properties, SCHEMA_EXCLUDE_LIST);
        rejectConflictingFilters(
                TABLE_INCLUDE_LIST, tableIncludes, TABLE_EXCLUDE_LIST, tableExcludes);
        rejectConflictingFilters(
                SCHEMA_INCLUDE_LIST, schemaIncludes, SCHEMA_EXCLUDE_LIST, schemaExcludes);
    }

    Set<QualifiedTable> resolve(Connection connection) throws SQLException {
        Set<QualifiedTable> tables = new LinkedHashSet<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT namespace.nspname, relation.relname "
                        + "FROM pg_catalog.pg_class relation "
                        + "JOIN pg_catalog.pg_namespace namespace "
                        + "ON namespace.oid = relation.relnamespace "
                        + "WHERE relation.relkind IN ('r','p') "
                        + "AND namespace.nspname <> 'information_schema' "
                        + "AND namespace.nspname NOT LIKE 'pg\\_%' ESCAPE '\\' "
                        + "ORDER BY namespace.nspname, relation.relname");
             ResultSet result = statement.executeQuery()) {
            while (result.next()) {
                String schema = result.getString(1);
                String table = result.getString(2);
                if (!includes(schema, table)) {
                    continue;
                }
                tables.add(QualifiedTable.of(schema, table));
            }
        }
        return Set.copyOf(tables);
    }

    boolean includes(String schema, String table) {
        if (isSystemSchema(schema)) {
            return false;
        }
        String qualifiedName = schema + "." + table;
        return selected(schema, schemaIncludes, schemaExcludes)
                && selected(qualifiedName, tableIncludes, tableExcludes);
    }

    private static boolean selected(
            String value, List<Pattern> includes, List<Pattern> excludes) {
        if (!includes.isEmpty()) {
            return includes.stream().anyMatch(pattern -> pattern.matcher(value).matches());
        }
        return excludes.stream().noneMatch(pattern -> pattern.matcher(value).matches());
    }

    private static boolean isSystemSchema(String schema) {
        return "information_schema".equals(schema) || schema.startsWith("pg_");
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
                throw new ConfigException(key, expression,
                        "must contain valid Java regular expressions");
            }
        }
        return List.copyOf(patterns);
    }

    private static void rejectConflictingFilters(
            String includeKey, List<Pattern> includes,
            String excludeKey, List<Pattern> excludes) {
        if (!includes.isEmpty() && !excludes.isEmpty()) {
            throw new ConfigException(includeKey + "/" + excludeKey, "both configured",
                    "include and exclude lists are mutually exclusive");
        }
    }
}
