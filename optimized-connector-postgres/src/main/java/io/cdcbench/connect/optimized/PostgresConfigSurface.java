package io.cdcbench.connect.optimized;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/** Version-pinned public configuration contract captured through Kafka Connect REST. */
final class PostgresConfigSurface {
    private static final String RESOURCE =
            "/io/cdcbench/connect/optimized/postgres-3.6.0-config.json";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ConfigDef CONFIG_DEF = load();

    private PostgresConfigSurface() {
    }

    static ConfigDef configDef() {
        return CONFIG_DEF;
    }

    private static ConfigDef load() {
        List<Map<String, Object>> entries;
        try (InputStream input = PostgresConfigSurface.class.getResourceAsStream(RESOURCE)) {
            if (input == null) {
                throw new IllegalStateException("Missing PostgreSQL config contract " + RESOURCE);
            }
            entries = JSON.readValue(input, new TypeReference<>() { });
        } catch (IOException error) {
            throw new ExceptionInInitializerError(error);
        }

        ConfigDef result = new ConfigDef();
        for (Map<String, Object> entry : entries) {
            String name = text(entry, "name");
            ConfigDef.Type type = ConfigDef.Type.valueOf(text(entry, "type"));
            ConfigDef.Importance importance = ConfigDef.Importance.valueOf(
                    text(entry, "importance"));
            String documentation = nullableText(entry.get("documentation"));
            String group = nullableText(entry.get("group"));
            int order = integer(entry.get("order_in_group"), 0);
            ConfigDef.Width width = ConfigDef.Width.valueOf(text(entry, "width"));
            String displayName = nullableText(entry.get("display_name"));
            List<String> dependents = stringList(entry.get("dependents"));
            Object defaultValue = defaultValue(entry.get("default_value"), type);
            result.define(name, type, defaultValue, null, importance,
                    documentation == null ? "" : documentation,
                    group, order, width,
                    displayName == null ? name : displayName,
                    dependents);
        }
        return result;
    }

    private static Object defaultValue(Object value, ConfigDef.Type type) {
        if (value == null) {
            return null;
        }
        String text = value.toString();
        return switch (type) {
            case BOOLEAN -> Boolean.valueOf(text);
            case INT -> Integer.valueOf(text);
            case LONG -> Long.valueOf(text);
            case LIST -> text.isEmpty() ? List.of() : List.of(text.split(",", -1));
            case CLASS -> loadClass(text);
            default -> text;
        };
    }

    private static Class<?> loadClass(String name) {
        try {
            return Class.forName(name, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException error) {
            throw new ConfigException("Config default class is unavailable: " + name);
        }
    }

    private static String text(Map<String, Object> entry, String key) {
        String value = nullableText(entry.get(key));
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("PostgreSQL config contract has no " + key);
        }
        return value;
    }

    private static String nullableText(Object value) {
        return value == null ? null : value.toString();
    }

    private static int integer(Object value, int defaultValue) {
        return value == null ? defaultValue : Integer.parseInt(value.toString());
    }

    private static List<String> stringList(Object value) {
        if (!(value instanceof List<?> values)) {
            return List.of();
        }
        return values.stream().map(Object::toString).toList();
    }
}
