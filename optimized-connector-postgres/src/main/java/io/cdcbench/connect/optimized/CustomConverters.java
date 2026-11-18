package io.cdcbench.connect.optimized;

import io.cdcbench.connect.common.ColumnMetadata;
import io.cdcbench.connect.common.PgOutputMessage.Column;
import io.cdcbench.connect.common.PostgresValueConverter;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.lang.reflect.InvocationTargetException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.regex.Pattern;

/** Adapter for the public Debezium custom-converter SPI. */
final class CustomConverters {
    static final String CONVERTERS = "converters";
    private static final Pattern SAFE_ALIAS = Pattern.compile("[A-Za-z_][A-Za-z0-9_-]*");

    private final List<ConfiguredConverter> converters;

    CustomConverters(Map<String, String> properties) {
        converters = configuredConverters(properties, true);
    }

    static List<String> prefixes(Map<String, String> properties) {
        List<String> prefixes = new ArrayList<>();
        for (String alias : aliases(properties)) {
            prefixes.add(alias + ".");
        }
        return List.copyOf(prefixes);
    }

    static void validateConfiguration(Map<String, String> properties) {
        configuredConverters(properties, false);
    }

    Binding binding(
            String schema, String table, Column column, ColumnMetadata metadata,
            String databaseType, long baseTypeOid, PostgresValueConverter defaultConverter) {
        Object defaultValue = metadata.defaultExpression() == null
                ? null : defaultConverter.defaultValue(
                        new Column(column.key(), column.name(), baseTypeOid, column.typeModifier()),
                        metadata);
        RelationalColumn field = new RelationalColumnImpl(
                column.name(), schema + "." + table, jdbcType(baseTypeOid),
                (int) column.typeOid(), metadata.typeName(), databaseType,
                length(column, metadata), scale(column), metadata.nullable(),
                defaultValue, metadata.defaultExpression() != null);
        for (ConfiguredConverter configured : converters) {
            Binding[] registered = new Binding[1];
            configured.converter().converterFor(field, (schemaBuilder, converter) -> {
                if (schemaBuilder == null || converter == null) {
                    throw new ConfigException(
                            CONVERTERS, configured.alias(),
                            "custom converter registered a null schema or value converter");
                }
                registered[0] = new Binding(schemaBuilder.build(), converter);
            });
            if (registered[0] != null) {
                return registered[0];
            }
        }
        return null;
    }

    private static List<ConfiguredConverter> configuredConverters(
            Map<String, String> properties, boolean configure) {
        List<ConfiguredConverter> configured = new ArrayList<>();
        for (String alias : aliases(properties)) {
            String typeKey = alias + ".type";
            String className = properties.get(typeKey);
            if (className == null || className.isBlank()) {
                throw new ConfigException(typeKey, className, "must name a CustomConverter class");
            }
            CustomConverter<SchemaBuilder, RelationalColumn> converter =
                    instantiate(className.trim(), typeKey);
            if (configure) {
                Properties options = new Properties();
                String prefix = alias + ".";
                properties.forEach((key, value) -> {
                    if (key.startsWith(prefix) && !key.equals(typeKey)) {
                        options.setProperty(key.substring(prefix.length()), value);
                    }
                });
                converter.configure(options);
            }
            configured.add(new ConfiguredConverter(alias, converter));
        }
        return List.copyOf(configured);
    }

    private static List<String> aliases(Map<String, String> properties) {
        String value = properties.get(CONVERTERS);
        if (value == null || value.isBlank()) {
            return List.of();
        }
        List<String> aliases = new ArrayList<>();
        for (String item : value.split(",")) {
            String alias = item.trim();
            if (alias.isEmpty()) {
                continue;
            }
            if (!SAFE_ALIAS.matcher(alias).matches()) {
                throw new ConfigException(CONVERTERS, item, "contains an invalid converter alias");
            }
            if (aliases.contains(alias)) {
                throw new ConfigException(CONVERTERS, item, "contains a duplicate converter alias");
            }
            aliases.add(alias);
        }
        if (aliases.isEmpty()) {
            throw new ConfigException(CONVERTERS, value, "must contain at least one alias");
        }
        return List.copyOf(aliases);
    }

    @SuppressWarnings("unchecked")
    private static CustomConverter<SchemaBuilder, RelationalColumn> instantiate(
            String className, String typeKey) {
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            Class<?> rawClass = Class.forName(className, true, loader);
            if (!CustomConverter.class.isAssignableFrom(rawClass)) {
                throw new ConfigException(
                        typeKey, className, "must implement " + CustomConverter.class.getName());
            }
            return (CustomConverter<SchemaBuilder, RelationalColumn>)
                    rawClass.getConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
                 | IllegalAccessException | InvocationTargetException error) {
            throw new ConfigException(
                    typeKey, className, "cannot instantiate custom converter: "
                            + rootMessage(error));
        }
    }

    private static int jdbcType(long typeOid) {
        return switch ((int) typeOid) {
            case 16 -> Types.BOOLEAN;
            case 17 -> Types.BINARY;
            case 20, 26 -> Types.BIGINT;
            case 21 -> Types.SMALLINT;
            case 23 -> Types.INTEGER;
            case 25, 1042, 1043 -> Types.VARCHAR;
            case 700 -> Types.REAL;
            case 701 -> Types.DOUBLE;
            case 1082 -> Types.DATE;
            case 1083 -> Types.TIME;
            case 1114 -> Types.TIMESTAMP;
            case 1184 -> Types.TIMESTAMP_WITH_TIMEZONE;
            case 1266 -> Types.TIME_WITH_TIMEZONE;
            case 1700, 790 -> Types.NUMERIC;
            default -> Types.OTHER;
        };
    }

    private static OptionalInt length(Column column, ColumnMetadata metadata) {
        int modifier = column.typeModifier();
        if (modifier < 0) {
            return OptionalInt.empty();
        }
        if (column.typeOid() == 1700L) {
            return OptionalInt.of(((modifier - 4) >>> 16) & 0xffff);
        }
        if (column.typeOid() == 1042L || column.typeOid() == 1043L) {
            return OptionalInt.of(Math.max(0, modifier - 4));
        }
        if (column.typeOid() == 1560L || column.typeOid() == 1562L) {
            return OptionalInt.of(modifier);
        }
        if (metadata.array()) {
            return OptionalInt.empty();
        }
        return OptionalInt.empty();
    }

    private static OptionalInt scale(Column column) {
        if (column.typeOid() != 1700L || column.typeModifier() < 0) {
            return OptionalInt.empty();
        }
        return OptionalInt.of((column.typeModifier() - 4) & 0x7ff);
    }

    private static String rootMessage(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null
                ? current.getClass().getSimpleName() : current.getMessage();
    }

    record Binding(Schema schema, CustomConverter.Converter converter) {
        Map<String, Object> schemaDescription(String field) {
            return ConnectSchemaAdapter.describe(schema, field);
        }

        Object convert(Object value) {
            return ConnectSchemaAdapter.encode(schema, converter.convert(value));
        }
    }

    private record ConfiguredConverter(
            String alias, CustomConverter<SchemaBuilder, RelationalColumn> converter) {}

    private record RelationalColumnImpl(
            String name, String dataCollection, int jdbcType, int nativeType,
            String typeName, String typeExpression, OptionalInt length,
            OptionalInt scale, boolean optional, Object defaultValue,
            boolean hasDefaultValue) implements RelationalColumn {
        @Override
        public boolean isOptional() {
            return optional;
        }
    }
}
