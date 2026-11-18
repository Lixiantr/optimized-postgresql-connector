package io.cdcbench.connect.optimized;

import io.cdcbench.connect.common.ColumnMetadata;
import io.cdcbench.connect.common.PgOutputMessage.Column;
import io.cdcbench.connect.common.PostgresValueConverter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Builds the version-pinned Debezium 3.6 JSON schema contract without runtime delegation. */
final class DebeziumSchemaFactory {
    private final PostgresValueConverter converter;
    private final ValueHandlingModes modes;
    private final TimePrecisionMode timePrecisionMode;

    DebeziumSchemaFactory(PostgresValueConverter converter) {
        this(converter, new ValueHandlingModes(Map.of()), new TimePrecisionMode(Map.of()));
    }

    DebeziumSchemaFactory(PostgresValueConverter converter, ValueHandlingModes modes) {
        this(converter, modes, new TimePrecisionMode(Map.of()));
    }

    DebeziumSchemaFactory(
            PostgresValueConverter converter, ValueHandlingModes modes,
            TimePrecisionMode timePrecisionMode) {
        this.converter = converter;
        this.modes = modes;
        this.timePrecisionMode = timePrecisionMode;
    }

    RelationSchemas relation(String prefix, String namespace, String table,
                             List<ColumnDefinition> columns) {
        return relationWithSchemaPrefix(
                prefix + "." + namespace + "." + table, columns);
    }

    RelationSchemas relationWithSchemaPrefix(
            String schemaPrefix, List<ColumnDefinition> columns) {
        String qualified = schemaPrefix;
        List<Map<String, Object>> keyFields = new ArrayList<>();
        for (ColumnDefinition column : columns) {
            if (column.metadata().key()) {
                keyFields.add(columnSchema(column, true));
            }
        }
        Map<String, Object> key = struct(keyFields, false, qualified + ".Key", null);

        List<Map<String, Object>> envelopeFields = new ArrayList<>();
        envelopeFields.add(row(columns, qualified + ".Value", "before"));
        envelopeFields.add(row(columns, qualified + ".Value", "after"));
        envelopeFields.add(source());
        envelopeFields.add(transaction());
        envelopeFields.add(primitiveField("string", false, "op"));
        envelopeFields.add(primitiveField("int64", true, "ts_ms"));
        envelopeFields.add(primitiveField("int64", true, "ts_us"));
        envelopeFields.add(primitiveField("int64", true, "ts_ns"));
        Map<String, Object> envelope = struct(
                envelopeFields, false, qualified + ".Envelope", null);
        envelope.put("version", 2);
        return new RelationSchemas(keyFields.isEmpty() ? null : key, envelope);
    }

    static Map<String, Object> heartbeatKey() {
        Map<String, Object> schema = struct(List.of(primitiveField("string", false, "serverName")),
                false, "io.debezium.connector.common.ServerNameKey", null);
        schema.put("version", 1);
        return schema;
    }

    static Map<String, Object> heartbeatValue() {
        Map<String, Object> schema = struct(List.of(primitiveField("int64", false, "ts_ms")),
                false, "io.debezium.connector.common.Heartbeat", null);
        schema.put("version", 1);
        return schema;
    }

    static Map<String, Object> transactionMetadataKey() {
        Map<String, Object> schema = struct(List.of(primitiveField("string", false, "id")),
                false, "io.debezium.connector.common.TransactionMetadataKey", null);
        schema.put("version", 1);
        return schema;
    }

    static Map<String, Object> transactionMetadataValue() {
        Map<String, Object> collection = struct(List.of(
                        primitiveField("string", false, "data_collection"),
                        primitiveField("int64", false, "event_count")),
                true, "event.collection", null);
        collection.put("version", 1);
        Map<String, Object> collections = typed("array");
        collections.put("items", collection);
        collections.put("optional", true);
        collections.put("field", "data_collections");
        Map<String, Object> schema = struct(List.of(
                        primitiveField("string", false, "status"),
                        primitiveField("string", false, "id"),
                        primitiveField("int64", true, "event_count"),
                        collections,
                        primitiveField("int64", false, "ts_ms")),
                false, "io.debezium.connector.common.TransactionMetadataValue", null);
        schema.put("version", 1);
        return schema;
    }

    static Map<String, Object> logicalMessageKey() {
        Map<String, Object> schema = struct(List.of(primitiveField("string", true, "prefix")),
                false, "io.debezium.connector.postgresql.MessageKey", null);
        schema.put("version", 1);
        return schema;
    }

    static Map<String, Object> logicalMessageValue() {
        Map<String, Object> message = struct(List.of(
                        primitiveField("string", true, "prefix"),
                        primitiveField("bytes", true, "content")),
                false, "io.debezium.connector.postgresql.Message", "message");
        message.put("version", 1);
        Map<String, Object> schema = struct(List.of(
                        primitiveField("string", false, "op"),
                        primitiveField("int64", true, "ts_ms"),
                        source(),
                        transaction(),
                        message),
                false, "io.debezium.connector.postgresql.MessageValue", null);
        schema.put("version", 1);
        return schema;
    }

    private Map<String, Object> row(List<ColumnDefinition> columns, String name, String field) {
        List<Map<String, Object>> fields = new ArrayList<>(columns.size());
        for (ColumnDefinition column : columns) {
            if (column.includedInValue()) {
                fields.add(columnSchema(column, false));
            }
        }
        return struct(fields, true, name, field);
    }

    private Map<String, Object> columnSchema(ColumnDefinition definition, boolean key) {
        Column column = definition.message();
        ColumnMetadata metadata = definition.metadata();
        Map<String, Object> schema;
        if (definition.customSchema() != null) {
            schema = new LinkedHashMap<>(definition.customSchema());
            if (key) {
                schema.remove("optional");
            }
        } else {
            schema = typeSchema(column, metadata, definition.unknownDatatype());
            schema.put("optional", key ? false : metadata.nullable());
            if (!key && metadata.defaultExpression() != null) {
                schema.put("default", converter.defaultValue(column, metadata));
            }
        }
        if (!key && !definition.schemaParameters().isEmpty()) {
            Map<String, Object> parameters = new LinkedHashMap<>();
            Object existing = schema.get("parameters");
            if (existing instanceof Map<?, ?> existingParameters) {
                existingParameters.forEach((name, value) ->
                        parameters.put(name.toString(), value));
            }
            parameters.putAll(definition.schemaParameters());
            schema.put("parameters", parameters);
        }
        schema.put("field", column.name());
        return schema;
    }

    private Map<String, Object> typeSchema(
            Column column, ColumnMetadata metadata, boolean unknownDatatype) {
        if (unknownDatatype) {
            if (!metadata.array()) {
                return typed("bytes");
            }
            Map<String, Object> schema = typed("array");
            Map<String, Object> items = typed("bytes");
            items.put("optional", false);
            schema.put("items", items);
            return schema;
        }
        if (metadata.array()) {
            Column element = new Column(false, "element", metadata.elementTypeOid(), column.typeModifier());
            ColumnMetadata elementMetadata = new ColumnMetadata(
                    true, null, false, metadata.elementTypeName(), false, 0L, null);
            Map<String, Object> schema = typed("array");
            Map<String, Object> items = typeSchema(element, elementMetadata, false);
            items.put("optional", true);
            schema.put("items", items);
            return schema;
        }
        if ("hstore".equals(metadata.typeName())) {
            if ("map".equals(modes.hstore())) {
                Map<String, Object> schema = typed("map");
                Map<String, Object> keys = typed("string");
                keys.put("optional", false);
                Map<String, Object> values = typed("string");
                values.put("optional", true);
                schema.put("keys", keys);
                schema.put("values", values);
                return schema;
            }
            return logical("string", "io.debezium.data.Json");
        }
        if ("ltree".equals(metadata.typeName())) {
            return logical("string", "io.debezium.data.Ltree");
        }
        return switch ((int) column.typeOid()) {
            case 16 -> typed("boolean");
            case 17 -> typed("bytes".equals(modes.binary()) ? "bytes" : "string");
            case 20, 26 -> typed("int64");
            case 21 -> typed("int16");
            case 23 -> typed("int32");
            case 700 -> typed("float");
            case 701 -> typed("double");
            case 1082 -> dateSchema();
            case 1083 -> timeSchema(column);
            case 1114 -> timestampSchema(column);
            case 1184 -> logical("string", "io.debezium.time.ZonedTimestamp");
            case 1186 -> "string".equals(modes.interval())
                    ? logical("string", "io.debezium.time.Interval")
                    : logical("int64", "io.debezium.time.MicroDuration");
            case 1266 -> logical("string", "io.debezium.time.ZonedTime");
            case 1560, 1562 -> bitSchema(column);
            case 1700 -> numericSchema(column);
            case 790 -> moneySchema();
            default -> typed("string");
        };
    }

    private static Map<String, Object> bitSchema(Column column) {
        if (column.typeOid() == 1560L && column.typeModifier() == 1) {
            return typed("boolean");
        }
        Map<String, Object> schema = logical("bytes", "io.debezium.data.Bits");
        schema.put("parameters", Map.of("length", Integer.toString(column.typeModifier())));
        return schema;
    }

    private Map<String, Object> dateSchema() {
        return switch (timePrecisionMode.value()) {
            case "connect" -> logical("int32", "org.apache.kafka.connect.data.Date");
            case "isostring" -> logical("string", "io.debezium.time.IsoDate");
            default -> logical("int32", "io.debezium.time.Date");
        };
    }

    private Map<String, Object> timeSchema(Column column) {
        return switch (timePrecisionMode.value()) {
            case "connect" -> logical("int32", "org.apache.kafka.connect.data.Time");
            case "isostring" -> logical("string", "io.debezium.time.IsoTime");
            case "nanoseconds" -> logical("int64", "io.debezium.time.NanoTime");
            case "adaptive" -> temporalPrecision(column) <= 3
                    ? logical("int32", "io.debezium.time.Time")
                    : logical("int64", "io.debezium.time.MicroTime");
            default -> logical("int64", "io.debezium.time.MicroTime");
        };
    }

    private Map<String, Object> timestampSchema(Column column) {
        return switch (timePrecisionMode.value()) {
            case "connect" -> logical("int64", "org.apache.kafka.connect.data.Timestamp");
            case "isostring" -> logical("string", "io.debezium.time.IsoTimestamp");
            case "nanoseconds" -> logical("int64", "io.debezium.time.NanoTimestamp");
            case "adaptive", "adaptive_time_microseconds" ->
                    temporalPrecision(column) <= 3
                            ? logical("int64", "io.debezium.time.Timestamp")
                            : logical("int64", "io.debezium.time.MicroTimestamp");
            default -> logical("int64", "io.debezium.time.MicroTimestamp");
        };
    }

    private static int temporalPrecision(Column column) {
        return column.typeModifier() < 0 ? 6 : column.typeModifier();
    }

    private Map<String, Object> numericSchema(Column column) {
        if ("string".equals(modes.decimal())) {
            return typed("string");
        }
        if ("double".equals(modes.decimal())) {
            return typed("double");
        }
        if (column.typeModifier() < 0) {
            List<Map<String, Object>> fields = List.of(
                    primitiveField("int32", false, "scale"),
                    primitiveField("bytes", false, "value"));
            Map<String, Object> schema = struct(
                    fields, false, "io.debezium.data.VariableScaleDecimal", null);
            schema.put("version", 1);
            schema.put("doc", "Variable scaled decimal");
            return schema;
        }
        int modifier = column.typeModifier() - 4;
        int precision = (modifier >>> 16) & 0xffff;
        int scale = modifier & 0x7ff;
        return decimalSchema(scale, precision);
    }

    private Map<String, Object> decimalSchema(int scale, int precision) {
        if ("string".equals(modes.decimal())) {
            return typed("string");
        }
        if ("double".equals(modes.decimal())) {
            return typed("double");
        }
        Map<String, Object> schema = logical("bytes", "org.apache.kafka.connect.data.Decimal");
        schema.put("parameters", Map.of(
                "scale", Integer.toString(scale),
                "connect.decimal.precision", Integer.toString(precision)));
        return schema;
    }

    private Map<String, Object> moneySchema() {
        if ("string".equals(modes.decimal())) {
            return typed("string");
        }
        if ("double".equals(modes.decimal())) {
            return typed("double");
        }
        Map<String, Object> schema = logical(
                "bytes", "org.apache.kafka.connect.data.Decimal");
        schema.put("parameters", Map.of("scale", "2"));
        return schema;
    }

    private static Map<String, Object> source() {
        List<Map<String, Object>> fields = new ArrayList<>();
        fields.add(primitiveField("string", false, "version"));
        fields.add(primitiveField("string", false, "connector"));
        fields.add(primitiveField("string", false, "name"));
        fields.add(primitiveField("int64", false, "ts_ms"));
        Map<String, Object> snapshot = logical("string", "io.debezium.data.Enum");
        snapshot.put("parameters", Map.of(
                "allowed", "true,first,first_in_data_collection,last_in_data_collection,last,false,incremental"));
        snapshot.put("optional", true);
        snapshot.put("default", "false");
        snapshot.put("field", "snapshot");
        fields.add(snapshot);
        fields.add(primitiveField("string", false, "db"));
        fields.add(primitiveField("string", true, "sequence"));
        fields.add(primitiveField("int64", true, "ts_us"));
        fields.add(primitiveField("int64", true, "ts_ns"));
        fields.add(primitiveField("string", false, "schema"));
        fields.add(primitiveField("string", false, "table"));
        fields.add(primitiveField("int64", true, "txId"));
        fields.add(primitiveField("int64", true, "lsn"));
        fields.add(primitiveField("int64", true, "xmin"));
        fields.add(primitiveField("string", true, "origin"));
        fields.add(primitiveField("int64", true, "origin_lsn"));
        Map<String, Object> source = struct(
                fields, false, "io.debezium.connector.postgresql.Source", "source");
        source.put("version", 1);
        return source;
    }

    private static Map<String, Object> transaction() {
        List<Map<String, Object>> fields = List.of(
                primitiveField("string", false, "id"),
                primitiveField("int64", false, "total_order"),
                primitiveField("int64", false, "data_collection_order"));
        Map<String, Object> transaction = struct(fields, true, "event.block", "transaction");
        transaction.put("version", 1);
        return transaction;
    }

    private static Map<String, Object> primitiveField(String type, boolean optional, String field) {
        Map<String, Object> schema = typed(type);
        schema.put("optional", optional);
        schema.put("field", field);
        return schema;
    }

    private static Map<String, Object> logical(String type, String name) {
        Map<String, Object> schema = typed(type);
        schema.put("name", name);
        schema.put("version", 1);
        return schema;
    }

    private static Map<String, Object> typed(String type) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", type);
        return schema;
    }

    private static Map<String, Object> struct(List<Map<String, Object>> fields, boolean optional,
                                              String name, String field) {
        Map<String, Object> schema = typed("struct");
        schema.put("fields", fields);
        schema.put("optional", optional);
        schema.put("name", name);
        if (field != null) {
            schema.put("field", field);
        }
        return schema;
    }

    record ColumnDefinition(
            Column message, ColumnMetadata metadata, boolean includedInValue,
            Map<String, Object> customSchema, Map<String, String> schemaParameters,
            boolean unknownDatatype) {
        ColumnDefinition(
                Column message, ColumnMetadata metadata, boolean includedInValue,
                Map<String, Object> customSchema, Map<String, String> schemaParameters) {
            this(message, metadata, includedInValue, customSchema, schemaParameters, false);
        }

        ColumnDefinition(Column message, ColumnMetadata metadata) {
            this(message, metadata, true, null, Map.of(), false);
        }
    }

    record RelationSchemas(Map<String, Object> key, Map<String, Object> envelope) {}
}
