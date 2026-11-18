package io.cdcbench.connect.optimized;

import io.cdcbench.connect.common.ColumnMetadata;
import io.cdcbench.connect.common.PgOutputMessage.Column;
import io.cdcbench.connect.common.PostgresValueConverter;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

final class DebeziumSchemaFactoryTest {
    private final DebeziumSchemaFactory factory =
            new DebeziumSchemaFactory(new PostgresValueConverter());

    @Test
    void buildsTheVersionPinnedKeyRowEnvelopeAndSourceContract() {
        DebeziumSchemaFactory.RelationSchemas schemas = factory.relation(
                "optimized_java", "public", "events", basicColumns());

        assertEquals("optimized_java.public.events.Key", schemas.key().get("name"));
        assertEquals(false, schemas.key().get("optional"));
        assertEquals(List.of(Map.of(
                "type", "int64", "optional", false, "field", "id")),
                schemas.key().get("fields"));

        Map<String, Object> envelope = schemas.envelope();
        assertEquals("optimized_java.public.events.Envelope", envelope.get("name"));
        assertEquals(2, envelope.get("version"));
        List<Map<String, Object>> fields = fields(envelope);
        assertEquals(List.of("before", "after", "source", "transaction",
                        "op", "ts_ms", "ts_us", "ts_ns"),
                fields.stream().map(field -> (String) field.get("field")).toList());
        assertEquals("optimized_java.public.events.Value", fields.get(0).get("name"));
        assertEquals("optimized_java.public.events.Value", fields.get(1).get("name"));
        assertEquals("io.debezium.connector.postgresql.Source", fields.get(2).get("name"));
        assertEquals(1, fields.get(2).get("version"));
        assertEquals("event.block", fields.get(3).get("name"));
        assertEquals(1, fields.get(3).get("version"));
    }

    @Test
    void emitsTheExactDecimalSchemaAndConnectDefault() {
        DebeziumSchemaFactory.RelationSchemas schemas = factory.relation(
                "optimized_java", "public", "events", basicColumns());
        Map<String, Object> amount = fields(fields(schemas.envelope()).get(0)).get(2);

        assertEquals("bytes", amount.get("type"));
        assertEquals("org.apache.kafka.connect.data.Decimal", amount.get("name"));
        assertEquals(1, amount.get("version"));
        assertEquals(Map.of("scale", "4", "connect.decimal.precision", "20"),
                amount.get("parameters"));
        assertArrayEquals(new byte[]{0}, (byte[]) amount.get("default"));
        assertFalse((Boolean) amount.get("optional"));
    }

    @Test
    void omitsTheKafkaKeySchemaWhenTheRelationHasNoIdentityColumns() {
        DebeziumSchemaFactory.RelationSchemas schemas = factory.relation(
                "optimized_java", "public", "without_identity",
                List.of(definition(false, "payload", 25L, -1, false, null)));

        assertNull(schemas.key());
    }

    @Test
    void propagatesSourceTypeParametersOnlyToTheValueSchema() {
        Column column = new Column(true, "id", 20L, -1);
        ColumnMetadata metadata = new ColumnMetadata(
                false, null, true, "int8", false, 0L, null);
        Map<String, String> parameters = Map.of(
                SourceTypePropagator.SOURCE_TYPE, "INT8",
                SourceTypePropagator.SOURCE_NAME, "id",
                SourceTypePropagator.SOURCE_LENGTH, "19",
                SourceTypePropagator.SOURCE_SCALE, "0");
        DebeziumSchemaFactory.RelationSchemas schemas = factory.relation(
                "optimized_java", "public", "events",
                List.of(new DebeziumSchemaFactory.ColumnDefinition(
                        column, metadata, true, null, parameters)));

        assertFalse(fields(schemas.key()).get(0).containsKey("parameters"));
        assertEquals(parameters, fields(fields(schemas.envelope()).get(0)).get(0)
                .get("parameters"));
        assertEquals(parameters, fields(fields(schemas.envelope()).get(1)).get(0)
                .get("parameters"));
    }

    @Test
    void buildsVersionPinnedHeartbeatSchemas() {
        assertEquals(Map.of(
                        "type", "struct",
                        "fields", List.of(Map.of(
                                "type", "string", "optional", false, "field", "serverName")),
                        "optional", false,
                        "name", "io.debezium.connector.common.ServerNameKey",
                        "version", 1),
                DebeziumSchemaFactory.heartbeatKey());
        assertEquals(Map.of(
                        "type", "struct",
                        "fields", List.of(Map.of(
                                "type", "int64", "optional", false, "field", "ts_ms")),
                        "optional", false,
                        "name", "io.debezium.connector.common.Heartbeat",
                        "version", 1),
                DebeziumSchemaFactory.heartbeatValue());
    }

    @Test
    void buildsVersionPinnedTransactionMetadataSchemas() {
        Map<String, Object> key = DebeziumSchemaFactory.transactionMetadataKey();
        assertEquals("io.debezium.connector.common.TransactionMetadataKey", key.get("name"));
        assertEquals(1, key.get("version"));
        assertEquals(List.of(Map.of(
                "type", "string", "optional", false, "field", "id")), key.get("fields"));

        Map<String, Object> value = DebeziumSchemaFactory.transactionMetadataValue();
        assertEquals("io.debezium.connector.common.TransactionMetadataValue", value.get("name"));
        assertEquals(1, value.get("version"));
        List<Map<String, Object>> valueFields = fields(value);
        assertEquals(List.of("status", "id", "event_count", "data_collections", "ts_ms"),
                valueFields.stream().map(field -> (String) field.get("field")).toList());
        @SuppressWarnings("unchecked")
        Map<String, Object> items = (Map<String, Object>) valueFields.get(3).get("items");
        assertEquals("event.collection", items.get("name"));
        assertEquals(1, items.get("version"));
        assertEquals(true, items.get("optional"));
    }

    @Test
    void buildsVersionPinnedLogicalMessageSchemas() {
        Map<String, Object> key = DebeziumSchemaFactory.logicalMessageKey();
        assertEquals("io.debezium.connector.postgresql.MessageKey", key.get("name"));
        assertEquals(1, key.get("version"));
        assertEquals(List.of(Map.of(
                "type", "string", "optional", true, "field", "prefix")), key.get("fields"));

        Map<String, Object> value = DebeziumSchemaFactory.logicalMessageValue();
        assertEquals("io.debezium.connector.postgresql.MessageValue", value.get("name"));
        assertEquals(1, value.get("version"));
        List<Map<String, Object>> valueFields = fields(value);
        assertEquals(List.of("op", "ts_ms", "source", "transaction", "message"),
                valueFields.stream().map(field -> (String) field.get("field")).toList());
        assertEquals("io.debezium.connector.postgresql.Source", valueFields.get(2).get("name"));
        assertEquals("event.block", valueFields.get(3).get("name"));
        assertEquals("io.debezium.connector.postgresql.Message", valueFields.get(4).get("name"));
        assertEquals(1, valueFields.get(4).get("version"));
        assertEquals(List.of("prefix", "content"), fields(valueFields.get(4)).stream()
                .map(field -> (String) field.get("field")).toList());
        assertEquals("bytes", fields(valueFields.get(4)).get(1).get("type"));
    }

    @Test
    void emitsExactStringHexMapAndIntervalStringSchemas() {
        ValueHandlingModes modes = new ValueHandlingModes(Map.of(
                ValueHandlingModes.DECIMAL, "string",
                ValueHandlingModes.BINARY, "hex",
                ValueHandlingModes.HSTORE, "map",
                ValueHandlingModes.INTERVAL, "string"));
        DebeziumSchemaFactory modeFactory = new DebeziumSchemaFactory(
                modes.converter(), modes);
        ColumnMetadata hstore = new ColumnMetadata(
                true, null, false, "hstore", false, 0L, null);
        List<DebeziumSchemaFactory.ColumnDefinition> columns = List.of(
                definition(true, "id", 20L, -1, false, null),
                definition(false, "amount", 1700L,
                        4 + (10 << 16) + 3, true, null),
                definition(false, "bytes_value", 17L, -1, true, null),
                new DebeziumSchemaFactory.ColumnDefinition(
                        new Column(false, "properties", 99_999L, -1), hstore),
                definition(false, "duration", 1186L, -1, true, null));

        List<Map<String, Object>> row = fields(fields(modeFactory.relation(
                "optimized_java", "public", "events", columns).envelope()).get(0));
        assertEquals("string", row.get(1).get("type"));
        assertEquals("string", row.get(2).get("type"));
        assertEquals(Map.of("type", "string", "optional", false),
                row.get(3).get("keys"));
        assertEquals(Map.of("type", "string", "optional", true),
                row.get(3).get("values"));
        assertEquals("map", row.get(3).get("type"));
        assertEquals("string", row.get(4).get("type"));
        assertEquals("io.debezium.time.Interval", row.get(4).get("name"));
    }

    @Test
    void emitsExactDoubleAndDefaultLogicalSchemas() {
        ValueHandlingModes doubleModes = new ValueHandlingModes(Map.of(
                ValueHandlingModes.DECIMAL, "double"));
        DebeziumSchemaFactory doubleFactory = new DebeziumSchemaFactory(
                doubleModes.converter(), doubleModes);
        Map<String, Object> numeric = fields(fields(doubleFactory.relation(
                "optimized_java", "public", "events",
                List.of(definition(false, "amount", 1700L, -1, true, null)))
                .envelope()).get(0)).get(0);
        assertEquals("double", numeric.get("type"));

        ColumnMetadata hstore = new ColumnMetadata(
                true, null, false, "hstore", false, 0L, null);
        List<Map<String, Object>> defaults = fields(fields(factory.relation(
                "optimized_java", "public", "events", List.of(
                        definition(false, "arbitrary", 1700L, -1, true, null),
                        new DebeziumSchemaFactory.ColumnDefinition(
                                new Column(false, "properties", 99_999L, -1), hstore)))
                .envelope()).get(0));
        assertEquals("Variable scaled decimal", defaults.get(0).get("doc"));
        assertEquals("io.debezium.data.Json", defaults.get(1).get("name"));
        assertEquals(1, defaults.get(1).get("version"));
    }

    @Test
    void emitsBytesForIncludedUnknownTypesAndLtreeForKnownExtensionTypes() {
        ColumnMetadata cube = new ColumnMetadata(
                true, null, false, "cube", false, 0L, null);
        ColumnMetadata cubes = new ColumnMetadata(
                true, null, false, "_cube", true, 99_999L, "cube");
        ColumnMetadata ltree = new ColumnMetadata(
                true, null, false, "ltree", false, 0L, null);
        List<DebeziumSchemaFactory.ColumnDefinition> columns = List.of(
                new DebeziumSchemaFactory.ColumnDefinition(
                        new Column(false, "unknown_cube", 88_001L, -1), cube,
                        true, null, Map.of(), true),
                new DebeziumSchemaFactory.ColumnDefinition(
                        new Column(false, "unknown_cubes", 88_002L, -1), cubes,
                        true, null, Map.of(), true),
                new DebeziumSchemaFactory.ColumnDefinition(
                        new Column(false, "known_path", 88_003L, -1), ltree));

        List<Map<String, Object>> row = fields(fields(factory.relation(
                "optimized_java", "public", "events", columns).envelope()).get(0));
        assertEquals("bytes", row.get(0).get("type"));
        assertEquals("array", row.get(1).get("type"));
        assertEquals(Map.of("type", "bytes", "optional", false),
                row.get(1).get("items"));
        assertEquals("string", row.get(2).get("type"));
        assertEquals("io.debezium.data.Ltree", row.get(2).get("name"));
    }

    @Test
    void omitsPrecisionFromThePinnedMoneyDecimalSchema() {
        Map<String, Object> money = fields(fields(factory.relation(
                "optimized_java", "public", "events",
                List.of(definition(false, "price", 790L, -1, true, null)))
                .envelope()).get(0)).get(0);

        assertEquals("bytes", money.get("type"));
        assertEquals("org.apache.kafka.connect.data.Decimal", money.get("name"));
        assertEquals(Map.of("scale", "2"), money.get("parameters"));
    }

    @Test
    void emitsExactTemporalSchemasForEveryPrecisionMode() {
        List<DebeziumSchemaFactory.ColumnDefinition> columns = List.of(
                definition(false, "date_value", 1082L, -1, true, null),
                definition(false, "time_3", 1083L, 3, true, null),
                definition(false, "time_6", 1083L, 6, true, null),
                definition(false, "timestamp_3", 1114L, 3, true, null),
                definition(false, "timestamp_6", 1114L, 6, true, null));

        List<Map<String, Object>> adaptive = temporalFields("adaptive", columns);
        assertTemporal(adaptive.get(0), "int32", "io.debezium.time.Date");
        assertTemporal(adaptive.get(1), "int32", "io.debezium.time.Time");
        assertTemporal(adaptive.get(2), "int64", "io.debezium.time.MicroTime");
        assertTemporal(adaptive.get(3), "int64", "io.debezium.time.Timestamp");
        assertTemporal(adaptive.get(4), "int64", "io.debezium.time.MicroTimestamp");

        List<Map<String, Object>> adaptiveTime = temporalFields(
                "adaptive_time_microseconds", columns);
        assertTemporal(adaptiveTime.get(1), "int64", "io.debezium.time.MicroTime");
        assertTemporal(adaptiveTime.get(3), "int64", "io.debezium.time.Timestamp");

        List<Map<String, Object>> connect = temporalFields("connect", columns);
        assertTemporal(connect.get(0), "int32", "org.apache.kafka.connect.data.Date");
        assertTemporal(connect.get(1), "int32", "org.apache.kafka.connect.data.Time");
        assertTemporal(connect.get(4), "int64", "org.apache.kafka.connect.data.Timestamp");

        List<Map<String, Object>> iso = temporalFields("isostring", columns);
        assertTemporal(iso.get(0), "string", "io.debezium.time.IsoDate");
        assertTemporal(iso.get(1), "string", "io.debezium.time.IsoTime");
        assertTemporal(iso.get(4), "string", "io.debezium.time.IsoTimestamp");

        List<Map<String, Object>> nanos = temporalFields("nanoseconds", columns);
        assertTemporal(nanos.get(1), "int64", "io.debezium.time.NanoTime");
        assertTemporal(nanos.get(4), "int64", "io.debezium.time.NanoTimestamp");
    }

    private static List<Map<String, Object>> temporalFields(
            String mode, List<DebeziumSchemaFactory.ColumnDefinition> columns) {
        ValueHandlingModes valueModes = new ValueHandlingModes(Map.of());
        TimePrecisionMode timeMode = new TimePrecisionMode(
                Map.of(TimePrecisionMode.PROPERTY, mode));
        DebeziumSchemaFactory temporalFactory = new DebeziumSchemaFactory(
                valueModes.converter(timeMode), valueModes, timeMode);
        return fields(fields(temporalFactory.relation(
                "optimized_java", "public", "events", columns).envelope()).get(0));
    }

    private static void assertTemporal(
            Map<String, Object> schema, String type, String name) {
        assertEquals(type, schema.get("type"));
        assertEquals(name, schema.get("name"));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> fields(Map<String, Object> schema) {
        return (List<Map<String, Object>>) schema.get("fields");
    }

    private static List<DebeziumSchemaFactory.ColumnDefinition> basicColumns() {
        int numeric20Scale4 = 4 + (20 << 16) + 4;
        return List.of(
                definition(true, "id", 20L, -1, false, null),
                definition(false, "payload", 25L, -1, false, null),
                definition(false, "amount", 1700L, numeric20Scale4, false, "0"));
    }

    private static DebeziumSchemaFactory.ColumnDefinition definition(
            boolean key, String name, long oid, int modifier,
            boolean nullable, String defaultExpression) {
        Column column = new Column(key, name, oid, modifier);
        ColumnMetadata metadata = new ColumnMetadata(
                nullable, defaultExpression, key, null, false, 0L, null);
        return new DebeziumSchemaFactory.ColumnDefinition(column, metadata);
    }
}
