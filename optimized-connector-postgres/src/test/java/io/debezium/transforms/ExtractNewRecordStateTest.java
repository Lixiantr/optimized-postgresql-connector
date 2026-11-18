package io.debezium.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

final class ExtractNewRecordStateTest {
    @Test
    void rewritesSchemalessDeleteAndDropsItsTombstone() {
        ExtractNewRecordState<SourceRecord> transform = transform();
        Map<String, Object> before = new LinkedHashMap<>();
        before.put("id", 1L);
        before.put("payload", null);
        SourceRecord result = transform.apply(record(Map.of(
                "before", before,
                "after", Map.of(),
                "source", Map.of("table", "events", "lsn", 42L),
                "op", "d")));

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("id", 1L);
        expected.put("payload", null);
        expected.put("__deleted", "true");
        expected.put("__op", "d");
        expected.put("__table", "events");
        expected.put("__lsn", 42L);
        assertEquals(expected, result.value());
        assertNull(transform.apply(record(null)));
    }

    @Test
    void preservesSchemaNameAndAddsOptionalMetadataFields() {
        ExtractNewRecordState<SourceRecord> transform = transform();
        Schema rowSchema = SchemaBuilder.struct().name("server.public.events.Value")
                .field("id", Schema.INT64_SCHEMA)
                .field("payload", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        Schema sourceSchema = SchemaBuilder.struct()
                .field("table", Schema.STRING_SCHEMA)
                .field("lsn", Schema.INT64_SCHEMA)
                .build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();
        Struct after = new Struct(rowSchema).put("id", 1L).put("payload", "a");
        Struct source = new Struct(sourceSchema).put("table", "events").put("lsn", 43L);
        Struct envelope = new Struct(envelopeSchema)
                .put("before", null).put("after", after).put("source", source).put("op", "c");

        SourceRecord result = transform.apply(record(envelopeSchema, envelope));
        Struct value = (Struct) result.value();
        assertEquals("server.public.events.Value", result.valueSchema().name());
        assertEquals(List.of("id", "payload", "__deleted", "__op", "__table", "__lsn"),
                result.valueSchema().fields().stream().map(field -> field.name()).toList());
        assertEquals("false", value.get("__deleted"));
        assertEquals("c", value.get("__op"));
        assertEquals("events", value.get("__table"));
        assertEquals(43L, value.get("__lsn"));
    }

    private static ExtractNewRecordState<SourceRecord> transform() {
        ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>();
        transform.configure(Map.of(
                "delete.tombstone.handling.mode", "rewrite",
                "add.fields", List.of("op", "table", "lsn")));
        return transform;
    }

    private static SourceRecord record(Object value) {
        return record(null, value);
    }

    private static SourceRecord record(Schema valueSchema, Object value) {
        return new SourceRecord(Map.of("server", "test"), Map.of("lsn", 1L),
                "topic", null, null, null, valueSchema, value, 1L);
    }
}
