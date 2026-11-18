package io.cdcbench.connect.optimized;

import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

final class OptimizedJsonConverterTest {
    private static final Map<String, Object> CONFIG = Map.of("schemas.enable", false);

    @Test
    void parallelSerializerIsByteIdenticalToOfficialJsonConverter() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("text", "quotes=\" slash=\\ unicode=Привет control=\n");
        nested.put("integer", 42L);
        nested.put("decimal", 1234567890.125d);
        nested.put("binary", "binary-value".getBytes(StandardCharsets.UTF_8));
        nested.put("array", List.of(true, false, "value"));
        nested.put("nullable", null);

        JsonConverter officialKey = official(true);
        JsonConverter officialValue = official(false);
        ParallelJsonSerializer parallel = new ParallelJsonSerializer();

        assertArrayEquals(officialKey.fromConnectData("topic", null, Map.of("id", 7L)),
                parallel.key("topic", Map.of("id", 7L)));
        assertArrayEquals(officialValue.fromConnectData("topic", null, nested),
                parallel.value("topic", nested));
    }

    @Test
    void outputConverterPassesPreSerializedRootBytesWithoutCopying() {
        byte[] serialized = "{\"already\":\"json\"}".getBytes(StandardCharsets.UTF_8);
        OptimizedJsonConverter converter = new OptimizedJsonConverter();
        converter.configure(CONFIG, false);

        assertSame(serialized, converter.fromConnectData("topic", null, serialized));
    }

    @Test
    void outputConverterDelegatesOrdinaryConnectValuesToOfficialConverter() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("id", 9L);
        value.put("payload", "escaped \"value\" Привет");
        JsonConverter official = official(false);
        OptimizedJsonConverter converter = new OptimizedJsonConverter();
        converter.configure(CONFIG, false);

        assertArrayEquals(official.fromConnectData("topic", null, value),
                converter.fromConnectData("topic", null, value));
    }

    @Test
    void rawUtf8PathMatchesOfficialStringSerializationByteForByte() {
        String text = "prefix quotes=\" slash=\\ unicode=Привет control=\n " + "x".repeat(70_000);
        Map<String, Object> officialValue = new LinkedHashMap<>();
        officialValue.put("before", null);
        officialValue.put("after", Map.of("padding", text));
        officialValue.put("op", "c");
        Map<String, Object> optimizedValue = new LinkedHashMap<>();
        optimizedValue.put("before", null);
        optimizedValue.put("after", Map.of(
                "padding", new RawUtf8String(text.getBytes(StandardCharsets.UTF_8))));
        optimizedValue.put("op", "c");

        JsonConverter official = official(false);
        ParallelJsonSerializer parallel = new ParallelJsonSerializer();

        assertArrayEquals(official.fromConnectData("topic", null, officialValue),
                parallel.value("topic", optimizedValue));
    }

    @Test
    void schemaFramingCacheRemainsByteIdenticalAcrossChangingPayloads() {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "struct");
        schema.put("fields", List.of(
                Map.of("type", "int64", "optional", false, "field", "id"),
                Map.of("type", "string", "optional", true, "field", "padding")));
        schema.put("optional", false);
        schema.put("name", "server.public.events.Value");

        Map<String, Object> first = new LinkedHashMap<>();
        first.put("id", 1L);
        first.put("padding", "quote=\" unicode=Привет");
        Map<String, Object> second = new LinkedHashMap<>();
        second.put("id", 2L);
        second.put("padding", "different\\value\n".repeat(100));

        JsonConverter official = official(false);
        ParallelJsonSerializer parallel = new ParallelJsonSerializer();

        assertArrayEquals(official.fromConnectData(
                        "topic", null, schemaEnvelope(schema, first)),
                parallel.valueWithSchema("topic", schema, first));
        assertArrayEquals(official.fromConnectData(
                        "topic", null, schemaEnvelope(schema, second)),
                parallel.valueWithSchema("topic", schema, second));
        assertEquals(1, parallel.schemaFramingCacheSize());
    }

    @Test
    void schemaFramingCachePreservesRawUtf8Payloads() {
        Map<String, Object> schema = Map.of(
                "type", "struct",
                "fields", List.of(Map.of(
                        "type", "string", "optional", true, "field", "padding")),
                "optional", false,
                "name", "server.public.events.Value");
        String firstText = "prefix quotes=\" slash=\\ unicode=Привет control=\n "
                + "x".repeat(70_000);
        String secondText = "second 😀 " + "y".repeat(70_000);
        Map<String, Object> first = Map.of("padding", firstText);
        Map<String, Object> second = Map.of("padding", secondText);
        Map<String, Object> firstRaw = Map.of("padding",
                new RawUtf8String(firstText.getBytes(StandardCharsets.UTF_8)));
        Map<String, Object> secondRaw = Map.of("padding",
                new RawUtf8String(secondText.getBytes(StandardCharsets.UTF_8)));

        JsonConverter official = official(false);
        ParallelJsonSerializer parallel = new ParallelJsonSerializer();

        byte[] firstExpected = official.fromConnectData(
                "topic", null, schemaEnvelope(schema, first));
        byte[] firstActual = parallel.valueWithSchema("topic", schema, firstRaw);
        assertArrayEquals(firstExpected, firstActual);
        assertArrayEquals(official.fromConnectData(
                        "topic", null, schemaEnvelope(schema, second)),
                parallel.valueWithSchema("topic", schema, secondRaw));
        assertEquals(1, parallel.schemaFramingCacheSize());
    }

    @Test
    void schemaFramingCacheSeparatesKeyAndEvolvedSchemaIdentities() {
        Map<String, Object> keySchema = Map.of(
                "type", "struct",
                "fields", List.of(Map.of(
                        "type", "int64", "optional", false, "field", "id")),
                "optional", false,
                "name", "server.public.events.Key");
        Map<String, Object> evolvedKeySchema = new LinkedHashMap<>(keySchema);
        evolvedKeySchema.put("version", 2);

        JsonConverter official = official(true);
        ParallelJsonSerializer parallel = new ParallelJsonSerializer();
        Map<String, Object> first = Map.of("id", 1L);
        Map<String, Object> second = Map.of("id", 2L);

        assertArrayEquals(official.fromConnectData(
                        "topic", null, schemaEnvelope(keySchema, first)),
                parallel.keyWithSchema("topic", keySchema, first));
        assertArrayEquals(official.fromConnectData(
                        "topic", null, schemaEnvelope(keySchema, second)),
                parallel.keyWithSchema("topic", keySchema, second));
        assertArrayEquals(official.fromConnectData(
                        "topic", null, schemaEnvelope(evolvedKeySchema, second)),
                parallel.keyWithSchema("topic", evolvedKeySchema, second));
        assertEquals(2, parallel.schemaFramingCacheSize());
    }

    @Test
    void schemaFramingCacheIsSafeAcrossMaterializerWorkers() throws Exception {
        Map<String, Object> schema = Map.of(
                "type", "struct",
                "fields", List.of(
                        Map.of("type", "int64", "optional", false, "field", "id"),
                        Map.of("type", "string", "optional", false, "field", "padding")),
                "optional", false,
                "name", "server.public.events.Value");
        ParallelJsonSerializer parallel = new ParallelJsonSerializer();
        ExecutorService workers = Executors.newFixedThreadPool(8);
        List<Future<byte[]>> results = new ArrayList<>();
        try {
            for (int index = 0; index < 1_000; index++) {
                long id = index;
                results.add(workers.submit(() -> parallel.valueWithSchema(
                        "topic", schema, Map.of(
                                "id", id,
                                "padding", "value-" + id + "-Привет"))));
            }
            JsonConverter official = official(false);
            for (int index = 0; index < results.size(); index++) {
                Map<String, Object> expectedPayload = Map.of(
                        "id", (long) index,
                        "padding", "value-" + index + "-Привет");
                assertArrayEquals(official.fromConnectData(
                                "topic", null, schemaEnvelope(schema, expectedPayload)),
                        results.get(index).get(30, TimeUnit.SECONDS));
            }
            assertEquals(1, parallel.schemaFramingCacheSize());
        } finally {
            workers.shutdownNow();
            workers.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void schemaFramingCacheRemainsBounded() {
        ParallelJsonSerializer parallel = new ParallelJsonSerializer();
        for (int index = 0; index < 1_001; index++) {
            Map<String, Object> schema = Map.of(
                    "type", "struct",
                    "fields", List.of(Map.of(
                            "type", "int64", "optional", false, "field", "id")),
                    "optional", false,
                    "name", "server.public.events.Value" + index);
            parallel.valueWithSchema("topic", schema, Map.of("id", 1L));
        }
        assertEquals(1_000, parallel.schemaFramingCacheSize());
    }

    private static Map<String, Object> schemaEnvelope(
            Map<String, Object> schema, Object payload) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("schema", schema);
        envelope.put("payload", payload);
        return envelope;
    }

    private static JsonConverter official(boolean isKey) {
        JsonConverter converter = new JsonConverter();
        converter.configure(CONFIG, isKey);
        return converter;
    }
}
