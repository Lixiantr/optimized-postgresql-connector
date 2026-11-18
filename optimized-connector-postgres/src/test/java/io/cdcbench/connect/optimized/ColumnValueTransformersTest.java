package io.cdcbench.connect.optimized;

import io.cdcbench.connect.common.PgOutputMessage.Column;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ColumnValueTransformersTest {
    @Test
    void masksNullAndLeavesKafkaKeyDecisionToTheCaller() {
        ColumnValueTransformers transformers = new ColumnValueTransformers(Map.of(
                "column.mask.with.5.chars", "public\\.events\\.secret"));

        ColumnValueTransformers.Binding binding = binding(
                transformers, "secret", 25L, -1);
        assertEquals("*****", binding.convert("value"));
        assertEquals("*****", binding.convert(null));
        assertEquals(Map.of("masked", "true"), binding.schemaParameters());
    }

    @Test
    void truncatesJavaCharactersAndRawUtf8() {
        ColumnValueTransformers transformers = new ColumnValueTransformers(Map.of(
                "column.truncate.to.4.chars", "public\\.events\\.payload"));
        ColumnValueTransformers.Binding binding = binding(
                transformers, "payload", 25L, -1);

        assertEquals("A😀Б", binding.convert("A😀БВГ"));
        assertEquals("abcd", binding.convert(new RawUtf8String(
                "abcdefgh".getBytes(StandardCharsets.UTF_8))));
        assertNull(binding.convert(null));
        assertEquals(Map.of("truncateLength", "4"), binding.schemaParameters());
    }

    @Test
    void reproducesLegacyAndVersionTwoSaltedHashes() {
        ColumnValueTransformers transformers = new ColumnValueTransformers(Map.of(
                "column.mask.hash.SHA-256.with.salt.pepper", "public\\.events\\.legacy",
                "column.mask.hash.v2.SHA-256.with.salt.pepper", "public\\.events\\.current"));

        assertEquals("54a65a4610f2169f17f86c8439546a1f2157874542e341f033bac495a52da329",
                binding(transformers, "legacy", 25L, -1).convert("sensitive"));
        assertEquals("f7a984f0cb6f7cde3132e94cc2fc0c28f9e51dddd5e1949b0f623b58198b0d8b",
                binding(transformers, "current", 25L, -1).convert("sensitive"));
    }

    @Test
    void shortensHashToDeclaredCharacterLength() {
        ColumnValueTransformers transformers = new ColumnValueTransformers(Map.of(
                "column.mask.hash.SHA-256.with.salt.pepper", "public\\.events\\.secret"));

        assertEquals("8e0089c1",
                binding(transformers, "secret", 1043L, 12).convert("secret12"));
    }

    @Test
    void usesDebeziumMapperPrecedenceAndFirstRule() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("column.mask.with.3.chars", "public\\.events\\.same");
        properties.put("column.mask.with.8.chars", "public\\.events\\.(same|mixed)");
        properties.put("column.truncate.to.4.chars", "public\\.events\\.mixed");
        ColumnValueTransformers transformers = new ColumnValueTransformers(properties);

        assertEquals("***", binding(transformers, "same", 25L, -1).convert("abcdef"));
        assertEquals("abcd", binding(transformers, "mixed", 25L, -1).convert("abcdef"));
    }

    @Test
    void ignoresNonCharacterColumnsAndRejectsInvalidRules() {
        ColumnValueTransformers transformers = new ColumnValueTransformers(Map.of(
                "column.mask.with.3.chars", "public\\.events\\.id"));
        assertNull(transformers.binding(
                "public", "events", new Column(false, "id", 23L, -1), 23L));

        assertThrows(ConfigException.class, () -> new ColumnValueTransformers(Map.of(
                "column.mask.with.999999999999.chars", "public\\.events\\.secret")));
        assertThrows(ConfigException.class, () -> new ColumnValueTransformers(Map.of(
                "column.truncate.to.3.chars", "[invalid")));
        assertThrows(ConfigException.class, () -> new ColumnValueTransformers(Map.of(
                "column.mask.hash.not-an-algorithm.with.salt.pepper",
                "public\\.events\\.secret")));
        assertTrue(ColumnValueTransformers.isProperty(
                "column.mask.hash.v2.SHA-512.with.salt.some.salt"));
    }

    private static ColumnValueTransformers.Binding binding(
            ColumnValueTransformers transformers, String name, long typeOid, int typeModifier) {
        ColumnValueTransformers.Binding binding = transformers.binding(
                "public", "events", new Column(false, name, typeOid, typeModifier), typeOid);
        if (binding == null) {
            throw new AssertionError("Expected a column transformer binding for " + name);
        }
        return binding;
    }
}
