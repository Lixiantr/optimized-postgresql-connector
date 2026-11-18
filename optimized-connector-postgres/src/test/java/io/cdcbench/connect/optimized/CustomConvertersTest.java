package io.cdcbench.connect.optimized;

import io.cdcbench.connect.common.ColumnMetadata;
import io.cdcbench.connect.common.PgOutputMessage.Column;
import io.cdcbench.connect.common.PostgresValueConverter;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class CustomConvertersTest {
    @Test
    void configuresAndAppliesTheFirstMatchingPublicSpiConverter() {
        CustomConverters converters = new CustomConverters(Map.of(
                CustomConverters.CONVERTERS, "prefix",
                "prefix.type", TestStringConverter.class.getName(),
                "prefix.prefix", "qualified:"));
        Column payload = new Column(false, "payload", 25L, -1);
        ColumnMetadata metadata = new ColumnMetadata(
                false, null, false, "text", false, 0L, null);

        CustomConverters.Binding binding = converters.binding(
                "public", "events", payload, metadata, "text", 25L,
                new PostgresValueConverter());

        assertNotNull(binding);
        assertEquals("qualified:HELLO", binding.convert("hello"));
        assertEquals(Map.of(
                "type", "string",
                "name", "io.cdcbench.test.PrefixedText",
                "parameters", Map.of("converter", "test-string"),
                "optional", false,
                "field", "payload"), binding.schemaDescription("payload"));
    }

    @Test
    void leavesNonMatchingColumnsOnTheDefaultConversionPath() {
        CustomConverters converters = new CustomConverters(Map.of(
                CustomConverters.CONVERTERS, "prefix",
                "prefix.type", TestStringConverter.class.getName()));
        Column other = new Column(false, "other", 25L, -1);
        ColumnMetadata metadata = new ColumnMetadata(
                true, null, false, "text", false, 0L, null);

        assertNull(converters.binding(
                "public", "events", other, metadata, "text", 25L,
                new PostgresValueConverter()));
    }

    @Test
    void rejectsInvalidAliasesAndConverterClasses() {
        assertThrows(ConfigException.class, () -> new CustomConverters(Map.of(
                CustomConverters.CONVERTERS, "bad.alias",
                "bad.alias.type", TestStringConverter.class.getName())));
        assertThrows(ConfigException.class, () -> new CustomConverters(Map.of(
                CustomConverters.CONVERTERS, "missing")));
        assertThrows(ConfigException.class, () -> new CustomConverters(Map.of(
                CustomConverters.CONVERTERS, "string",
                "string.type", String.class.getName())));
    }
}
