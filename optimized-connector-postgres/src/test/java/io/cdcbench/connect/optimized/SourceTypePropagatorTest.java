package io.cdcbench.connect.optimized;

import io.cdcbench.connect.common.ColumnMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SourceTypePropagatorTest {
    @Test
    void selectsAnchoredColumnAndDatatypeNames() {
        SourceTypePropagator propagator = new SourceTypePropagator("benchmark", Map.of(
                SourceTypePropagator.COLUMN_PROPERTY,
                "benchmark\\.public\\.events\\.name",
                SourceTypePropagator.DATATYPE_PROPERTY,
                "(?i)benchmark\\.public\\.events\\.numeric"));

        assertTrue(propagator.enabled());

        assertEquals(Map.of(
                        SourceTypePropagator.SOURCE_TYPE, "VARCHAR",
                        SourceTypePropagator.SOURCE_NAME, "name",
                        SourceTypePropagator.SOURCE_LENGTH, "12",
                        SourceTypePropagator.SOURCE_SCALE, "0"),
                propagator.parameters("public", "events",
                        "name", metadata("varchar"),
                        new SourceTypePropagator.JdbcMetadata(12, 0)));
        assertEquals(Map.of(
                        SourceTypePropagator.SOURCE_TYPE, "NUMERIC",
                        SourceTypePropagator.SOURCE_NAME, "amount",
                        SourceTypePropagator.SOURCE_LENGTH, "10",
                        SourceTypePropagator.SOURCE_SCALE, "3"),
                propagator.parameters("public", "events",
                        "amount", metadata("numeric"),
                        new SourceTypePropagator.JdbcMetadata(10, 3)));
        assertEquals(Map.of(), propagator.parameters("public", "events",
                "other", metadata("text"),
                new SourceTypePropagator.JdbcMetadata(Integer.MAX_VALUE, 0)));
    }

    @Test
    void usesJdbcMetadataWithoutPostgresTypeSpecificRules() {
        SourceTypePropagator propagator = new SourceTypePropagator("benchmark", Map.of(
                SourceTypePropagator.COLUMN_PROPERTY, ".*"));

        assertEquals(Map.of(
                        SourceTypePropagator.SOURCE_TYPE, "TEXT",
                        SourceTypePropagator.SOURCE_NAME, "payload",
                        SourceTypePropagator.SOURCE_LENGTH, Integer.toString(Integer.MAX_VALUE),
                        SourceTypePropagator.SOURCE_SCALE, "0"),
                propagator.parameters("public", "events", "payload", metadata("text"),
                        new SourceTypePropagator.JdbcMetadata(Integer.MAX_VALUE, 0)));
        assertEquals(Map.of(
                        SourceTypePropagator.SOURCE_TYPE, "INT8",
                        SourceTypePropagator.SOURCE_NAME, "id"),
                propagator.parameters("public", "events", "id", metadata("int8"), null));
    }

    @Test
    void rejectsInvalidExpressions() {
        assertThrows(ConfigException.class, () -> new SourceTypePropagator(
                "benchmark", Map.of(SourceTypePropagator.COLUMN_PROPERTY, "[invalid")));
    }

    @Test
    void remainsDisabledWithoutPropagationProperties() {
        assertFalse(new SourceTypePropagator("benchmark", Map.of()).enabled());
    }

    private static ColumnMetadata metadata(String typeName) {
        return new ColumnMetadata(true, null, false, typeName, false, 0L, null);
    }
}
