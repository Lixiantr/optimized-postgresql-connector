package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ValueHandlingModesTest {
    @Test
    void usesDebeziumDefaults() {
        ValueHandlingModes modes = new ValueHandlingModes(Map.of());

        assertEquals("precise", modes.decimal());
        assertEquals("bytes", modes.binary());
        assertEquals("json", modes.hstore());
        assertEquals("numeric", modes.interval());
    }

    @Test
    void acceptsEveryPublishedMode() {
        ValueHandlingModes modes = new ValueHandlingModes(Map.of(
                ValueHandlingModes.DECIMAL, "double",
                ValueHandlingModes.BINARY, "base64-url-safe",
                ValueHandlingModes.HSTORE, "map",
                ValueHandlingModes.INTERVAL, "string"));

        assertEquals("double", modes.decimal());
        assertEquals("base64-url-safe", modes.binary());
        assertEquals("map", modes.hstore());
        assertEquals("string", modes.interval());
    }

    @Test
    void rejectsUnknownModes() {
        assertThrows(ConfigException.class, () -> new ValueHandlingModes(
                Map.of(ValueHandlingModes.DECIMAL, "lossy")));
    }
}
