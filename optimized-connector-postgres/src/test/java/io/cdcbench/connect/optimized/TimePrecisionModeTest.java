package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class TimePrecisionModeTest {
    @Test
    void usesAdaptiveByDefault() {
        assertEquals("adaptive", new TimePrecisionMode(Map.of()).value());
    }

    @Test
    void acceptsEveryPublishedMode() {
        for (String mode : new String[]{
                "adaptive", "adaptive_time_microseconds", "connect",
                "isostring", "microseconds", "nanoseconds"}) {
            assertEquals(mode, new TimePrecisionMode(
                    Map.of(TimePrecisionMode.PROPERTY, mode)).value());
        }
    }

    @Test
    void rejectsUnknownModes() {
        assertThrows(ConfigException.class, () -> new TimePrecisionMode(
                Map.of(TimePrecisionMode.PROPERTY, "milliseconds")));
    }
}
