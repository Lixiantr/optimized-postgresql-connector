package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class EventProcessingFailureHandlingModeTest {
    @Test
    void failsByDefault() {
        EventProcessingFailureHandlingMode mode = EventProcessingFailureHandlingMode.from(Map.of());

        assertEquals(EventProcessingFailureHandlingMode.FAIL, mode);
        assertEquals("fail", mode.value());
        assertFalse(mode.skipsFailedEvents());
    }

    @Test
    void acceptsEveryPublishedMode() {
        assertEquals(EventProcessingFailureHandlingMode.FAIL, mode("fail"));
        assertEquals(EventProcessingFailureHandlingMode.WARN, mode("warn"));
        assertEquals(EventProcessingFailureHandlingMode.SKIP, mode("skip"));
        assertTrue(mode("warn").skipsFailedEvents());
        assertTrue(mode("skip").skipsFailedEvents());
    }

    @Test
    void rejectsUnknownModes() {
        assertThrows(ConfigException.class, () -> mode("ignore"));
    }

    private static EventProcessingFailureHandlingMode mode(String value) {
        return EventProcessingFailureHandlingMode.from(
                Map.of(EventProcessingFailureHandlingMode.PROPERTY, value));
    }
}
