package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ColumnFilterTest {
    @Test
    void appliesAnchoredIncludePatterns() {
        ColumnFilter filter = new ColumnFilter(Map.of(
                ColumnFilter.COLUMN_INCLUDE_LIST,
                "inventory\\.orders\\.(id|total),public\\.events\\.payload"));

        assertEquals(true, filter.includes("inventory", "orders", "id"));
        assertEquals(true, filter.includes("inventory", "orders", "total"));
        assertEquals(true, filter.includes("public", "events", "payload"));
        assertEquals(false, filter.includes("inventory", "orders", "created_at"));
        assertEquals(false, filter.includes("archive", "events", "payload"));
    }

    @Test
    void appliesExcludesAndDefaultsToAllColumns() {
        ColumnFilter excluded = new ColumnFilter(Map.of(
                ColumnFilter.COLUMN_EXCLUDE_LIST, ".*\\.secret_.*"));
        ColumnFilter defaults = new ColumnFilter(Map.of());

        assertEquals(false, excluded.includes("public", "accounts", "secret_token"));
        assertEquals(true, excluded.includes("public", "accounts", "id"));
        assertEquals(true, defaults.includes("public", "accounts", "secret_token"));
    }

    @Test
    void rejectsConflictingOrInvalidFilters() {
        assertThrows(ConfigException.class, () -> new ColumnFilter(Map.of(
                ColumnFilter.COLUMN_INCLUDE_LIST, ".*\\.id",
                ColumnFilter.COLUMN_EXCLUDE_LIST, ".*\\.payload")));
        assertThrows(ConfigException.class, () -> new ColumnFilter(Map.of(
                ColumnFilter.COLUMN_INCLUDE_LIST, "[invalid")));
    }
}
