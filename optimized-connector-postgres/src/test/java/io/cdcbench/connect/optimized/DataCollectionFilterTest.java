package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class DataCollectionFilterTest {
    @Test
    void appliesAnchoredTableIncludePatterns() {
        DataCollectionFilter filter = new DataCollectionFilter(Map.of(
                DataCollectionFilter.TABLE_INCLUDE_LIST,
                "inventory\\.orders_[0-9]+,public\\.events"));

        assertEquals(true, filter.includes("inventory", "orders_42"));
        assertEquals(true, filter.includes("public", "events"));
        assertEquals(false, filter.includes("inventory", "orders_current"));
        assertEquals(false, filter.includes("archive", "events"));
    }

    @Test
    void combinesSchemaIncludesWithTableExcludes() {
        DataCollectionFilter filter = new DataCollectionFilter(Map.of(
                DataCollectionFilter.SCHEMA_INCLUDE_LIST, "tenant_.*",
                DataCollectionFilter.TABLE_EXCLUDE_LIST, ".*\\.audit_.*"));

        assertEquals(true, filter.includes("tenant_eu", "orders"));
        assertEquals(false, filter.includes("tenant_eu", "audit_log"));
        assertEquals(false, filter.includes("public", "orders"));
        assertEquals(false, filter.includes("pg_catalog", "pg_class"));
    }

    @Test
    void matchesDotsInsideActualSchemaAndTableNames() {
        DataCollectionFilter filter = new DataCollectionFilter(Map.of(
                DataCollectionFilter.TABLE_INCLUDE_LIST,
                "tenant\\.east\\.Order\\.Items"));

        assertEquals(true, filter.includes("tenant.east", "Order.Items"));
        assertEquals(false, filter.includes("tenant.west", "Order.Items"));
    }

    @Test
    void rejectsConflictingOrInvalidFilters() {
        assertThrows(ConfigException.class, () -> new DataCollectionFilter(Map.of(
                DataCollectionFilter.TABLE_INCLUDE_LIST, "public\\..*",
                DataCollectionFilter.TABLE_EXCLUDE_LIST, "public\\.audit")));
        assertThrows(ConfigException.class, () -> new DataCollectionFilter(Map.of(
                DataCollectionFilter.SCHEMA_INCLUDE_LIST, "[invalid")));
    }
}
