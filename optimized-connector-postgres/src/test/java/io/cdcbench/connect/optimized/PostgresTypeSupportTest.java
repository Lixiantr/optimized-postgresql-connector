package io.cdcbench.connect.optimized;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PostgresTypeSupportTest {
    @Test
    void distinguishesKnownExtensionsEnumsAndUnknownBaseTypes() {
        assertTrue(PostgresTypeSupport.isSupported(
                "ltree", "b", false, null, null, null, null));
        assertTrue(PostgresTypeSupport.isSupported(
                "application_state", "e", false, null, null, null, null));
        assertTrue(PostgresTypeSupport.isSupported(
                "_application_state", "b", true,
                "application_state", "e", null, null));
        assertFalse(PostgresTypeSupport.isSupported(
                "cube", "b", false, null, null, null, null));
        assertFalse(PostgresTypeSupport.isSupported(
                "_cube", "b", true, "cube", "b", null, null));
    }

    @Test
    void resolvesDomainsThroughTheirBaseType() {
        assertTrue(PostgresTypeSupport.isSupported(
                "email_address", "d", false, null, null, "text", "b"));
        assertFalse(PostgresTypeSupport.isSupported(
                "opaque_value", "d", false, null, null, "cube", "b"));
    }
}
