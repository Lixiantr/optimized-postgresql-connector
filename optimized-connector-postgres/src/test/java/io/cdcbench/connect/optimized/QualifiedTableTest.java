package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class QualifiedTableTest {
    @Test
    void parsesSimpleAndQuotedIdentifiersReversibly() {
        QualifiedTable simple = QualifiedTable.parse("public.events");
        QualifiedTable dotted = QualifiedTable.parse(
                "\"schema.with.dot\".\"table.with.dot\"");
        QualifiedTable escaped = QualifiedTable.parse("\"a\"\"b\".plain");

        assertEquals("public", simple.schema());
        assertEquals("public.events", simple.canonicalName());
        assertEquals("\"public\".\"events\"", simple.sqlName());
        assertEquals("schema.with.dot.table.with.dot", dotted.filterName());
        assertEquals("\"schema.with.dot\".\"table.with.dot\"", dotted.canonicalName());
        assertEquals(dotted, QualifiedTable.parse(dotted.canonicalName()));
        assertEquals("a\"b", escaped.schema());
        assertEquals(escaped, QualifiedTable.parse(escaped.canonicalName()));
    }

    @Test
    void preservesQuotedCaseAndQuotesNonCanonicalSimpleNames() {
        QualifiedTable table = QualifiedTable.parse("\"Tenant\".\"Order Items\"");

        assertEquals("Tenant", table.schema());
        assertEquals("Order Items", table.table());
        assertEquals("\"Tenant\".\"Order Items\"", table.canonicalName());
    }

    @Test
    void rejectsAmbiguousOrMalformedNames() {
        for (String value : new String[]{
                "public", "public.events.extra", ".events", "public.",
                "\"unterminated.events", "\"\".events", "public.*",
                "public,other.events"
        }) {
            assertThrows(IllegalArgumentException.class,
                    () -> QualifiedTable.parse(value), value);
        }
        assertThrows(ConfigException.class,
                () -> QualifiedTable.parse("public.events.extra", "signal.data.collection"));
    }
}
