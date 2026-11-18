package io.cdcbench.connect.optimized;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static io.cdcbench.connect.common.PgOutputMessage.TupleValue;
import static io.cdcbench.connect.common.PgOutputMessage.ValueKind;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class OptimizedPostgresTaskTest {
    @Test
    void exposesTheObservedLogicalMessageTransactionCollectionShape() {
        assertTrue(Pattern.matches(
                "io\\.debezium\\.connector\\.postgresql\\."
                        + "LogicalDecodingMessageMonitor\\$LogicalDecodingMessageId@[0-9a-f]+",
                OptimizedPostgresTask.logicalMessageDataCollection()));
    }

    @Test
    void selectsAsyncReaderOnlyForLargePgoutputMessages() {
        assertFalse(OptimizedPostgresTask.shouldStartAsyncReader(64 * 1024 - 1));
        assertTrue(OptimizedPostgresTask.shouldStartAsyncReader(64 * 1024));
        assertTrue(OptimizedPostgresTask.shouldStartAsyncReader(1024 * 1024));
    }

    @Test
    void snapshotCollectionOffsetsPreferUnambiguousJsonAndReadLegacyValues() {
        assertEquals(List.of("public.events", "inventory.orders"),
                OptimizedPostgresTask.parseOffsetCollections(
                        null, "public.events,inventory.orders"));
        assertEquals(List.of("\"schema.with.dot\".\"table,with,comma\""),
                OptimizedPostgresTask.parseOffsetCollections(
                        "[\"\\\"schema.with.dot\\\".\\\"table,with,comma\\\"\"]",
                        "ignored.legacy"));
        assertThrows(ConnectException.class,
                () -> OptimizedPostgresTask.parseOffsetCollections(
                        "[\"public.events.extra\"]", null));
        assertThrows(ConnectException.class,
                () -> OptimizedPostgresTask.parseOffsetCollections("null", null));
    }

    @Test
    void usesRawUtf8OnlyForLargePostgresTextValues() {
        TupleValue below = new TupleValue(ValueKind.TEXT, new byte[8 * 1024 - 1]);
        TupleValue boundary = new TupleValue(ValueKind.TEXT, new byte[8 * 1024]);
        TupleValue binary = new TupleValue(ValueKind.BINARY, new byte[8 * 1024]);

        assertFalse(OptimizedPostgresTask.shouldUseRawUtf8(25L, below));
        assertTrue(OptimizedPostgresTask.shouldUseRawUtf8(25L, boundary));
        assertFalse(OptimizedPostgresTask.shouldUseRawUtf8(1043L, boundary));
        assertFalse(OptimizedPostgresTask.shouldUseRawUtf8(25L, binary));
    }

    @Test
    void retainsOverflowSafeLegacyByteAccountingHelper() {
        assertEquals(1_024L, OptimizedPostgresTask.checkedTransactionBytes(0L, 1_024L, 2_048L));
        assertEquals(2_048L, OptimizedPostgresTask.checkedTransactionBytes(1_024L, 1_024L, 2_048L));
        assertThrows(ConnectException.class,
                () -> OptimizedPostgresTask.checkedTransactionBytes(2_048L, 1L, 2_048L));
        assertThrows(ConnectException.class,
                () -> OptimizedPostgresTask.checkedTransactionBytes(Long.MAX_VALUE, 1L, Long.MAX_VALUE));
    }

    @Test
    void assignsDebeziumSnapshotMarkersAcrossTheGlobalRowStream() {
        assertEquals("first", OptimizedPostgresTask.snapshotMarker(true, true));
        assertEquals("true", OptimizedPostgresTask.snapshotMarker(false, true));
        assertEquals("last", OptimizedPostgresTask.snapshotMarker(false, false));
        assertEquals("last", OptimizedPostgresTask.snapshotMarker(true, false));
    }

    @Test
    void createsTypedIncrementalKeyParameters() {
        assertEquals("CAST(? AS bigint)",
                OptimizedPostgresTask.typedParameterExpression(List.of("bigint")));
        assertEquals("(CAST(? AS date),CAST(? AS uuid),CAST(? AS numeric(10,2)))",
                OptimizedPostgresTask.typedParameterExpression(
                        List.of("date", "uuid", "numeric(10,2)")));
        assertThrows(ConnectException.class,
                () -> OptimizedPostgresTask.typedParameterExpression(List.of("")));
    }

    @Test
    void expandsPgoutputTransactionIdsAcrossWraparound() {
        long epochSize = 1L << 32;

        assertEquals(epochSize + 42L,
                OptimizedPostgresTask.expandTransactionId(42L, epochSize + 100L));
        assertEquals(epochSize + 7L,
                OptimizedPostgresTask.expandTransactionId(7L, epochSize - 5L));
        assertEquals(epochSize - 7L,
                OptimizedPostgresTask.expandTransactionId(
                        epochSize - 7L, epochSize + 5L));
    }

    @Test
    void migratesLegacyIncrementalKeyOffsetValuesToText() {
        Date date = Date.valueOf("2026-08-02");
        Timestamp timestamp = Timestamp.valueOf("2026-08-02 03:04:05");

        assertEquals("2026-08-02",
                OptimizedPostgresTask.normalizeStoredKeyValue(date.getTime(), 1082L));
        assertEquals("2026-08-02 03:04:05.0",
                OptimizedPostgresTask.normalizeStoredKeyValue(timestamp.getTime(), 1114L));
        assertEquals("12.30",
                OptimizedPostgresTask.normalizeStoredKeyValue(new BigDecimal("12.30"), 1700L));
        assertEquals("192.0.2.1",
                OptimizedPostgresTask.normalizeStoredKeyValue(
                        Map.of("type", "inet", "value", "192.0.2.1"), 869L));
    }

    @Test
    void convertsPostgresDomainsUsingTheirBaseTypeOid() {
        io.cdcbench.connect.common.PgOutputMessage.Column domain =
                new io.cdcbench.connect.common.PgOutputMessage.Column(
                        false, "domain_value", 91_001L, -1);

        io.cdcbench.connect.common.PgOutputMessage.Column valueColumn =
                OptimizedPostgresTask.valueColumn(domain, 23L);

        assertEquals("domain_value", valueColumn.name());
        assertEquals(23L, valueColumn.typeOid());
        assertEquals(-1, valueColumn.typeModifier());
    }

    @Test
    void usesTheDomainTypeModifierForItsBaseNumericSchemaAndValue() {
        io.cdcbench.connect.common.PgOutputMessage.Column domain =
                new io.cdcbench.connect.common.PgOutputMessage.Column(
                        true, "domain_amount", 91_002L, -1);
        int numeric20Scale6 = 4 + (20 << 16) + 6;

        io.cdcbench.connect.common.PgOutputMessage.Column valueColumn =
                OptimizedPostgresTask.valueColumn(
                        domain, 1700L, numeric20Scale6);

        assertEquals(1700L, valueColumn.typeOid());
        assertEquals(numeric20Scale6, valueColumn.typeModifier());
    }

    @Test
    void checkpointsEveryLargeTransactionRecordAtItsExactChangeCount() {
        SourceRecord base = new SourceRecord(
                Map.of("server", "large_tx"),
                Map.of("lsn", 10L, "last_emitted_lsn", 11L,
                        "transaction_resume_id", 99L,
                        "transaction_resume_count", 1),
                "large_tx.public.events", null, null);

        SourceRecord intermediate = OptimizedPostgresTask.withTransactionProgress(
                base, 20L, 21L, 99L, 37, false);

        assertEquals(20L, intermediate.sourceOffset().get("lsn"));
        assertEquals(21L, intermediate.sourceOffset().get("last_emitted_lsn"));
        assertEquals(99L, intermediate.sourceOffset().get("transaction_resume_id"));
        assertEquals(37, intermediate.sourceOffset().get("transaction_resume_count"));

        SourceRecord committed = OptimizedPostgresTask.withTransactionProgress(
                intermediate, 30L, 31L, 99L, 128, true);

        assertEquals(30L, committed.sourceOffset().get("lsn"));
        assertEquals(31L, committed.sourceOffset().get("last_emitted_lsn"));
        assertFalse(committed.sourceOffset().containsKey("transaction_resume_id"));
        assertFalse(committed.sourceOffset().containsKey("transaction_resume_count"));
    }
}
