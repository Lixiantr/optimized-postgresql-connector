package io.cdcbench.connect.common;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DebeziumEnvelopeTest {
    @Test
    void createsSchemalessDebeziumEnvelope() {
        Map<String, Object> after = Map.of("id", 1L, "payload", "{}");
        Map<String, Object> value = DebeziumEnvelope.create(
                null, after, "c", "3.6.0.Final", "candidate", "benchmark",
                "public", "events", 123, 1000, 2000, 42);

        assertNull(value.get("before"));
        assertEquals(after, value.get("after"));
        assertEquals("c", value.get("op"));
        assertNull(value.get("transaction"));

        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) value.get("source");
        assertEquals("postgresql", source.get("connector"));
        assertEquals("candidate", source.get("name"));
        assertEquals("false", source.get("snapshot"));
        assertEquals("[\"1000\",\"2000\"]", source.get("sequence"));
        assertEquals(946_684_800_000L, source.get("ts_ms"));
    }

    @Test
    void includesNativeTransactionMetadataWhenProvided() {
        Map<String, Object> transaction = Map.of(
                "id", "123:2000", "total_order", 1L, "data_collection_order", 1L);
        Map<String, Object> value = DebeziumEnvelope.create(
                null, Map.of("id", 1L), "c", "3.6.0.Final", "candidate", "benchmark",
                "public", "events", 123, 1000, 2000, 42, transaction);

        assertEquals(transaction, value.get("transaction"));
    }

    @Test
    void includesReplicationOriginMetadataWhenProvided() {
        Map<String, Object> value = DebeziumEnvelope.create(
                null, Map.of("id", 1L), "c", "3.6.0.Final", "candidate", "benchmark",
                "public", "events", 123, 1000, 2000, 42, null,
                "upstream-node", 0xABCDEFL);

        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) value.get("source");
        assertEquals("upstream-node", source.get("origin"));
        assertEquals(11_259_375L, source.get("origin_lsn"));
    }

    @Test
    void usesNullAsTheInitialSequenceLowerBound() {
        Map<String, Object> value = DebeziumEnvelope.create(
                null, Map.of("id", 1L), "c", "3.6.0.Final", "candidate", "benchmark",
                "public", "events", 123, 0, 2000, 42);

        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) value.get("source");
        assertEquals("[null,\"2000\"]", source.get("sequence"));
    }

    @Test
    void createsTheLogicalMessageEnvelopeWithoutRowOrProcessingPrecisionFields() {
        Map<String, Object> value = DebeziumEnvelope.createLogicalMessage(
                "audit", "cGF5bG9hZA==", "3.6.0.Final", "candidate", "benchmark",
                null, 2000L, 2000L, 42L, null);

        assertEquals(java.util.List.of("op", "ts_ms", "source", "transaction", "message"),
                java.util.List.copyOf(value.keySet()));
        assertEquals("m", value.get("op"));
        assertNull(value.get("transaction"));
        assertEquals(Map.of("prefix", "audit", "content", "cGF5bG9hZA=="),
                value.get("message"));

        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) value.get("source");
        assertEquals("", source.get("schema"));
        assertEquals("", source.get("table"));
        assertNull(source.get("txId"));
        assertEquals("[\"2000\",\"2000\"]", source.get("sequence"));
    }

    @Test
    void createsVersionPinnedSnapshotReadEvents() {
        Map<String, Object> after = Map.of("id", 7L, "payload", "snapshot");
        Map<String, Object> value = DebeziumEnvelope.createSnapshot(
                after, "3.6.0.Final", "candidate", "benchmark", "public", "events",
                123L, 2000L, 42L, "last");

        assertNull(value.get("before"));
        assertEquals(after, value.get("after"));
        assertEquals("r", value.get("op"));
        assertNull(value.get("transaction"));
        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) value.get("source");
        assertEquals("last", source.get("snapshot"));
        assertEquals("[null,\"2000\"]", source.get("sequence"));
        assertEquals(123L, source.get("txId"));
    }

    @Test
    void createsVersionPinnedIncrementalSnapshotReadEvents() {
        Map<String, Object> value = DebeziumEnvelope.createIncrementalSnapshot(
                Map.of("id", 8L), "3.6.0.Final", "candidate", "benchmark",
                "public", "events", 1000L, 2000L, 42L);

        assertEquals("r", value.get("op"));
        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) value.get("source");
        assertEquals("incremental", source.get("snapshot"));
        assertEquals("[\"1000\",\"2000\"]", source.get("sequence"));
        assertNull(source.get("txId"));
        assertNull(source.get("lsn"));
    }
}
