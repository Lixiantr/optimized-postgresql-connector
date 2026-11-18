package io.cdcbench.connect.optimized;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SnapshotSignalTest {
    @Test
    void parsesBlockingAndIncrementalExecuteSnapshotSignals() {
        SnapshotSignal.Request blocking = SnapshotSignal.parse(Map.of(
                "id", "one",
                "type", "execute-snapshot",
                "data", "{\"data-collections\":[\"public.orders\"],\"type\":\"blocking\"}"))
                .orElseThrow();
        SnapshotSignal.Request incremental = SnapshotSignal.parse(Map.of(
                "id", "two",
                "type", "execute-snapshot",
                "data", "{\"type\":\"incremental\",\"data-collections\":[\"a.one\",\"b.two\"]}"))
                .orElseThrow();

        assertEquals(SnapshotSignal.Type.BLOCKING, blocking.type());
        assertEquals(SnapshotSignal.Action.EXECUTE, blocking.action());
        assertEquals(java.util.List.of("public.orders"), blocking.dataCollections());
        assertEquals(SnapshotSignal.Type.INCREMENTAL, incremental.type());
        assertEquals(java.util.List.of("a.one", "b.two"), incremental.dataCollections());
    }

    @Test
    void parsesDefaultTypeConditionsEmptyCollectionsAndStop() {
        SnapshotSignal.Request filtered = SnapshotSignal.parse(Map.of(
                "type", "execute-snapshot",
                "data", "{\"data-collections\":[\"public.orders\"],"
                        + "\"additional-conditions\":[{\"data-collection\":\"public.orders\","
                        + "\"filter\":\"amount > 10\"}]}"))
                .orElseThrow();
        SnapshotSignal.Request empty = SnapshotSignal.parse(Map.of(
                "type", "execute-snapshot",
                "data", "{\"type\":\"incremental\",\"data-collections\":[]}"))
                .orElseThrow();
        SnapshotSignal.Request stop = SnapshotSignal.parse(Map.of(
                "type", "stop-snapshot", "data", "{\"type\":\"incremental\"}"))
                .orElseThrow();
        SnapshotSignal.Request stopSelected = SnapshotSignal.parse(Map.of(
                "type", "stop-snapshot",
                "data", "{\"type\":\"incremental\","
                        + "\"data-collections\":[\"public.orders\"]}"))
                .orElseThrow();

        assertEquals(SnapshotSignal.Type.INCREMENTAL, filtered.type());
        assertEquals(Map.of("public.orders", "amount > 10"), filtered.additionalConditions());
        assertTrue(empty.dataCollections().isEmpty());
        assertEquals(SnapshotSignal.Action.STOP, stop.action());
        assertTrue(stop.stop());
        assertTrue(stop.dataCollections().isEmpty());
        assertEquals(java.util.List.of("public.orders"), stopSelected.dataCollections());
    }

    @Test
    void ignoresUnrelatedSignalTypes() {
        assertTrue(SnapshotSignal.parse(Map.of(
                "type", "log", "data", "{}"))
                .isEmpty());
    }

    @Test
    void parsesKafkaEnvelopeAndIgnoresOtherConnectorKeys() {
        SnapshotSignal.Request request = SnapshotSignal.parseKafka(
                "server", "server",
                "{\"type\":\"execute-snapshot\",\"data\":{"
                        + "\"type\":\"INCREMENTAL\","
                        + "\"data-collections\":[\"public.orders\"]}}")
                .orElseThrow();

        assertEquals(SnapshotSignal.Type.INCREMENTAL, request.type());
        assertEquals(java.util.List.of("public.orders"), request.dataCollections());
        assertTrue(SnapshotSignal.parseKafka(
                "server", "other", "{\"type\":\"log\",\"data\":{}}")
                .isEmpty());
        assertThrows(ConnectException.class, () -> SnapshotSignal.parseKafka(
                "server", "server", "{\"type\":\"execute-snapshot\",\"data\":[]}"));
    }

    @Test
    void canonicalizesQuotedDottedSignalCollectionsAndConditions() {
        SnapshotSignal.Request request = SnapshotSignal.parse(Map.of(
                "type", "execute-snapshot",
                "data", "{\"data-collections\":["
                        + "\"\\\"schema.with.dot\\\".\\\"Table.Items\\\"\"],"
                        + "\"additional-conditions\":[{\"data-collection\":"
                        + "\"\\\"schema.with.dot\\\".\\\"Table.Items\\\"\","
                        + "\"filter\":\"id > 1\"}]}"))
                .orElseThrow();

        String collection = "\"schema.with.dot\".\"Table.Items\"";
        assertEquals(java.util.List.of(collection), request.dataCollections());
        assertEquals(Map.of(collection, "id > 1"), request.additionalConditions());
    }

    @Test
    void rejectsMalformedExecuteSnapshotSignals() {
        assertThrows(ConnectException.class, () -> SnapshotSignal.parse(Map.of(
                "type", "execute-snapshot", "data", "not-json")));
        assertThrows(ConnectException.class, () -> SnapshotSignal.parse(Map.of(
                "type", "execute-snapshot", "data", "{\"type\":\"blocking\"}")));
        assertThrows(ConnectException.class, () -> SnapshotSignal.parse(Map.of(
                "type", "execute-snapshot",
                "data", "{\"type\":\"unknown\",\"data-collections\":[\"public.t\"]}")));
        assertThrows(ConnectException.class, () -> SnapshotSignal.parse(Map.of(
                "type", "execute-snapshot",
                "data", "{\"data-collections\":[\"public.t\"],"
                        + "\"additional-conditions\":[{\"data-collection\":\"public.other\","
                        + "\"filter\":\"id > 1\"}]}")));
        assertThrows(ConnectException.class, () -> SnapshotSignal.parse(Map.of(
                "type", "stop-snapshot", "data", "{\"type\":\"blocking\"}")));
        assertThrows(ConnectException.class, () -> SnapshotSignal.parse(Map.of(
                "type", "execute-snapshot",
                "data", "{\"type\":\"blocking\",\"data-collections\":[]}")));
    }
}
