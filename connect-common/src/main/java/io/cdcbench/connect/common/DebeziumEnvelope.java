package io.cdcbench.connect.common;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public final class DebeziumEnvelope {
    private static final long POSTGRES_EPOCH_MICROS = 946_684_800_000_000L;

    private DebeziumEnvelope() {}

    public static Map<String, Object> create(
            Map<String, Object> before,
            Map<String, Object> after,
            String operation,
            String connectorVersion,
            String logicalName,
            String database,
            String schema,
            String table,
            long transactionId,
            long previousLsn,
            long lsn,
            long commitTimeMicros) {
        return create(before, after, operation, connectorVersion, logicalName, database,
                schema, table, transactionId, previousLsn, lsn, commitTimeMicros, null);
    }

    public static Map<String, Object> create(
            Map<String, Object> before,
            Map<String, Object> after,
            String operation,
            String connectorVersion,
            String logicalName,
            String database,
            String schema,
            String table,
            long transactionId,
            long previousLsn,
            long lsn,
            long commitTimeMicros,
            Map<String, Object> transactionMetadata) {

        return create(before, after, operation, connectorVersion, logicalName, database,
                schema, table, transactionId, previousLsn, lsn, commitTimeMicros,
                transactionMetadata, null, null);
    }

    public static Map<String, Object> create(
            Map<String, Object> before,
            Map<String, Object> after,
            String operation,
            String connectorVersion,
            String logicalName,
            String database,
            String schema,
            String table,
            long transactionId,
            long previousLsn,
            long lsn,
            long commitTimeMicros,
            Map<String, Object> transactionMetadata,
            String origin,
            Long originLsn) {

        return createRow(before, after, operation, connectorVersion, logicalName, database,
                schema, table, transactionId, previousLsn, lsn, commitTimeMicros,
                transactionMetadata, "false", null, origin, originLsn);
    }

    public static Map<String, Object> createSnapshot(
            Map<String, Object> after,
            String connectorVersion,
            String logicalName,
            String database,
            String schema,
            String table,
            long transactionId,
            long lsn,
            long snapshotTimeMicros,
            String snapshotMarker) {

        return createRow(null, after, "r", connectorVersion, logicalName, database,
                schema, table, transactionId, 0L, lsn, snapshotTimeMicros,
                null, snapshotMarker, null, null, null);
    }

    public static Map<String, Object> createIncrementalSnapshot(
            Map<String, Object> after,
            String connectorVersion,
            String logicalName,
            String database,
            String schema,
            String table,
            long lowWatermarkLsn,
            long highWatermarkLsn,
            long snapshotTimeMicros) {

        String sequence = "[\"" + lowWatermarkLsn + "\",\"" + highWatermarkLsn + "\"]";
        return createRow(null, after, "r", connectorVersion, logicalName, database,
                schema, table, null, 0L, null, snapshotTimeMicros,
                null, "incremental", sequence, null, null);
    }

    private static Map<String, Object> createRow(
            Map<String, Object> before,
            Map<String, Object> after,
            String operation,
            String connectorVersion,
            String logicalName,
            String database,
            String schema,
            String table,
            Long transactionId,
            long previousLsn,
            Long lsn,
            long commitTimeMicros,
            Map<String, Object> transactionMetadata,
            String snapshotMarker,
            String sequenceOverride,
            String origin,
            Long originLsn) {

        Instant processingTime = Instant.now();
        long processingMicros = Math.addExact(
                Math.multiplyExact(processingTime.getEpochSecond(), 1_000_000L),
                processingTime.getNano() / 1_000L);
        long processingNanos = Math.addExact(
                Math.multiplyExact(processingTime.getEpochSecond(), 1_000_000_000L),
                processingTime.getNano());

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("before", before);
        envelope.put("after", after);
        envelope.put("source", source(connectorVersion, logicalName, database, schema, table,
                transactionId, previousLsn, lsn, commitTimeMicros, snapshotMarker,
                sequenceOverride, origin, originLsn));
        envelope.put("transaction", transactionMetadata);
        envelope.put("op", operation);
        envelope.put("ts_ms", processingMicros / 1_000L);
        envelope.put("ts_us", processingMicros);
        envelope.put("ts_ns", processingNanos);
        return envelope;
    }

    public static Map<String, Object> createLogicalMessage(
            String prefix,
            String content,
            String connectorVersion,
            String logicalName,
            String database,
            Long transactionId,
            long previousLsn,
            long lsn,
            long commitTimeMicros,
            Map<String, Object> transactionMetadata) {

        return createLogicalMessage(prefix, content, connectorVersion, logicalName, database,
                transactionId, previousLsn, lsn, commitTimeMicros, transactionMetadata,
                null, null);
    }

    public static Map<String, Object> createLogicalMessage(
            String prefix,
            String content,
            String connectorVersion,
            String logicalName,
            String database,
            Long transactionId,
            long previousLsn,
            long lsn,
            long commitTimeMicros,
            Map<String, Object> transactionMetadata,
            String origin,
            Long originLsn) {

        Map<String, Object> message = new LinkedHashMap<>();
        message.put("prefix", prefix);
        message.put("content", content);

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("op", "m");
        envelope.put("ts_ms", Instant.now().toEpochMilli());
        envelope.put("source", source(connectorVersion, logicalName, database, "", "",
                transactionId, previousLsn, lsn, commitTimeMicros, "false", null,
                origin, originLsn));
        envelope.put("transaction", transactionMetadata);
        envelope.put("message", message);
        return envelope;
    }

    private static Map<String, Object> source(
            String connectorVersion,
            String logicalName,
            String database,
            String schema,
            String table,
            Long transactionId,
            long previousLsn,
            Long lsn,
            long commitTimeMicros,
            String snapshotMarker,
            String sequenceOverride,
            String origin,
            Long originLsn) {

        long sourceMicros = Math.addExact(POSTGRES_EPOCH_MICROS, commitTimeMicros);
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("version", connectorVersion);
        source.put("connector", "postgresql");
        source.put("name", logicalName);
        source.put("ts_ms", sourceMicros / 1_000L);
        source.put("snapshot", snapshotMarker);
        source.put("db", database);
        String previousSequence = previousLsn > 0L ? "\"" + previousLsn + "\"" : "null";
        source.put("sequence", sequenceOverride == null
                ? "[" + previousSequence + ",\"" + lsn + "\"]"
                : sequenceOverride);
        source.put("ts_us", sourceMicros);
        source.put("ts_ns", Math.multiplyExact(sourceMicros, 1_000L));
        source.put("schema", schema);
        source.put("table", table);
        source.put("txId", transactionId);
        source.put("lsn", lsn);
        source.put("xmin", null);
        source.put("origin", origin);
        source.put("origin_lsn", originLsn);
        return source;
    }
}
