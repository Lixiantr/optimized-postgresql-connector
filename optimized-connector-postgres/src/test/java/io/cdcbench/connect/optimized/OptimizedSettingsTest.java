package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class OptimizedSettingsTest {
    @Test
    void usesSingleWorkerAsTheZeroOverheadDefault() {
        OptimizedSettings settings = new OptimizedSettings(Map.of());

        assertEquals(1, settings.processingWorkers());
        assertEquals(512, settings.processingChunkSize());
        assertEquals(16, settings.maxInflightTransactions());
        assertEquals(64L * 1024L * 1024L, settings.maxInflightBytes());
        assertEquals(true, settings.transactionSpillDirectory().isAbsolute());
        assertEquals(false, settings.rawUtf8Enabled());
        assertEquals(false, settings.includeUnknownDatatypes());
        assertEquals(null, settings.heartbeatActionQuery());
        assertEquals(10_000, settings.statusUpdateIntervalMs());
        assertEquals(false, settings.schemasEnabled());
        assertEquals(null, settings.signalDataCollection());
        assertEquals(java.util.Set.of("source"), settings.signalEnabledChannels());
        assertEquals(false, settings.kafkaSignalingEnabled());
        assertEquals(false, settings.readOnly());
        assertEquals(1024, settings.incrementalSnapshotChunkSize());
        assertEquals(false, settings.externalTransformsConfigured());
        assertEquals(Map.of(), settings.slotStreamParameters());
        assertEquals(1, settings.pgOutputProtocolVersion());
        assertEquals(false, settings.twoPhaseEnabled());
        assertEquals(false, settings.slotFailover());
    }

    @Test
    void acceptsHeartbeatActionQueryAndNormalizesBlankToDisabled() {
        OptimizedSettings configured = new OptimizedSettings(Map.of(
                OptimizedSettings.HEARTBEAT_ACTION_QUERY,
                "UPDATE public.heartbeat SET touched_at = clock_timestamp()"));
        OptimizedSettings blank = new OptimizedSettings(Map.of(
                OptimizedSettings.HEARTBEAT_ACTION_QUERY, "   "));

        assertEquals("UPDATE public.heartbeat SET touched_at = clock_timestamp()",
                configured.heartbeatActionQuery());
        assertEquals(null, blank.heartbeatActionQuery());
    }

    @Test
    void keepsReplicationStatusUpdatesIndependentFromEventHeartbeats() {
        OptimizedSettings settings = new OptimizedSettings(Map.of(
                "heartbeat.interval.ms", "0",
                OptimizedSettings.STATUS_UPDATE_INTERVAL_MS, "2500"));

        assertEquals(2_500, settings.statusUpdateIntervalMs());
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                OptimizedSettings.STATUS_UPDATE_INTERVAL_MS, "0")));
    }

    @Test
    void acceptsAnExplicitParallelConfiguration() {
        OptimizedSettings settings = new OptimizedSettings(Map.of(
                OptimizedSettings.EXECUTION_MODE, "fast",
                OptimizedSettings.PROCESSING_WORKERS, "8",
                OptimizedSettings.PROCESSING_CHUNK_SIZE, "256",
                OptimizedSettings.MAX_INFLIGHT_TRANSACTIONS, "32",
                OptimizedSettings.MAX_INFLIGHT_BYTES, "33554432",
                OptimizedSettings.INCLUDE_UNKNOWN_DATATYPES, "true",
                OptimizedSettings.RAW_UTF8_ENABLED, "true"));

        assertEquals(8, settings.processingWorkers());
        assertEquals(256, settings.processingChunkSize());
        assertEquals(32, settings.maxInflightTransactions());
        assertEquals(33554432L, settings.maxInflightBytes());
        assertEquals(true, settings.rawUtf8Enabled());
        assertEquals(true, settings.includeUnknownDatatypes());
    }

    @Test
    void acceptsPostgreSqlFailoverSlots() {
        OptimizedSettings settings = new OptimizedSettings(Map.of(
                OptimizedSettings.SLOT_FAILOVER, "true"));

        assertEquals(true, settings.slotFailover());
    }

    @Test
    void acceptsMatchingSchemafulConverters() {
        OptimizedSettings settings = new OptimizedSettings(Map.of(
                "key.converter.schemas.enable", "true",
                "value.converter.schemas.enable", "true"));

        assertEquals(true, settings.schemasEnabled());
    }

    @Test
    void parsesTwoPhasePgOutputStreamParameters() {
        OptimizedSettings settings = new OptimizedSettings(Map.of(
                OptimizedSettings.SLOT_STREAM_PARAMS,
                "proto_version=3;two_phase=true;origin=any"));

        assertEquals(Map.of(
                "proto_version", "3", "two_phase", "true", "origin", "any"),
                settings.slotStreamParameters());
        assertEquals(3, settings.pgOutputProtocolVersion());
        assertEquals(true, settings.twoPhaseEnabled());

        OptimizedSettings inferredProtocol = new OptimizedSettings(Map.of(
                OptimizedSettings.SLOT_STREAM_PARAMS, "two_phase=true"));
        assertEquals(3, inferredProtocol.pgOutputProtocolVersion());
    }

    @Test
    void parsesStreamedTransactionPgOutputParameters() {
        OptimizedSettings settings = new OptimizedSettings(Map.of(
                OptimizedSettings.SLOT_STREAM_PARAMS,
                "proto_version=2;streaming=on"));

        assertEquals(true, settings.streamedTransactionsEnabled());
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                OptimizedSettings.SLOT_STREAM_PARAMS,
                "proto_version=1;streaming=on")));
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                OptimizedSettings.SLOT_STREAM_PARAMS,
                "proto_version=2;streaming=sometimes")));
    }

    @Test
    void rejectsMalformedOrIncompatiblePgOutputStreamParameters() {
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                OptimizedSettings.SLOT_STREAM_PARAMS, "two_phase")));
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                OptimizedSettings.SLOT_STREAM_PARAMS,
                "proto_version=1;two_phase=true")));
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                OptimizedSettings.SLOT_STREAM_PARAMS,
                "publication_names=other")));
    }

    @Test
    void detectsKafkaConnectTransformsWithoutOwningTheirConfiguration() {
        OptimizedSettings settings = new OptimizedSettings(Map.of(
                OptimizedSettings.TRANSFORMS, "unwrap",
                "transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState"));

        assertEquals(true, settings.externalTransformsConfigured());
    }

    @Test
    void acceptsSnapshotSignalSettings() {
        OptimizedSettings settings = new OptimizedSettings(Map.of(
                OptimizedSettings.SIGNAL_DATA_COLLECTION, "public.debezium_signal",
                OptimizedSettings.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, "256"));

        assertEquals("public.debezium_signal", settings.signalDataCollection());
        assertEquals(true, settings.signalingEnabled());
        assertEquals(256, settings.incrementalSnapshotChunkSize());
    }

    @Test
    void canonicalizesQuotedSnapshotSignalTable() {
        OptimizedSettings settings = new OptimizedSettings(Map.of(
                OptimizedSettings.SIGNAL_DATA_COLLECTION,
                "\"schema.with.dot\".\"Signal.Table\""));

        assertEquals("\"schema.with.dot\".\"Signal.Table\"",
                settings.signalDataCollection());
        assertEquals("schema.with.dot", settings.signalTable().schema());
        assertEquals("Signal.Table", settings.signalTable().table());
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                OptimizedSettings.SIGNAL_DATA_COLLECTION, "schema.table.extra")));
    }

    @Test
    void acceptsKafkaAndReadOnlySignalSettings() {
        OptimizedSettings settings = new OptimizedSettings(Map.of(
                OptimizedSettings.SIGNAL_ENABLED_CHANNELS, "kafka",
                OptimizedSettings.SIGNAL_KAFKA_TOPIC, "signals",
                OptimizedSettings.SIGNAL_KAFKA_GROUP_ID, "signal-group",
                OptimizedSettings.SIGNAL_KAFKA_BOOTSTRAP_SERVERS, "kafka:29092",
                OptimizedSettings.SIGNAL_KAFKA_POLL_TIMEOUT_MS, "50",
                OptimizedSettings.SIGNAL_CONSUMER_PREFIX + "security.protocol", "PLAINTEXT",
                OptimizedSettings.READ_ONLY, "true"));

        assertEquals(java.util.Set.of("kafka"), settings.signalEnabledChannels());
        assertEquals(true, settings.kafkaSignalingEnabled());
        assertEquals("signals", settings.signalKafkaTopic("server"));
        assertEquals("signal-group", settings.signalKafkaGroupId());
        assertEquals("kafka:29092", settings.signalKafkaBootstrapServers());
        assertEquals(50L, settings.signalKafkaPollTimeoutMs());
        assertEquals(Map.of("security.protocol", "PLAINTEXT"),
                settings.signalConsumerProperties());
        assertEquals(true, settings.readOnly());
    }

    @Test
    void acceptsSourceSignalPollInterval() {
        OptimizedSettings settings = new OptimizedSettings(Map.of(
                OptimizedSettings.SIGNAL_POLL_INTERVAL_MS, "50"));

        assertEquals(50L, settings.signalPollIntervalMs());
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                OptimizedSettings.SIGNAL_POLL_INTERVAL_MS, "-1")));
    }

    @Test
    void rejectsMismatchedSchemafulConverters() {
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                "key.converter.schemas.enable", "true",
                "value.converter.schemas.enable", "false")));
    }

    @Test
    void filtersLogicalMessagesWithVersionPinnedRegexLists() {
        OptimizedSettings includes = new OptimizedSettings(Map.of(
                "message.prefix.include.list", "business_.*,audit"));
        OptimizedSettings excludes = new OptimizedSettings(Map.of(
                "message.prefix.exclude.list", "internal_.*"));

        assertEquals(true, includes.includesLogicalMessage("business_order"));
        assertEquals(true, includes.logicalMessagesEnabled());
        assertEquals(true, includes.includesLogicalMessage("audit"));
        assertEquals(false, includes.includesLogicalMessage("other"));
        assertEquals(false, excludes.includesLogicalMessage("internal_probe"));
        assertEquals(true, excludes.includesLogicalMessage("public_probe"));
        assertEquals(true, excludes.logicalMessagesEnabled());
        assertEquals(false, new OptimizedSettings(Map.of()).logicalMessagesEnabled());
    }

    @Test
    void rejectsConflictingOrInvalidLogicalMessageFilters() {
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                "message.prefix.include.list", "included",
                "message.prefix.exclude.list", "excluded")));
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                "message.prefix.include.list", "[invalid")));
    }

    @Test
    void rejectsInvalidWorkerCounts() {
        Map<String, String> properties = new HashMap<>();
        properties.put(OptimizedSettings.PROCESSING_WORKERS, "0");

        assertThrows(ConfigException.class, () -> new OptimizedSettings(properties));
    }

    @Test
    void rejectsInvalidInflightByteLimits() {
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                OptimizedSettings.MAX_INFLIGHT_BYTES, "0")));
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                OptimizedSettings.MAX_INFLIGHT_BYTES, "not-a-number")));
    }

    @Test
    void rejectsBlankTransactionSpillDirectory() {
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                OptimizedSettings.TRANSACTION_SPILL_DIRECTORY, "   ")));
    }

    @Test
    void rejectsInvalidRawUtf8Toggle() {
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                OptimizedSettings.RAW_UTF8_ENABLED, "sometimes")));
    }

    @Test
    void rejectsLegacyDelegatingExecutionModes() {
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                OptimizedSettings.EXECUTION_MODE, "auto")));
        assertThrows(ConfigException.class, () -> new OptimizedSettings(Map.of(
                OptimizedSettings.EXECUTION_MODE, "debezium")));
    }
}
