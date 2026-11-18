package io.cdcbench.connect.optimized;

import io.cdcbench.connect.common.CandidateConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class FastPathSelectorTest {
    @Test
    void selectsFastPathOnlyForTheVerifiedProfile() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void nativePathIsTheOnlyDefault() {
        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(fastProperties()));
    }

    @Test
    void acceptsInitialSnapshotsOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put("snapshot.mode", "initial");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsKafkaHeartbeatsOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(CandidateConfig.HEARTBEAT_INTERVAL_MS, "1000");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsTransactionMetadataOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(CandidateConfig.PROVIDE_TRANSACTION_METADATA, "true");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsLogicalMessagePrefixFiltersOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put("message.prefix.include.list", "business_.*");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsAnExactSignalingTableOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(OptimizedSettings.SIGNAL_DATA_COLLECTION, "public.debezium_signal");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsKafkaAndReadOnlySignalingOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(OptimizedSettings.SIGNAL_ENABLED_CHANNELS, "kafka");
        properties.put(OptimizedSettings.SIGNAL_KAFKA_TOPIC, "signals");
        properties.put(OptimizedSettings.SIGNAL_KAFKA_GROUP_ID, "signal-group");
        properties.put(OptimizedSettings.SIGNAL_KAFKA_BOOTSTRAP_SERVERS, "kafka:29092");
        properties.put(OptimizedSettings.SIGNAL_KAFKA_POLL_TIMEOUT_MS, "50");
        properties.put(OptimizedSettings.SIGNAL_CONSUMER_PREFIX + "security.protocol", "PLAINTEXT");
        properties.put(OptimizedSettings.READ_ONLY, "true");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsTransactionMetadataLogicalMessageCombination() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(CandidateConfig.PROVIDE_TRANSACTION_METADATA, "true");
        properties.put("message.prefix.include.list", "business_.*");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsTransactionMetadataSnapshotSignalCombination() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(CandidateConfig.PROVIDE_TRANSACTION_METADATA, "true");
        properties.put(OptimizedSettings.SIGNAL_DATA_COLLECTION, "public.debezium_signal");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsColumnValueTransformersOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put("column.mask.with.8.chars", "public.events.payload");
        properties.put("column.truncate.to.4.chars", "public.events.summary");
        properties.put("column.mask.hash.v2.SHA-256.with.salt.pepper",
                "public.events.secret");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsSourceTypePropagationOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(SourceTypePropagator.COLUMN_PROPERTY,
                "benchmark\\.public\\.events\\.(id|payload)");
        properties.put(SourceTypePropagator.DATATYPE_PROPERTY,
                "(?i)benchmark\\.public\\.events\\.numeric");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsValueHandlingModesOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(ValueHandlingModes.DECIMAL, "string");
        properties.put(ValueHandlingModes.BINARY, "hex");
        properties.put(ValueHandlingModes.HSTORE, "map");
        properties.put(ValueHandlingModes.INTERVAL, "string");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsTimePrecisionModesOnTheNativePath() {
        for (String mode : new String[]{
                "adaptive", "adaptive_time_microseconds", "connect",
                "isostring", "microseconds", "nanoseconds"}) {
            Map<String, String> properties = fastProperties();
            properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
            properties.put(TimePrecisionMode.PROPERTY, mode);
            assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties), mode);
        }
    }

    @Test
    void acceptsEventProcessingFailureModesOnTheNativePath() {
        for (String mode : new String[]{"fail", "warn", "skip"}) {
            Map<String, String> properties = fastProperties();
            properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
            properties.put(EventProcessingFailureHandlingMode.PROPERTY, mode);
            assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties), mode);
        }
    }

    @Test
    void rejectsEveryFeatureOutsideTheNativeContract() {
        Map<String, String> featureExamples = Map.ofEntries(
                Map.entry("snapshot.mode", "always"),
                Map.entry("include.schema.changes", "true"),
                Map.entry("signal.data.collection", "public.signal.*"),
                Map.entry("connection.validation.timeout.ms", "5000"),
                Map.entry("schema.refresh.mode", "columns_diff")
        );

        featureExamples.forEach((key, value) -> {
            Map<String, String> properties = fastProperties();
            properties.put(key, value);
            assertThrows(ConfigException.class, () -> FastPathSelector.select(properties), key);
        });
    }

    @Test
    void acceptsPostgreSqlFailoverSlotsOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.SLOT_FAILOVER, "true");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsKafkaConnectWorkerDefinedExactlyOnceOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put("exactly.once.support", "required");
        properties.put("transaction.boundary", "poll");
        properties.put("offsets.storage.topic", "connector_offsets");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsConnectorDefinedTransactionBoundaries() {
        Map<String, String> properties = fastProperties();
        properties.put("transaction.boundary", "connector");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void rejectsIntervalTransactionBoundariesOutsideTheNativeContract() {
        Map<String, String> properties = fastProperties();
        properties.put("transaction.boundary", "interval");

        assertThrows(ConfigException.class, () -> FastPathSelector.select(properties));
    }

    @Test
    void acceptsTlsAndDriverPropertiesOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(PostgresConnectionSettings.SSL_MODE, "verify-full");
        properties.put(PostgresConnectionSettings.SSL_ROOT_CERT, "/run/certs/ca.crt");
        properties.put(PostgresConnectionSettings.SSL_CERT, "/run/certs/client.crt");
        properties.put(PostgresConnectionSettings.SSL_KEY, "/run/certs/client.pk8");
        properties.put(PostgresConnectionSettings.SSL_PASSWORD, "secret");
        properties.put(PostgresConnectionSettings.TCP_KEEP_ALIVE, "true");
        properties.put(PostgresConnectionSettings.DRIVER_PREFIX + "ApplicationName", "cdc-native");
        properties.put(PostgresConnectionSettings.DRIVER_PREFIX + "socketTimeout", "30");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsSchemalessKafkaConnectTransformsOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(OptimizedSettings.TRANSFORMS, "unwrap");
        properties.put("transforms.unwrap.type",
                "io.debezium.transforms.ExtractNewRecordState");
        properties.put(OptimizedSettings.PREDICATES, "data");
        properties.put("predicates.data.type",
                "org.apache.kafka.connect.transforms.predicates.TopicNameMatches");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsAnchoredSchemaAndTableFiltersOnTheNativePath() {
        Map<String, String> tableIncludes = fastProperties();
        tableIncludes.put(OptimizedSettings.EXECUTION_MODE, "fast");
        tableIncludes.put(CandidateConfig.TABLE_INCLUDE_LIST,
                "public\\.events_.*,inventory\\.orders");

        Map<String, String> schemaIncludes = fastProperties();
        schemaIncludes.put(OptimizedSettings.EXECUTION_MODE, "fast");
        schemaIncludes.remove(CandidateConfig.TABLE_INCLUDE_LIST);
        schemaIncludes.put(DataCollectionFilter.SCHEMA_INCLUDE_LIST, "tenant_.*");
        schemaIncludes.put(DataCollectionFilter.TABLE_EXCLUDE_LIST, ".*\\.audit_.*");

        assertEquals(FastPathSelector.Path.FAST,
                FastPathSelector.select(tableIncludes));
        assertEquals(FastPathSelector.Path.FAST,
                FastPathSelector.select(schemaIncludes));
    }

    @Test
    void acceptsQuotedDottedSignalCollectionOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.SIGNAL_DATA_COLLECTION,
                "\"tenant.east\".\"Signal.Table\"");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsAnchoredColumnFiltersOnTheNativePath() {
        Map<String, String> includes = fastProperties();
        includes.put(OptimizedSettings.EXECUTION_MODE, "fast");
        includes.put(ColumnFilter.COLUMN_INCLUDE_LIST,
                "public\\.events\\.(id|payload)");

        Map<String, String> excludes = fastProperties();
        excludes.put(OptimizedSettings.EXECUTION_MODE, "fast");
        excludes.put(ColumnFilter.COLUMN_EXCLUDE_LIST,
                "public\\.events\\.secret_.*");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(includes));
        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(excludes));
    }

    @Test
    void acceptsConfiguredTopicNamingStrategiesOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(TopicNames.STRATEGY,
                "io.debezium.schema.SchemaRegexTopicNamingStrategy");
        properties.put(TopicNames.TOPIC_REGEX,
                "^([^.]+)\\.([^.]+)\\.([^.]+)$");
        properties.put(TopicNames.TOPIC_REPLACEMENT, "$1.routed.$2.$3");
        properties.put(TopicNames.TOPIC_KEY_ENFORCE_UNIQUENESS, "false");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsPublicCustomConverterSpiOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(CustomConverters.CONVERTERS, "prefix");
        properties.put("prefix.type", TestStringConverter.class.getName());
        properties.put("prefix.prefix", "qualified:");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsSchemafulAndRejectsEmptyKafkaConnectTransformsOnTheNativePath() {
        Map<String, String> schemaful = fastProperties();
        schemaful.put(OptimizedSettings.EXECUTION_MODE, "fast");
        schemaful.put(OptimizedSettings.TRANSFORMS, "unwrap");
        schemaful.put("key.converter.schemas.enable", "true");
        schemaful.put("value.converter.schemas.enable", "true");

        Map<String, String> empty = fastProperties();
        empty.put(OptimizedSettings.EXECUTION_MODE, "fast");
        empty.put(OptimizedSettings.TRANSFORMS, " ");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(schemaful));
        assertThrows(ConfigException.class, () -> FastPathSelector.select(empty));
    }

    @Test
    void acceptsPublicationAndSlotLifecycleModesOnTheNativePath() {
        for (String mode : new String[]{"disabled", "filtered", "all_tables", "no_tables"}) {
            Map<String, String> properties = fastProperties();
            properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
            properties.put(OptimizedSettings.PUBLICATION_AUTOCREATE_MODE, mode);
            properties.put(OptimizedSettings.SLOT_DROP_ON_STOP, "true");

            assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties), mode);
        }
    }

    @Test
    void acceptsTwoPhasePgOutputStreamParametersOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(OptimizedSettings.SLOT_STREAM_PARAMS,
                "proto_version=3;two_phase=true");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsUnknownDatatypeForwardingOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(OptimizedSettings.INCLUDE_UNKNOWN_DATATYPES, "true");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsHeartbeatActionQueryOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(OptimizedSettings.HEARTBEAT_ACTION_QUERY,
                "UPDATE public.heartbeat SET touched_at = clock_timestamp()");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void rejectsUnknownPublicationLifecycleModeOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(OptimizedSettings.PUBLICATION_AUTOCREATE_MODE, "unknown");

        assertThrows(ConfigException.class, () -> FastPathSelector.select(properties));
    }

    @Test
    void forcedFastModeRefusesSemanticDegradation() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put("snapshot.mode", "always");

        assertThrows(ConfigException.class, () -> FastPathSelector.select(properties));
    }

    @Test
    void rejectsRemovedDebeziumMode() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "debezium");

        assertThrows(ConfigException.class, () -> FastPathSelector.select(properties));
    }

    @Test
    void acceptsTombstonesAndTruncateEmissionOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put(CandidateConfig.TOMBSTONES_ON_DELETE, "true");
        properties.put("skipped.operations", "none");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void acceptsMatchingSchemafulJsonConvertersOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put("key.converter.schemas.enable", "true");
        properties.put("value.converter.schemas.enable", "true");

        assertEquals(FastPathSelector.Path.FAST, FastPathSelector.select(properties));
    }

    @Test
    void rejectsMismatchedSchemafulJsonConvertersOnTheNativePath() {
        Map<String, String> properties = fastProperties();
        properties.put(OptimizedSettings.EXECUTION_MODE, "fast");
        properties.put("key.converter.schemas.enable", "true");

        assertThrows(ConfigException.class, () -> FastPathSelector.select(properties));
    }

    private static Map<String, String> fastProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector.class", OptimizedPostgresConnector.class.getName());
        properties.put("tasks.max", "1");
        properties.put(CandidateConfig.DATABASE_HOSTNAME, "postgres");
        properties.put(CandidateConfig.DATABASE_PORT, "5432");
        properties.put(CandidateConfig.DATABASE_USER, "debezium");
        properties.put(CandidateConfig.DATABASE_PASSWORD, "secret");
        properties.put(CandidateConfig.DATABASE_DBNAME, "benchmark");
        properties.put(CandidateConfig.TOPIC_PREFIX, "optimized_java");
        properties.put(CandidateConfig.PLUGIN_NAME, "pgoutput");
        properties.put(CandidateConfig.SLOT_NAME, "optimized_slot");
        properties.put(CandidateConfig.PUBLICATION_NAME, "publication");
        properties.put(CandidateConfig.TABLE_INCLUDE_LIST, "public.events");
        properties.put(CandidateConfig.SNAPSHOT_MODE, "no_data");
        properties.put(CandidateConfig.INCLUDE_SCHEMA_CHANGES, "false");
        properties.put(CandidateConfig.PROVIDE_TRANSACTION_METADATA, "false");
        properties.put(CandidateConfig.TOMBSTONES_ON_DELETE, "false");
        properties.put(CandidateConfig.EXTENDED_HEADERS, "false");
        properties.put(CandidateConfig.HEARTBEAT_INTERVAL_MS, "0");
        properties.put("publication.autocreate.mode", "disabled");
        properties.put("schema.refresh.mode", "columns_diff_exclude_unchanged_toast");
        properties.put("skipped.operations", "t");
        properties.put("key.converter", OptimizedJsonConverter.class.getName());
        properties.put("key.converter.schemas.enable", "false");
        properties.put("value.converter", OptimizedJsonConverter.class.getName());
        properties.put("value.converter.schemas.enable", "false");
        return properties;
    }
}
