package io.cdcbench.connect.optimized;

import io.cdcbench.connect.common.CandidateConfig;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class FastPathSelector {
    private static final Set<String> FAST_PATH_KEYS = Set.of(
            "name",
            "connector.class",
            "task.class",
            "tasks.max",
            "exactly.once.support",
            "transaction.boundary",
            "transaction.boundary.interval.ms",
            "offsets.storage.topic",
            CandidateConfig.DATABASE_HOSTNAME,
            CandidateConfig.DATABASE_PORT,
            CandidateConfig.DATABASE_USER,
            CandidateConfig.DATABASE_PASSWORD,
            CandidateConfig.DATABASE_DBNAME,
            CandidateConfig.TOPIC_PREFIX,
            CandidateConfig.PLUGIN_NAME,
            CandidateConfig.SLOT_NAME,
            CandidateConfig.PUBLICATION_NAME,
            CandidateConfig.TABLE_INCLUDE_LIST,
            DataCollectionFilter.TABLE_EXCLUDE_LIST,
            DataCollectionFilter.SCHEMA_INCLUDE_LIST,
            DataCollectionFilter.SCHEMA_EXCLUDE_LIST,
            ColumnFilter.COLUMN_INCLUDE_LIST,
            ColumnFilter.COLUMN_EXCLUDE_LIST,
            SourceTypePropagator.COLUMN_PROPERTY,
            SourceTypePropagator.DATATYPE_PROPERTY,
            ValueHandlingModes.DECIMAL,
            ValueHandlingModes.BINARY,
            ValueHandlingModes.HSTORE,
            ValueHandlingModes.INTERVAL,
            TimePrecisionMode.PROPERTY,
            EventProcessingFailureHandlingMode.PROPERTY,
            OptimizedSettings.INCLUDE_UNKNOWN_DATATYPES,
            OptimizedSettings.HEARTBEAT_ACTION_QUERY,
            CandidateConfig.SNAPSHOT_MODE,
            CandidateConfig.INCLUDE_SCHEMA_CHANGES,
            CandidateConfig.PROVIDE_TRANSACTION_METADATA,
            CandidateConfig.TOMBSTONES_ON_DELETE,
            CandidateConfig.EXTENDED_HEADERS,
            CandidateConfig.HEARTBEAT_INTERVAL_MS,
            CandidateConfig.POLL_INTERVAL_MS,
            CandidateConfig.MAX_BATCH_SIZE,
            CandidateConfig.MAX_QUEUE_SIZE,
            CandidateConfig.SOURCE_VERSION,
            "publication.autocreate.mode",
            "slot.drop.on.stop",
            OptimizedSettings.SLOT_FAILOVER,
            OptimizedSettings.SLOT_STREAM_PARAMS,
            PostgresConnectionSettings.QUERY_TIMEOUT_MS,
            PostgresConnectionSettings.INITIAL_STATEMENTS,
            PostgresConnectionSettings.SSL_MODE,
            PostgresConnectionSettings.SSL_CERT,
            PostgresConnectionSettings.SSL_PASSWORD,
            PostgresConnectionSettings.SSL_ROOT_CERT,
            PostgresConnectionSettings.SSL_KEY,
            PostgresConnectionSettings.SSL_FACTORY,
            PostgresConnectionSettings.TCP_KEEP_ALIVE,
            OptimizedSettings.TRANSFORMS,
            OptimizedSettings.PREDICATES,
            TopicNames.STRATEGY,
            TopicNames.HEARTBEAT_TOPICS_PREFIX,
            TopicNames.TOPIC_REGEX,
            TopicNames.TOPIC_REPLACEMENT,
            TopicNames.TOPIC_KEY_ENFORCE_UNIQUENESS,
            TopicNames.TOPIC_KEY_FIELD_NAME,
            TopicNames.TOPIC_KEY_FIELD_REGEX,
            TopicNames.TOPIC_KEY_FIELD_REPLACEMENT,
            CustomConverters.CONVERTERS,
            "schema.refresh.mode",
            "skipped.operations",
            "message.prefix.include.list",
            "message.prefix.exclude.list",
            OptimizedSettings.SIGNAL_DATA_COLLECTION,
            OptimizedSettings.SIGNAL_ENABLED_CHANNELS,
            OptimizedSettings.SIGNAL_KAFKA_TOPIC,
            OptimizedSettings.SIGNAL_KAFKA_GROUP_ID,
            OptimizedSettings.SIGNAL_KAFKA_BOOTSTRAP_SERVERS,
            OptimizedSettings.SIGNAL_KAFKA_POLL_TIMEOUT_MS,
            OptimizedSettings.SIGNAL_POLL_INTERVAL_MS,
            OptimizedSettings.INCREMENTAL_SNAPSHOT_CHUNK_SIZE,
            OptimizedSettings.READ_ONLY,
            "key.converter",
            "key.converter.schemas.enable",
            "value.converter",
            "value.converter.schemas.enable",
            OptimizedSettings.EXECUTION_MODE,
            OptimizedSettings.PROCESSING_WORKERS,
            OptimizedSettings.PROCESSING_CHUNK_SIZE,
            OptimizedSettings.MAX_INFLIGHT_TRANSACTIONS,
            OptimizedSettings.MAX_INFLIGHT_BYTES,
            OptimizedSettings.TRANSACTION_SPILL_DIRECTORY,
            OptimizedSettings.RAW_UTF8_ENABLED);

    private FastPathSelector() {
    }

    static Path select(Map<String, String> properties) {
        new OptimizedSettings(properties);
        List<String> issues = compatibilityIssues(properties);
        if (issues.isEmpty()) {
            return Path.FAST;
        }
        throw new ConfigException(OptimizedSettings.EXECUTION_MODE, "fast",
                "configuration is outside the verified native contract: " + String.join("; ", issues));
    }

    static List<String> compatibilityIssues(Map<String, String> properties) {
        List<String> issues = new ArrayList<>();
        List<String> customConverterPrefixes = CustomConverters.prefixes(properties);
        for (String key : properties.keySet()) {
            boolean customConverterProperty = customConverterPrefixes.stream()
                    .anyMatch(key::startsWith);
            if (!FAST_PATH_KEYS.contains(key)
                    && !key.startsWith(OptimizedSettings.SIGNAL_CONSUMER_PREFIX)
                    && !key.startsWith(PostgresConnectionSettings.DRIVER_PREFIX)
                    && !key.startsWith(OptimizedSettings.TRANSFORMS + ".")
                    && !key.startsWith(OptimizedSettings.PREDICATES + ".")
                    && !ColumnValueTransformers.isProperty(key)
                    && !customConverterProperty) {
                issues.add("unsupported property " + key);
            }
        }

        requireValue(properties, CandidateConfig.PLUGIN_NAME, "pgoutput", "pgoutput", issues);
        String snapshotMode = properties.getOrDefault(CandidateConfig.SNAPSHOT_MODE, "no_data");
        if (!Set.of("no_data", "initial").contains(snapshotMode)) {
            issues.add(CandidateConfig.SNAPSHOT_MODE + " must be no_data or initial");
        }
        requireFalse(properties, CandidateConfig.INCLUDE_SCHEMA_CHANGES, issues);
        requireFalse(properties, CandidateConfig.EXTENDED_HEADERS, issues);
        requireValue(properties, "schema.refresh.mode", null,
                "columns_diff_exclude_unchanged_toast", issues);
        String skipped = properties.getOrDefault("skipped.operations", "t");
        if (!skipped.isBlank() && !"none".equalsIgnoreCase(skipped)
                && !skipped.matches("[cudt](?:,[cudt])*")) {
            issues.add("skipped.operations must be a comma-separated subset of c,u,d,t or none");
        }
        requireValue(properties, "tasks.max", "1", "1", issues);
        String transactionBoundary = properties.getOrDefault("transaction.boundary", "poll");
        if (!Set.of("poll", "connector").contains(transactionBoundary)) {
            issues.add("transaction.boundary must be poll or connector");
        }
        requireValue(properties, "key.converter", null,
                OptimizedJsonConverter.class.getName(), issues);
        requireValue(properties, "value.converter", null,
                OptimizedJsonConverter.class.getName(), issues);
        String keySchemas = properties.getOrDefault("key.converter.schemas.enable", "false");
        String valueSchemas = properties.getOrDefault("value.converter.schemas.enable", "false");
        if (!Set.of("true", "false").contains(keySchemas)
                || !Set.of("true", "false").contains(valueSchemas)
                || !keySchemas.equals(valueSchemas)) {
            issues.add("key/value.converter.schemas.enable must both be true or both be false");
        }
        if (properties.containsKey(OptimizedSettings.TRANSFORMS)) {
            if (properties.get(OptimizedSettings.TRANSFORMS).isBlank()) {
                issues.add("transforms must contain at least one Kafka Connect SMT alias");
            }
        }
        OptimizedSettings settings = new OptimizedSettings(properties);

        if (settings.signalEnabledChannels().isEmpty()
                || !Set.of("source", "kafka").containsAll(settings.signalEnabledChannels())) {
            issues.add("signal.enabled.channels must contain only source and/or kafka");
        }
        if (settings.kafkaSignalingEnabled()
                && settings.signalKafkaBootstrapServers() == null) {
            issues.add("signal.kafka.bootstrap.servers is required for the kafka signal channel");
        }

        String signalTable = properties.get(OptimizedSettings.SIGNAL_DATA_COLLECTION);
        if (signalTable != null) {
            try {
                QualifiedTable.parse(signalTable, OptimizedSettings.SIGNAL_DATA_COLLECTION);
            } catch (ConfigException error) {
                issues.add(OptimizedSettings.SIGNAL_DATA_COLLECTION
                        + " must contain one exact schema.table name");
            }
        }

        new DataCollectionFilter(properties);
        new ColumnFilter(properties);
        new ColumnValueTransformers(properties);
        new SourceTypePropagator(
                properties.get(CandidateConfig.DATABASE_DBNAME), properties);
        new ValueHandlingModes(properties);
        new TimePrecisionMode(properties);
        EventProcessingFailureHandlingMode.from(properties);
        new TopicNames(properties);
        CustomConverters.validateConfiguration(properties);
        return List.copyOf(issues);
    }

    private static void requireFalse(Map<String, String> properties, String key, List<String> issues) {
        requireValue(properties, key, "false", "false", issues);
    }

    private static void requireValue(Map<String, String> properties, String key, String defaultValue,
                                     String expected, List<String> issues) {
        String actual = properties.getOrDefault(key, defaultValue);
        if (!expected.equals(actual)) {
            issues.add(key + " must be " + expected);
        }
    }

    enum Path {
        FAST
    }
}
