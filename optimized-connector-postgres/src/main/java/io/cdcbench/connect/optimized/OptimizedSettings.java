package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.nio.file.Path;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

final class OptimizedSettings {
    static final String EXECUTION_MODE = "optimized.execution.mode";
    static final String PROCESSING_WORKERS = "optimized.processing.workers";
    static final String PROCESSING_CHUNK_SIZE = "optimized.processing.chunk.size";
    static final String MAX_INFLIGHT_TRANSACTIONS = "optimized.max.inflight.transactions";
    static final String MAX_INFLIGHT_BYTES = "optimized.max.inflight.bytes";
    static final String TRANSACTION_SPILL_DIRECTORY = "optimized.transaction.spill.directory";
    static final String RAW_UTF8_ENABLED = "optimized.raw.utf8.enabled";
    static final String INCLUDE_UNKNOWN_DATATYPES = "include.unknown.datatypes";
    static final String HEARTBEAT_ACTION_QUERY = "heartbeat.action.query";
    static final String STATUS_UPDATE_INTERVAL_MS = "status.update.interval.ms";
    static final String SIGNAL_DATA_COLLECTION = "signal.data.collection";
    static final String SIGNAL_ENABLED_CHANNELS = "signal.enabled.channels";
    static final String SIGNAL_KAFKA_TOPIC = "signal.kafka.topic";
    static final String SIGNAL_KAFKA_GROUP_ID = "signal.kafka.groupId";
    static final String SIGNAL_KAFKA_BOOTSTRAP_SERVERS = "signal.kafka.bootstrap.servers";
    static final String SIGNAL_KAFKA_POLL_TIMEOUT_MS = "signal.kafka.poll.timeout.ms";
    static final String SIGNAL_POLL_INTERVAL_MS = "signal.poll.interval.ms";
    static final String SIGNAL_CONSUMER_PREFIX = "signal.consumer.";
    static final String INCREMENTAL_SNAPSHOT_CHUNK_SIZE = "incremental.snapshot.chunk.size";
    static final String READ_ONLY = "read.only";
    static final String PUBLICATION_AUTOCREATE_MODE = "publication.autocreate.mode";
    static final String SLOT_DROP_ON_STOP = "slot.drop.on.stop";
    static final String SLOT_FAILOVER = "slot.failover";
    static final String SLOT_STREAM_PARAMS = "slot.stream.params";
    static final String TRANSFORMS = "transforms";
    static final String PREDICATES = "predicates";

    private static final int DEFAULT_WORKERS = 1;
    private static final int DEFAULT_CHUNK_SIZE = 512;
    private static final int DEFAULT_MAX_INFLIGHT_TRANSACTIONS = 16;
    private static final long DEFAULT_MAX_INFLIGHT_BYTES = 64L * 1024L * 1024L;
    private static final int DEFAULT_INCREMENTAL_SNAPSHOT_CHUNK_SIZE = 1_024;
    private static final int MAX_WORKERS = 64;

    private final int processingWorkers;
    private final int processingChunkSize;
    private final int maxInflightTransactions;
    private final long maxInflightBytes;
    private final Path transactionSpillDirectory;
    private final boolean rawUtf8Enabled;
    private final boolean includeUnknownDatatypes;
    private final String heartbeatActionQuery;
    private final int statusUpdateIntervalMs;
    private final boolean schemasEnabled;
    private final QualifiedTable signalDataCollection;
    private final Set<String> signalEnabledChannels;
    private final String signalKafkaTopic;
    private final String signalKafkaGroupId;
    private final String signalKafkaBootstrapServers;
    private final long signalKafkaPollTimeoutMs;
    private final long signalPollIntervalMs;
    private final Map<String, String> signalConsumerProperties;
    private final int incrementalSnapshotChunkSize;
    private final boolean readOnly;
    private final PublicationAutoCreateMode publicationAutoCreateMode;
    private final boolean slotDropOnStop;
    private final boolean slotFailover;
    private final Map<String, String> slotStreamParameters;
    private final PostgresConnectionSettings postgresConnectionSettings;
    private final boolean externalTransformsConfigured;
    private final List<Pattern> messagePrefixIncludes;
    private final List<Pattern> messagePrefixExcludes;

    OptimizedSettings(Map<String, String> properties) {
        validateNativeExecutionMode(properties.get(EXECUTION_MODE));
        processingWorkers = positiveInt(properties, PROCESSING_WORKERS, DEFAULT_WORKERS, MAX_WORKERS);
        processingChunkSize = positiveInt(properties, PROCESSING_CHUNK_SIZE, DEFAULT_CHUNK_SIZE, Integer.MAX_VALUE);
        maxInflightTransactions = positiveInt(
                properties, MAX_INFLIGHT_TRANSACTIONS, DEFAULT_MAX_INFLIGHT_TRANSACTIONS, Integer.MAX_VALUE);
        maxInflightBytes = positiveLong(properties, MAX_INFLIGHT_BYTES, DEFAULT_MAX_INFLIGHT_BYTES);
        String spillDirectory = properties.getOrDefault(
                TRANSACTION_SPILL_DIRECTORY,
                Path.of(System.getProperty("java.io.tmpdir"),
                        "optimized-postgres-transactions").toString());
        if (spillDirectory.isBlank()) {
            throw new ConfigException(TRANSACTION_SPILL_DIRECTORY, spillDirectory,
                    "must be a non-blank filesystem path");
        }
        transactionSpillDirectory = Path.of(spillDirectory).toAbsolutePath().normalize();
        rawUtf8Enabled = booleanValue(properties, RAW_UTF8_ENABLED, false);
        includeUnknownDatatypes = booleanValue(properties, INCLUDE_UNKNOWN_DATATYPES, false);
        heartbeatActionQuery = optionalText(properties, HEARTBEAT_ACTION_QUERY);
        statusUpdateIntervalMs = positiveInt(
                properties, STATUS_UPDATE_INTERVAL_MS, 10_000, Integer.MAX_VALUE);
        boolean keySchemas = booleanValue(properties, "key.converter.schemas.enable", false);
        boolean valueSchemas = booleanValue(properties, "value.converter.schemas.enable", false);
        if (keySchemas != valueSchemas) {
            throw new ConfigException("key/value.converter.schemas.enable", keySchemas + "/" + valueSchemas,
                    "must use the same value on the native path");
        }
        schemasEnabled = keySchemas;
        String signalCollection = optionalText(properties, SIGNAL_DATA_COLLECTION);
        signalDataCollection = signalCollection == null ? null
                : QualifiedTable.parse(signalCollection, SIGNAL_DATA_COLLECTION);
        signalEnabledChannels = commaSeparatedValues(
                properties.getOrDefault(SIGNAL_ENABLED_CHANNELS, "source"));
        signalKafkaTopic = optionalText(properties, SIGNAL_KAFKA_TOPIC);
        signalKafkaGroupId = optionalText(properties, SIGNAL_KAFKA_GROUP_ID) == null
                ? "kafka-signal" : optionalText(properties, SIGNAL_KAFKA_GROUP_ID);
        signalKafkaBootstrapServers = optionalText(
                properties, SIGNAL_KAFKA_BOOTSTRAP_SERVERS);
        signalKafkaPollTimeoutMs = nonNegativeLong(
                properties, SIGNAL_KAFKA_POLL_TIMEOUT_MS, 100L);
        signalPollIntervalMs = nonNegativeLong(
                properties, SIGNAL_POLL_INTERVAL_MS, 5_000L);
        signalConsumerProperties = signalConsumerProperties(properties);
        incrementalSnapshotChunkSize = positiveInt(
                properties, INCREMENTAL_SNAPSHOT_CHUNK_SIZE,
                DEFAULT_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, Integer.MAX_VALUE);
        readOnly = booleanValue(properties, READ_ONLY, false);
        publicationAutoCreateMode = publicationAutoCreateMode(properties);
        slotDropOnStop = booleanValue(properties, SLOT_DROP_ON_STOP, false);
        slotFailover = booleanValue(properties, SLOT_FAILOVER, false);
        slotStreamParameters = slotStreamParameters(properties.get(SLOT_STREAM_PARAMS));
        validatePgOutputProtocol(slotStreamParameters);
        postgresConnectionSettings = new PostgresConnectionSettings(properties);
        externalTransformsConfigured = optionalText(properties, TRANSFORMS) != null;
        messagePrefixIncludes = patterns(properties, "message.prefix.include.list");
        messagePrefixExcludes = patterns(properties, "message.prefix.exclude.list");
        if (!messagePrefixIncludes.isEmpty() && !messagePrefixExcludes.isEmpty()) {
            throw new ConfigException("message.prefix.include.list/message.prefix.exclude.list",
                    "both configured", "only one logical message prefix filter may be configured");
        }
    }

    int processingWorkers() {
        return processingWorkers;
    }

    int processingChunkSize() {
        return processingChunkSize;
    }

    int maxInflightTransactions() {
        return maxInflightTransactions;
    }

    long maxInflightBytes() {
        return maxInflightBytes;
    }

    Path transactionSpillDirectory() {
        return transactionSpillDirectory;
    }

    boolean rawUtf8Enabled() {
        return rawUtf8Enabled;
    }

    boolean includeUnknownDatatypes() {
        return includeUnknownDatatypes;
    }

    String heartbeatActionQuery() {
        return heartbeatActionQuery;
    }

    int statusUpdateIntervalMs() {
        return statusUpdateIntervalMs;
    }

    boolean schemasEnabled() {
        return schemasEnabled;
    }

    String signalDataCollection() {
        return signalDataCollection == null ? null : signalDataCollection.canonicalName();
    }

    QualifiedTable signalTable() {
        return signalDataCollection;
    }

    boolean signalingEnabled() {
        return signalDataCollection != null;
    }

    boolean sourceSignalingEnabled() {
        return signalDataCollection != null && signalEnabledChannels.contains("source");
    }

    boolean kafkaSignalingEnabled() {
        return signalEnabledChannels.contains("kafka");
    }

    Set<String> signalEnabledChannels() {
        return signalEnabledChannels;
    }

    String signalKafkaTopic(String topicPrefix) {
        return signalKafkaTopic == null ? topicPrefix + "-signal" : signalKafkaTopic;
    }

    String signalKafkaGroupId() {
        return signalKafkaGroupId;
    }

    String signalKafkaBootstrapServers() {
        return signalKafkaBootstrapServers;
    }

    long signalKafkaPollTimeoutMs() {
        return signalKafkaPollTimeoutMs;
    }

    long signalPollIntervalMs() {
        return signalPollIntervalMs;
    }

    Map<String, String> signalConsumerProperties() {
        return signalConsumerProperties;
    }

    int incrementalSnapshotChunkSize() {
        return incrementalSnapshotChunkSize;
    }

    boolean readOnly() {
        return readOnly;
    }

    PublicationAutoCreateMode publicationAutoCreateMode() {
        return publicationAutoCreateMode;
    }

    boolean slotDropOnStop() {
        return slotDropOnStop;
    }

    boolean slotFailover() {
        return slotFailover;
    }

    Map<String, String> slotStreamParameters() {
        return slotStreamParameters;
    }

    int pgOutputProtocolVersion() {
        String configured = slotStreamParameters.get("proto_version");
        if (configured != null) {
            return Integer.parseInt(configured);
        }
        return twoPhaseEnabled() ? 3 : 1;
    }

    boolean twoPhaseEnabled() {
        return Boolean.parseBoolean(slotStreamParameters.getOrDefault("two_phase", "false"));
    }

    boolean streamedTransactionsEnabled() {
        return "on".equalsIgnoreCase(
                slotStreamParameters.getOrDefault("streaming", "off"));
    }

    PostgresConnectionSettings postgresConnectionSettings() {
        return postgresConnectionSettings;
    }

    boolean externalTransformsConfigured() {
        return externalTransformsConfigured;
    }

    boolean includesLogicalMessage(String prefix) {
        if (!messagePrefixIncludes.isEmpty()) {
            return messagePrefixIncludes.stream().anyMatch(pattern -> pattern.matcher(prefix).matches());
        }
        if (!messagePrefixExcludes.isEmpty()) {
            return messagePrefixExcludes.stream().noneMatch(pattern -> pattern.matcher(prefix).matches());
        }
        return false;
    }

    boolean logicalMessagesEnabled() {
        return !messagePrefixIncludes.isEmpty() || !messagePrefixExcludes.isEmpty();
    }

    private static List<Pattern> patterns(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            return List.of();
        }
        List<Pattern> patterns = new ArrayList<>();
        for (String expression : value.split(",")) {
            String normalized = expression.trim();
            if (normalized.isEmpty()) {
                continue;
            }
            try {
                patterns.add(Pattern.compile(normalized));
            } catch (PatternSyntaxException error) {
                throw new ConfigException(key, expression, "must contain valid regular expressions");
            }
        }
        return List.copyOf(patterns);
    }

    private static String optionalText(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    private static Set<String> commaSeparatedValues(String value) {
        Set<String> values = new LinkedHashSet<>();
        for (String item : value.split(",")) {
            String normalized = item.trim().toLowerCase(Locale.ROOT);
            if (!normalized.isEmpty()) {
                values.add(normalized);
            }
        }
        return Set.copyOf(values);
    }

    private static Map<String, String> signalConsumerProperties(
            Map<String, String> properties) {
        Map<String, String> consumer = new LinkedHashMap<>();
        properties.forEach((key, value) -> {
            if (key.startsWith(SIGNAL_CONSUMER_PREFIX)
                    && key.length() > SIGNAL_CONSUMER_PREFIX.length()) {
                consumer.put(key.substring(SIGNAL_CONSUMER_PREFIX.length()), value);
            }
        });
        return Map.copyOf(consumer);
    }

    private static Map<String, String> slotStreamParameters(String value) {
        if (value == null || value.isBlank()) {
            return Map.of();
        }
        Map<String, String> parameters = new LinkedHashMap<>();
        for (String item : value.split(";", -1)) {
            String normalized = item.trim();
            if (normalized.isEmpty()) {
                continue;
            }
            int separator = normalized.indexOf('=');
            if (separator <= 0 || separator == normalized.length() - 1) {
                throw new ConfigException(SLOT_STREAM_PARAMS, value,
                        "must contain semicolon-separated key=value parameters");
            }
            String key = normalized.substring(0, separator).trim();
            String parameterValue = normalized.substring(separator + 1).trim();
            if (key.isEmpty() || parameterValue.isEmpty()) {
                throw new ConfigException(SLOT_STREAM_PARAMS, value,
                        "must not contain blank keys or values");
            }
            if (parameters.putIfAbsent(key, parameterValue) != null) {
                throw new ConfigException(SLOT_STREAM_PARAMS, value,
                        "contains duplicate parameter " + key);
            }
        }
        return Map.copyOf(parameters);
    }

    private static void validatePgOutputProtocol(Map<String, String> parameters) {
        String twoPhase = parameters.get("two_phase");
        if (twoPhase != null && !Set.of("true", "false").contains(twoPhase.toLowerCase(Locale.ROOT))) {
            throw new ConfigException(SLOT_STREAM_PARAMS, twoPhase,
                    "two_phase must be true or false");
        }
        String protocol = parameters.get("proto_version");
        int version = 1;
        if (protocol != null) {
            try {
                version = Integer.parseInt(protocol);
            } catch (NumberFormatException error) {
                throw new ConfigException(SLOT_STREAM_PARAMS, protocol,
                        "proto_version must be an integer");
            }
            if (version < 1 || version > 4) {
                throw new ConfigException(SLOT_STREAM_PARAMS, protocol,
                        "proto_version must be between 1 and 4");
            }
        }
        if (Boolean.parseBoolean(twoPhase) && protocol != null && version < 3) {
            throw new ConfigException(SLOT_STREAM_PARAMS, protocol,
                    "two_phase=true requires proto_version 3 or higher");
        }
        String streaming = parameters.get("streaming");
        if (streaming != null
                && !Set.of("on", "off").contains(streaming.toLowerCase(Locale.ROOT))) {
            throw new ConfigException(SLOT_STREAM_PARAMS, streaming,
                    "streaming must be on or off");
        }
        if ("on".equalsIgnoreCase(streaming) && version < 2) {
            throw new ConfigException(SLOT_STREAM_PARAMS, protocol,
                    "streaming=on requires proto_version 2 or higher");
        }
        if (parameters.containsKey("publication_names")) {
            throw new ConfigException(SLOT_STREAM_PARAMS, parameters.get("publication_names"),
                    "publication_names is controlled by publication.name");
        }
    }

    private static void validateNativeExecutionMode(String value) {
        if (value == null || value.isBlank() || "fast".equalsIgnoreCase(value.trim())) {
            return;
        }
        throw new ConfigException(EXECUTION_MODE, value,
                "must be fast");
    }

    private static PublicationAutoCreateMode publicationAutoCreateMode(
            Map<String, String> properties) {
        String value = properties.getOrDefault(PUBLICATION_AUTOCREATE_MODE, "all_tables");
        try {
            return PublicationAutoCreateMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException error) {
            throw new ConfigException(PUBLICATION_AUTOCREATE_MODE, value,
                    "must be one of disabled, filtered, all_tables or no_tables");
        }
    }

    private static int positiveInt(Map<String, String> properties, String key, int defaultValue, int maximum) {
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        final int parsed;
        try {
            parsed = Integer.parseInt(value.trim());
        } catch (NumberFormatException error) {
            throw new ConfigException(key, value, "must be an integer");
        }
        if (parsed < 1 || parsed > maximum) {
            throw new ConfigException(key, value, "must be between 1 and " + maximum);
        }
        return parsed;
    }

    private static long positiveLong(Map<String, String> properties, String key, long defaultValue) {
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        final long parsed;
        try {
            parsed = Long.parseLong(value.trim());
        } catch (NumberFormatException error) {
            throw new ConfigException(key, value, "must be an integer");
        }
        if (parsed < 1L) {
            throw new ConfigException(key, value, "must be positive");
        }
        return parsed;
    }

    private static long nonNegativeLong(
            Map<String, String> properties, String key, long defaultValue) {
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        final long parsed;
        try {
            parsed = Long.parseLong(value.trim());
        } catch (NumberFormatException error) {
            throw new ConfigException(key, value, "must be an integer");
        }
        if (parsed < 0L) {
            throw new ConfigException(key, value, "must not be negative");
        }
        return parsed;
    }

    private static boolean booleanValue(Map<String, String> properties, String key, boolean defaultValue) {
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value.trim())) {
            return true;
        }
        if ("false".equalsIgnoreCase(value.trim())) {
            return false;
        }
        throw new ConfigException(key, value, "must be true or false");
    }

    enum PublicationAutoCreateMode {
        DISABLED,
        FILTERED,
        ALL_TABLES,
        NO_TABLES
    }
}
