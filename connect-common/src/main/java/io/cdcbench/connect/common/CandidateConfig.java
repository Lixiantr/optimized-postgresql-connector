package io.cdcbench.connect.common;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.PASSWORD;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public final class CandidateConfig extends AbstractConfig {
    public static final String DATABASE_HOSTNAME = "database.hostname";
    public static final String DATABASE_PORT = "database.port";
    public static final String DATABASE_USER = "database.user";
    public static final String DATABASE_PASSWORD = "database.password";
    public static final String DATABASE_DBNAME = "database.dbname";
    public static final String TOPIC_PREFIX = "topic.prefix";
    public static final String PLUGIN_NAME = "plugin.name";
    public static final String SLOT_NAME = "slot.name";
    public static final String PUBLICATION_NAME = "publication.name";
    public static final String TABLE_INCLUDE_LIST = "table.include.list";
    public static final String SNAPSHOT_MODE = "snapshot.mode";
    public static final String INCLUDE_SCHEMA_CHANGES = "include.schema.changes";
    public static final String PROVIDE_TRANSACTION_METADATA = "provide.transaction.metadata";
    public static final String TOMBSTONES_ON_DELETE = "tombstones.on.delete";
    public static final String EXTENDED_HEADERS = "extended.headers.enabled";
    public static final String HEARTBEAT_INTERVAL_MS = "heartbeat.interval.ms";
    public static final String POLL_INTERVAL_MS = "poll.interval.ms";
    public static final String MAX_BATCH_SIZE = "max.batch.size";
    public static final String MAX_QUEUE_SIZE = "max.queue.size";
    public static final String SOURCE_VERSION = "compatibility.source.version";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DATABASE_HOSTNAME, STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    HIGH, "PostgreSQL host")
            .define(DATABASE_PORT, INT, 5432, ConfigDef.Range.between(1, 65535), HIGH, "PostgreSQL port")
            .define(DATABASE_USER, STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    HIGH, "PostgreSQL replication user")
            .define(DATABASE_PASSWORD, PASSWORD, ConfigDef.NO_DEFAULT_VALUE, HIGH, "PostgreSQL password")
            .define(DATABASE_DBNAME, STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    HIGH, "PostgreSQL database")
            .define(TOPIC_PREFIX, STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    HIGH, "Debezium-compatible logical server name and topic prefix")
            .define(PLUGIN_NAME, STRING, "pgoutput", ConfigDef.ValidString.in("pgoutput"), HIGH,
                    "Logical decoding plugin")
            .define(SLOT_NAME, STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    HIGH, "Logical replication slot")
            .define(PUBLICATION_NAME, STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    HIGH, "PostgreSQL publication")
            .define(TABLE_INCLUDE_LIST, STRING, "", LOW,
                    "Comma-separated anchored schema.table regular expressions")
            .define(SNAPSHOT_MODE, STRING, "no_data", HIGH, "Snapshot mode")
            .define(INCLUDE_SCHEMA_CHANGES, BOOLEAN, false, LOW, "Emit schema changes")
            .define(PROVIDE_TRANSACTION_METADATA, BOOLEAN, false, LOW, "Emit transaction metadata")
            .define(TOMBSTONES_ON_DELETE, BOOLEAN, false, LOW, "Emit delete tombstones")
            .define(EXTENDED_HEADERS, BOOLEAN, false, LOW, "Emit Debezium context headers")
            .define(HEARTBEAT_INTERVAL_MS, LONG, 1000L, ConfigDef.Range.atLeast(0L), LOW,
                    "Replication status interval")
            .define(POLL_INTERVAL_MS, LONG, 100L, ConfigDef.Range.atLeast(1L), LOW,
                    "SourceTask poll interval")
            .define(MAX_BATCH_SIZE, INT, 8192, ConfigDef.Range.atLeast(1), LOW,
                    "Maximum records returned by a poll")
            .define(MAX_QUEUE_SIZE, INT, 32768, ConfigDef.Range.atLeast(1), LOW,
                    "Maximum in-memory records")
            .define(SOURCE_VERSION, STRING, "3.6.0.Final", new ConfigDef.NonEmptyString(), LOW,
                    "Value reported in source.version during compatibility testing");

    public CandidateConfig(Map<?, ?> properties) {
        super(CONFIG_DEF, properties, false);
    }

    public void validateSupportedConfiguration() {
        String snapshotMode = getString(SNAPSHOT_MODE);
        if (!Set.of("no_data", "initial").contains(snapshotMode)) {
            throw new ConfigException(SNAPSHOT_MODE, snapshotMode,
                    "native path supports no_data or initial");
        }
        requireFalse(INCLUDE_SCHEMA_CHANGES);
        requireFalse(EXTENDED_HEADERS);
        if (getInt(MAX_BATCH_SIZE) > getInt(MAX_QUEUE_SIZE)) {
            throw new ConfigException(MAX_BATCH_SIZE, getInt(MAX_BATCH_SIZE),
                    "must not exceed " + MAX_QUEUE_SIZE);
        }
    }

    private void requireFalse(String key) {
        if (getBoolean(key)) {
            throw new ConfigException(key, true,
                    "must be false for the native PostgreSQL connector");
        }
    }

    public String host() { return getString(DATABASE_HOSTNAME); }
    public int port() { return getInt(DATABASE_PORT); }
    public String user() { return getString(DATABASE_USER); }
    public String password() {
        Password password = getPassword(DATABASE_PASSWORD);
        return password == null ? null : password.value();
    }
    public String database() { return getString(DATABASE_DBNAME); }
    public String topicPrefix() { return getString(TOPIC_PREFIX); }
    public String slot() { return getString(SLOT_NAME); }
    public String publication() { return getString(PUBLICATION_NAME); }
    public String tableIncludeList() { return getString(TABLE_INCLUDE_LIST); }
    public String snapshotMode() { return getString(SNAPSHOT_MODE); }
    public long heartbeatIntervalMs() { return getLong(HEARTBEAT_INTERVAL_MS); }
    public long pollIntervalMs() { return getLong(POLL_INTERVAL_MS); }
    public int maxBatchSize() { return getInt(MAX_BATCH_SIZE); }
    public int maxQueueSize() { return getInt(MAX_QUEUE_SIZE); }
    public String sourceVersion() { return getString(SOURCE_VERSION); }
    public boolean tombstonesOnDelete() { return getBoolean(TOMBSTONES_ON_DELETE); }
    public boolean provideTransactionMetadata() { return getBoolean(PROVIDE_TRANSACTION_METADATA); }
}
