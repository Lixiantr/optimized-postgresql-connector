package io.cdcbench.connect.common;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class CandidateConfigTest {
    @Test
    void acceptsTheSupportedConfiguration() {
        CandidateConfig config = new CandidateConfig(validProperties());
        assertDoesNotThrow(config::validateSupportedConfiguration);
    }

    @Test
    void acceptsInitialSnapshotsAndRejectsUnimplementedModes() {
        Map<String, String> properties = validProperties();
        properties.put(CandidateConfig.SNAPSHOT_MODE, "initial");
        assertDoesNotThrow(new CandidateConfig(properties)::validateSupportedConfiguration);

        properties.put(CandidateConfig.SNAPSHOT_MODE, "always");
        assertThrows(ConfigException.class,
                new CandidateConfig(properties)::validateSupportedConfiguration);
    }

    @Test
    void rejectsSchemaChangesInsteadOfSilentlyDroppingThem() {
        Map<String, String> properties = validProperties();
        properties.put(CandidateConfig.INCLUDE_SCHEMA_CHANGES, "true");
        CandidateConfig config = new CandidateConfig(properties);
        assertThrows(ConfigException.class, config::validateSupportedConfiguration);
    }

    @Test
    void rejectsBatchSizeLargerThanTheQueue() {
        Map<String, String> properties = validProperties();
        properties.put(CandidateConfig.MAX_BATCH_SIZE, "1024");
        properties.put(CandidateConfig.MAX_QUEUE_SIZE, "512");
        CandidateConfig config = new CandidateConfig(properties);
        assertThrows(ConfigException.class, config::validateSupportedConfiguration);
    }

    @Test
    void acceptsNativeTransactionMetadata() {
        Map<String, String> properties = validProperties();
        properties.put(CandidateConfig.PROVIDE_TRANSACTION_METADATA, "true");
        CandidateConfig config = new CandidateConfig(properties);

        assertDoesNotThrow(config::validateSupportedConfiguration);
    }

    private static Map<String, String> validProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CandidateConfig.DATABASE_HOSTNAME, "postgres");
        properties.put(CandidateConfig.DATABASE_USER, "debezium");
        properties.put(CandidateConfig.DATABASE_PASSWORD, "secret");
        properties.put(CandidateConfig.DATABASE_DBNAME, "benchmark");
        properties.put(CandidateConfig.TOPIC_PREFIX, "candidate");
        properties.put(CandidateConfig.SLOT_NAME, "candidate_slot");
        properties.put(CandidateConfig.PUBLICATION_NAME, "candidate_publication");
        properties.put(CandidateConfig.TABLE_INCLUDE_LIST, "public.events");
        return properties;
    }
}
