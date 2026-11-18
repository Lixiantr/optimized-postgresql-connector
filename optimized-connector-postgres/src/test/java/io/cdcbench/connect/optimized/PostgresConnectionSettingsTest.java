package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class PostgresConnectionSettingsTest {
    @Test
    void mapsTlsAndDriverPropertiesForEveryConnectionKind() {
        PostgresConnectionSettings settings = new PostgresConnectionSettings(Map.of(
                PostgresConnectionSettings.SSL_MODE, "verify-full",
                PostgresConnectionSettings.SSL_ROOT_CERT, "/certs/ca.crt",
                PostgresConnectionSettings.SSL_CERT, "/certs/client.crt",
                PostgresConnectionSettings.SSL_KEY, "/certs/client.pk8",
                PostgresConnectionSettings.SSL_PASSWORD, "key-secret",
                PostgresConnectionSettings.TCP_KEEP_ALIVE, "false",
                PostgresConnectionSettings.DRIVER_PREFIX + "ApplicationName", "native-cdc",
                PostgresConnectionSettings.DRIVER_PREFIX + "socketTimeout", "30"));

        Properties control = settings.controlProperties("cdc", "database-secret");
        Properties replication = settings.replicationProperties("cdc", "database-secret");

        assertEquals("verify-full", control.getProperty("sslmode"));
        assertEquals("/certs/ca.crt", control.getProperty("sslrootcert"));
        assertEquals("/certs/client.crt", control.getProperty("sslcert"));
        assertEquals("/certs/client.pk8", control.getProperty("sslkey"));
        assertEquals("key-secret", control.getProperty("sslpassword"));
        assertEquals("false", control.getProperty("tcpKeepAlive"));
        assertEquals("native-cdc", control.getProperty("ApplicationName"));
        assertEquals("30", control.getProperty("socketTimeout"));
        assertEquals("cdc", control.getProperty("user"));
        assertEquals("database-secret", control.getProperty("password"));
        assertEquals("database", replication.getProperty("replication"));
        assertEquals("simple", replication.getProperty("preferQueryMode"));
        assertEquals("verify-full", replication.getProperty("sslmode"));
    }

    @Test
    void preservesSemicolonsInInitialStatementsAndQueryTimeout() {
        PostgresConnectionSettings settings = new PostgresConnectionSettings(Map.of(
                PostgresConnectionSettings.QUERY_TIMEOUT_MS, "1250",
                PostgresConnectionSettings.INITIAL_STATEMENTS,
                "SET application_name='first';SET search_path='public;;audit';;"));

        assertEquals(1250, settings.queryTimeoutMillis());
        assertEquals(java.util.List.of(
                "SET application_name='first'",
                "SET search_path='public;audit';"), settings.initialStatements());
    }

    @Test
    void refusesInvalidConnectionSettings() {
        assertThrows(ConfigException.class, () -> new PostgresConnectionSettings(Map.of(
                PostgresConnectionSettings.SSL_MODE, "encrypted-maybe")));
        assertThrows(ConfigException.class, () -> new PostgresConnectionSettings(Map.of(
                PostgresConnectionSettings.TCP_KEEP_ALIVE, "sometimes")));
        assertThrows(ConfigException.class, () -> new PostgresConnectionSettings(Map.of(
                PostgresConnectionSettings.QUERY_TIMEOUT_MS, "-1")));
        assertThrows(ConfigException.class, () -> new PostgresConnectionSettings(Map.of(
                PostgresConnectionSettings.DRIVER_PREFIX, "invalid")));
    }
}
