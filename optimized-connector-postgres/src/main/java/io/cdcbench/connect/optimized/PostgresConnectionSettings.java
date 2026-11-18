package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;
import org.postgresql.PGProperty;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

final class PostgresConnectionSettings {
    static final String DRIVER_PREFIX = "driver.";
    static final String QUERY_TIMEOUT_MS = "database.query.timeout.ms";
    static final String INITIAL_STATEMENTS = "database.initial.statements";
    static final String SSL_MODE = "database.sslmode";
    static final String SSL_CERT = "database.sslcert";
    static final String SSL_PASSWORD = "database.sslpassword";
    static final String SSL_ROOT_CERT = "database.sslrootcert";
    static final String SSL_KEY = "database.sslkey";
    static final String SSL_FACTORY = "database.sslfactory";
    static final String TCP_KEEP_ALIVE = "database.tcpKeepAlive";

    private static final Set<String> SSL_MODES = Set.of(
            "disable", "allow", "prefer", "require", "verify-ca", "verify-full");
    private static final Map<String, String> JDBC_PROPERTIES = Map.of(
            SSL_MODE, "sslmode",
            SSL_CERT, "sslcert",
            SSL_PASSWORD, "sslpassword",
            SSL_ROOT_CERT, "sslrootcert",
            SSL_KEY, "sslkey",
            SSL_FACTORY, "sslfactory",
            TCP_KEEP_ALIVE, "tcpKeepAlive");

    private final Map<String, String> commonProperties;
    private final int queryTimeoutMillis;
    private final List<String> initialStatements;

    PostgresConnectionSettings(Map<String, String> properties) {
        String sslMode = properties.getOrDefault(SSL_MODE, "prefer").trim().toLowerCase();
        if (!SSL_MODES.contains(sslMode)) {
            throw new ConfigException(SSL_MODE, properties.get(SSL_MODE),
                    "must be one of disable, allow, prefer, require, verify-ca or verify-full");
        }

        Map<String, String> jdbc = new LinkedHashMap<>();
        jdbc.put("sslmode", sslMode);
        for (Map.Entry<String, String> mapping : JDBC_PROPERTIES.entrySet()) {
            if (mapping.getKey().equals(SSL_MODE)) {
                continue;
            }
            String value = properties.get(mapping.getKey());
            if (value != null && !value.isBlank()) {
                jdbc.put(mapping.getValue(), value.trim());
            }
        }
        String keepAlive = properties.getOrDefault(TCP_KEEP_ALIVE, "true").trim();
        if (!"true".equalsIgnoreCase(keepAlive) && !"false".equalsIgnoreCase(keepAlive)) {
            throw new ConfigException(TCP_KEEP_ALIVE, keepAlive, "must be true or false");
        }
        jdbc.put("tcpKeepAlive", Boolean.toString(Boolean.parseBoolean(keepAlive)));

        properties.forEach((key, value) -> {
            if (key.startsWith(DRIVER_PREFIX)) {
                String driverKey = key.substring(DRIVER_PREFIX.length());
                if (driverKey.isBlank()) {
                    throw new ConfigException(key, value,
                            "driver property name must not be empty");
                }
                jdbc.put(driverKey, value);
            }
        });
        commonProperties = Map.copyOf(jdbc);
        queryTimeoutMillis = nonNegativeInt(properties, QUERY_TIMEOUT_MS, 600_000);
        initialStatements = splitStatements(properties.get(INITIAL_STATEMENTS));
    }

    Properties controlProperties(String user, String password) {
        Properties properties = copyCommonProperties();
        setCredentials(properties, user, password);
        properties.putIfAbsent(PGProperty.CONNECT_TIMEOUT.getName(), "5");
        return properties;
    }

    Properties replicationProperties(String user, String password) {
        Properties properties = controlProperties(user, password);
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
        PGProperty.REPLICATION.set(properties, "database");
        PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
        return properties;
    }

    void initializeControlConnection(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT pg_catalog.set_config('statement_timeout', ?, false)")) {
            statement.setString(1, Integer.toString(queryTimeoutMillis));
            statement.executeQuery().close();
        }
        for (String sql : initialStatements) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
    }

    Map<String, String> commonProperties() {
        return commonProperties;
    }

    int queryTimeoutMillis() {
        return queryTimeoutMillis;
    }

    List<String> initialStatements() {
        return initialStatements;
    }

    private Properties copyCommonProperties() {
        Properties properties = new Properties();
        properties.putAll(commonProperties);
        return properties;
    }

    private static void setCredentials(Properties properties, String user, String password) {
        PGProperty.USER.set(properties, user);
        PGProperty.PASSWORD.set(properties, password);
    }

    private static int nonNegativeInt(
            Map<String, String> properties, String key, int defaultValue) {
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
        if (parsed < 0) {
            throw new ConfigException(key, value, "must not be negative");
        }
        return parsed;
    }

    private static List<String> splitStatements(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character != ';') {
                current.append(character);
                continue;
            }
            if (index + 1 < value.length() && value.charAt(index + 1) == ';') {
                current.append(';');
                index++;
                continue;
            }
            addStatement(statements, current);
        }
        addStatement(statements, current);
        return List.copyOf(statements);
    }

    private static void addStatement(List<String> statements, StringBuilder current) {
        String statement = current.toString().trim();
        if (!statement.isEmpty()) {
            statements.add(statement);
        }
        current.setLength(0);
    }
}
