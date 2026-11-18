package io.debezium.schema;

import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

/** Native implementation of Debezium's qualified schema-based topic contract. */
public class SchemaTopicNamingStrategy implements TopicNamingStrategy<DataCollectionId> {
    protected String prefix;
    protected String delimiter;
    protected String transaction;
    protected String heartbeatPrefix;

    public SchemaTopicNamingStrategy() {
    }

    public SchemaTopicNamingStrategy(Properties properties) {
        configure(properties);
    }

    public SchemaTopicNamingStrategy(Properties properties, boolean multiPartitionMode) {
        configure(properties);
    }

    @Override
    public void configure(Properties properties) {
        Objects.requireNonNull(properties, "properties");
        prefix = required(properties, "topic.prefix");
        delimiter = properties.getProperty("topic.delimiter", ".");
        if (delimiter.isEmpty()) {
            throw new ConfigException("topic.delimiter", delimiter, "must not be empty");
        }
        transaction = properties.getProperty("topic.transaction", "transaction");
        heartbeatPrefix = properties.getProperty(
                "heartbeat.topics.prefix", "__debezium-heartbeat");
    }

    @Override
    public String dataChangeTopic(DataCollectionId id) {
        return sanitizedTopicName(join(id.schemaParts()));
    }

    @Override
    public String schemaChangeTopic() {
        return prefix;
    }

    @Override
    public String heartbeatTopic() {
        return sanitizedTopicName(heartbeatPrefix + delimiter + prefix);
    }

    @Override
    public String transactionTopic() {
        return sanitizedTopicName(prefix + delimiter + transaction);
    }

    @Override
    public String sanitizedTopicName(String topicName) {
        if (topicName == null) {
            return null;
        }
        StringBuilder result = new StringBuilder(Math.min(topicName.length(), MAX_NAME_LENGTH));
        for (int index = 0; index < topicName.length() && result.length() < MAX_NAME_LENGTH; index++) {
            char character = topicName.charAt(index);
            result.append(valid(character) ? character : '_');
        }
        return result.toString();
    }

    protected String originTopic(DataCollectionId id) {
        return sanitizedTopicName(join(id.schemaParts()));
    }

    private String join(List<String> parts) {
        StringBuilder result = new StringBuilder(prefix);
        for (String part : parts) {
            if (part == null || part.isEmpty()) {
                continue;
            }
            result.append(delimiter).append(part);
        }
        return result.toString();
    }

    private static boolean valid(char character) {
        return character >= 'a' && character <= 'z'
                || character >= 'A' && character <= 'Z'
                || character >= '0' && character <= '9'
                || character == '.' || character == '_' || character == '-';
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new ConfigException(key, value, "must not be blank");
        }
        return value.trim();
    }
}
