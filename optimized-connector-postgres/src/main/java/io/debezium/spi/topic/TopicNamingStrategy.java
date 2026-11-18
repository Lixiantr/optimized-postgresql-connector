package io.debezium.spi.topic;

import io.debezium.spi.schema.DataCollectionId;

import java.util.Properties;

/** Clean-room binary-compatible topic-naming extension contract. */
public interface TopicNamingStrategy<I extends DataCollectionId> {
    int MAX_NAME_LENGTH = 249;
    TopicSchemaAugment<?> NO_SCHEMA_OP = ignored -> false;
    TopicValueAugment<?, ?, ?> NO_VALUE_OP = (ignoredId, ignoredSchema, ignoredValue) -> false;

    void configure(Properties properties);

    String dataChangeTopic(I id);

    String schemaChangeTopic();

    String heartbeatTopic();

    String transactionTopic();

    default String recordSchemaPrefix(I id) {
        return dataChangeTopic(id);
    }

    String sanitizedTopicName(String topicName);

    @SuppressWarnings("rawtypes")
    default TopicSchemaAugment keySchemaAugment() {
        return NO_SCHEMA_OP;
    }

    @SuppressWarnings("rawtypes")
    default TopicValueAugment keyValueAugment() {
        return NO_VALUE_OP;
    }

    @FunctionalInterface
    interface TopicSchemaAugment<S> {
        boolean augment(S schema);
    }

    @FunctionalInterface
    interface TopicValueAugment<I extends DataCollectionId, S, R> {
        boolean augment(I id, S schema, R value);
    }
}
