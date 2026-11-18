package io.cdcbench.connect.optimized;

import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Properties;

/** Loads the public Debezium topic-naming SPI without delegating CDC processing. */
final class TopicNames {
    static final String STRATEGY = "topic.naming.strategy";
    static final String DEFAULT_STRATEGY = "io.debezium.schema.SchemaTopicNamingStrategy";
    static final String HEARTBEAT_TOPICS_PREFIX = "heartbeat.topics.prefix";
    static final String TOPIC_REGEX = "topic.regex";
    static final String TOPIC_REPLACEMENT = "topic.replacement";
    static final String TOPIC_KEY_ENFORCE_UNIQUENESS = "topic.key.enforce.uniqueness";
    static final String TOPIC_KEY_FIELD_NAME = "topic.key.field.name";
    static final String TOPIC_KEY_FIELD_REGEX = "topic.key.field.regex";
    static final String TOPIC_KEY_FIELD_REPLACEMENT = "topic.key.field.replacement";

    private final TopicNamingStrategy<DataCollectionId> strategy;
    private final TopicNamingStrategy.TopicSchemaAugment<SchemaBuilder> keySchemaAugment;
    private final TopicNamingStrategy.TopicValueAugment<
            DataCollectionId, Schema, Struct> keyValueAugment;

    TopicNames(Map<String, String> properties) {
        String className = properties.getOrDefault(STRATEGY, DEFAULT_STRATEGY).trim();
        if (className.isEmpty()) {
            throw new ConfigException(STRATEGY, className, "must name a TopicNamingStrategy class");
        }
        Properties configuration = new Properties();
        configuration.putAll(properties);
        strategy = instantiate(className, configuration);
        keySchemaAugment = keySchemaAugment(strategy);
        keyValueAugment = keyValueAugment(strategy);
    }

    String dataChangeTopic(String schema, String table) {
        return strategy.dataChangeTopic(tableId(schema, table));
    }

    String recordSchemaPrefix(String schema, String table) {
        return strategy.recordSchemaPrefix(tableId(schema, table));
    }

    String heartbeatTopic() {
        return strategy.heartbeatTopic();
    }

    String transactionTopic() {
        return strategy.transactionTopic();
    }

    Map<String, Object> augmentKeySchema(
            String schema, String table, Map<String, Object> description) {
        if (keySchemaAugment == TopicNamingStrategy.NO_SCHEMA_OP) {
            return description;
        }
        SchemaBuilder builder = ConnectSchemaAdapter.schemaBuilder(description);
        if (!keySchemaAugment.augment(builder)) {
            return description;
        }
        return ConnectSchemaAdapter.describe(builder.build(), null);
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> augmentKeyValue(
            String schema, String table, Map<String, Object> keySchema,
            Map<String, Object> key) {
        if (keyValueAugment == TopicNamingStrategy.NO_VALUE_OP) {
            return key;
        }
        ConnectSchemaAdapter.AdaptedValue adapted =
                ConnectSchemaAdapter.adapt(keySchema, key);
        Struct value = (Struct) adapted.value();
        if (!keyValueAugment.augment(
                tableId(schema, table), adapted.schema(), value)) {
            return key;
        }
        return (Map<String, Object>) ConnectSchemaAdapter.encode(
                adapted.schema(), value);
    }

    private static TableId tableId(String schema, String table) {
        return new TableId(null, schema, table);
    }

    @SuppressWarnings("unchecked")
    private static TopicNamingStrategy.TopicSchemaAugment<SchemaBuilder> keySchemaAugment(
            TopicNamingStrategy<DataCollectionId> strategy) {
        return (TopicNamingStrategy.TopicSchemaAugment<SchemaBuilder>)
                strategy.keySchemaAugment();
    }

    @SuppressWarnings("unchecked")
    private static TopicNamingStrategy.TopicValueAugment<
            DataCollectionId, Schema, Struct> keyValueAugment(
                    TopicNamingStrategy<DataCollectionId> strategy) {
        return (TopicNamingStrategy.TopicValueAugment<
                DataCollectionId, Schema, Struct>) strategy.keyValueAugment();
    }

    @SuppressWarnings("unchecked")
    private static TopicNamingStrategy<DataCollectionId> instantiate(
            String className, Properties configuration) {
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            Class<?> rawClass = Class.forName(className, true, loader);
            if (!TopicNamingStrategy.class.isAssignableFrom(rawClass)) {
                throw new ConfigException(
                        STRATEGY, className, "must implement " + TopicNamingStrategy.class.getName());
            }
            Class<? extends TopicNamingStrategy<?>> strategyClass =
                    (Class<? extends TopicNamingStrategy<?>>) rawClass;
            try {
                Constructor<? extends TopicNamingStrategy<?>> constructor =
                        strategyClass.getConstructor(Properties.class);
                return (TopicNamingStrategy<DataCollectionId>)
                        constructor.newInstance(configuration);
            } catch (NoSuchMethodException ignored) {
                Constructor<? extends TopicNamingStrategy<?>> constructor =
                        strategyClass.getConstructor();
                TopicNamingStrategy<DataCollectionId> result =
                        (TopicNamingStrategy<DataCollectionId>) constructor.newInstance();
                result.configure(configuration);
                return result;
            }
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
                 | IllegalAccessException | InvocationTargetException error) {
            throw new ConfigException(
                    STRATEGY, className, "cannot instantiate topic naming strategy: "
                            + rootMessage(error));
        }
    }

    private static String rootMessage(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null
                ? current.getClass().getSimpleName() : current.getMessage();
    }
}
