package io.debezium.schema;

import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Native regex routing strategy, including collision-safe key augmentation. */
public class SchemaRegexTopicNamingStrategy extends SchemaTopicNamingStrategy {
    private Pattern topicRegex;
    private String topicReplacement;
    private boolean keyEnforceUniqueness;
    private String keyFieldName;
    private Pattern keyFieldRegex;
    private String keyFieldReplacement;

    public SchemaRegexTopicNamingStrategy() {
    }

    public SchemaRegexTopicNamingStrategy(Properties properties) {
        configure(properties);
    }

    @Override
    public void configure(Properties properties) {
        super.configure(properties);
        topicRegex = pattern(properties, "topic.regex", true);
        topicReplacement = required(properties, "topic.replacement");
        keyEnforceUniqueness = Boolean.parseBoolean(properties.getProperty(
                "topic.key.enforce.uniqueness", "true"));
        keyFieldName = properties.getProperty("topic.key.field.name", "__dbz__physicalTableIdentifier");
        keyFieldRegex = pattern(properties, "topic.key.field.regex", false);
        keyFieldReplacement = properties.getProperty("topic.key.field.replacement", "$0");
    }

    @Override
    public String dataChangeTopic(DataCollectionId id) {
        String origin = getOriginTopic(id);
        return sanitizedTopicName(topicRegex.matcher(origin).replaceAll(topicReplacement));
    }

    public String getOriginTopic(DataCollectionId id) {
        return originTopic(id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public TopicNamingStrategy.TopicSchemaAugment<SchemaBuilder> keySchemaAugment() {
        if (!keyEnforceUniqueness) {
            return (TopicNamingStrategy.TopicSchemaAugment<SchemaBuilder>)
                    (TopicNamingStrategy.TopicSchemaAugment<?>) TopicNamingStrategy.NO_SCHEMA_OP;
        }
        return schema -> {
            if (schema == null) {
                return false;
            }
            schema.field(keyFieldName, Schema.STRING_SCHEMA);
            return true;
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public TopicNamingStrategy.TopicValueAugment<DataCollectionId, Schema, Struct> keyValueAugment() {
        if (!keyEnforceUniqueness) {
            return (TopicNamingStrategy.TopicValueAugment<DataCollectionId, Schema, Struct>)
                    (TopicNamingStrategy.TopicValueAugment<?, ?, ?>) TopicNamingStrategy.NO_VALUE_OP;
        }
        return (id, schema, value) -> {
            if (id == null || value == null) {
                return false;
            }
            String identifier = getOriginTopic(id);
            String physical = keyFieldRegex == null
                    ? identifier : keyFieldRegex.matcher(identifier).replaceAll(keyFieldReplacement);
            value.put(keyFieldName, physical);
            return true;
        };
    }

    private static Pattern pattern(Properties properties, String key, boolean required) {
        String expression = properties.getProperty(key);
        if (expression == null || expression.isBlank()) {
            if (required) {
                throw new ConfigException(key, expression, "must not be blank");
            }
            return null;
        }
        try {
            return Pattern.compile(expression);
        } catch (PatternSyntaxException error) {
            throw new ConfigException(key, expression, "must be a valid regular expression");
        }
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new ConfigException(key, value, "must not be blank");
        }
        return value;
    }
}
