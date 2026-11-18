package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class TopicNamesTest {
    @Test
    void usesTheDefaultSchemaTopicContract() {
        TopicNames names = new TopicNames(Map.of("topic.prefix", "server"));

        assertEquals("server.public.events", names.dataChangeTopic("public", "events"));
        assertEquals("server.public.events", names.recordSchemaPrefix("public", "events"));
        assertEquals("__debezium-heartbeat.server", names.heartbeatTopic());
        assertEquals("server.transaction", names.transactionTopic());
    }

    @Test
    void loadsAndConfiguresARegexTopicNamingStrategy() {
        Map<String, String> properties = new HashMap<>();
        properties.put("topic.prefix", "server");
        properties.put(TopicNames.STRATEGY,
                "io.debezium.schema.SchemaRegexTopicNamingStrategy");
        properties.put(TopicNames.TOPIC_REGEX,
                "^([^.]+)\\.([^.]+)\\.([^.]+)$");
        properties.put(TopicNames.TOPIC_REPLACEMENT, "$1.routed.$2.$3");
        properties.put(TopicNames.TOPIC_KEY_ENFORCE_UNIQUENESS, "false");

        TopicNames names = new TopicNames(properties);

        assertEquals("server.routed.public.events",
                names.dataChangeTopic("public", "events"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void appliesTopicKeySchemaAndValueAugments() {
        Map<String, String> properties = new HashMap<>();
        properties.put("topic.prefix", "server");
        properties.put(TopicNames.STRATEGY,
                "io.debezium.schema.SchemaRegexTopicNamingStrategy");
        properties.put(TopicNames.TOPIC_REGEX,
                "^([^.]+)\\.([^.]+)\\.([^.]+)$");
        properties.put(TopicNames.TOPIC_REPLACEMENT, "$1.routed");
        properties.put(TopicNames.TOPIC_KEY_FIELD_NAME, "__physical");
        properties.put(TopicNames.TOPIC_KEY_FIELD_REGEX,
                "^[^.]+\\.([^.]+)\\.([^.]+)$");
        properties.put(TopicNames.TOPIC_KEY_FIELD_REPLACEMENT, "$1.$2");
        TopicNames names = new TopicNames(properties);
        Map<String, Object> id = new LinkedHashMap<>();
        id.put("type", "int64");
        id.put("optional", false);
        id.put("field", "id");
        Map<String, Object> keySchema = new LinkedHashMap<>();
        keySchema.put("type", "struct");
        keySchema.put("name", "server.public.events.Key");
        keySchema.put("optional", false);
        keySchema.put("fields", List.of(id));

        Map<String, Object> augmentedSchema = names.augmentKeySchema(
                "public", "events", keySchema);
        Map<String, Object> augmentedKey = names.augmentKeyValue(
                "public", "events", augmentedSchema, Map.of("id", 7L));
        List<Map<String, Object>> fields =
                (List<Map<String, Object>>) augmentedSchema.get("fields");

        assertEquals(List.of("id", "__physical"),
                fields.stream().map(field -> field.get("field").toString()).toList());
        assertEquals(Map.of("id", 7L, "__physical", "public.events"), augmentedKey);
    }

    @Test
    void rejectsMissingOrIncompatibleStrategies() {
        assertThrows(ConfigException.class, () -> new TopicNames(Map.of(
                "topic.prefix", "server", TopicNames.STRATEGY, "missing.Strategy")));
        assertThrows(ConfigException.class, () -> new TopicNames(Map.of(
                "topic.prefix", "server", TopicNames.STRATEGY, "java.lang.String")));
    }
}
