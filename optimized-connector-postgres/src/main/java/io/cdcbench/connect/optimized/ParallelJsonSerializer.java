package io.cdcbench.connect.optimized;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/** Serializes key/value data in the materializer workers before ordered output. */
final class ParallelJsonSerializer {
    private static final Map<String, Object> SCHEMALESS_CONFIG = Map.of("schemas.enable", false);
    private static final int MAX_SCHEMA_FRAMINGS = 1_000;

    private final ThreadLocal<JsonConverter> keyConverters =
            ThreadLocal.withInitial(() -> configuredConverter(true));
    private final ThreadLocal<JsonConverter> valueConverters =
            ThreadLocal.withInitial(() -> configuredConverter(false));
    private final ThreadLocal<JsonSerializer> jsonSerializers =
            ThreadLocal.withInitial(JsonSerializer::new);
    private final Map<SchemaIdentity, Framing> schemaFramings =
            new LinkedHashMap<>(16, 0.75f, true);

    byte[] key(String topic, Object value) {
        return serialize(topic, value, keyConverters);
    }

    byte[] value(String topic, Object value) {
        return serialize(topic, value, valueConverters);
    }

    byte[] keyWithSchema(String topic, Map<String, Object> schema, Object value) {
        return serializeWithSchema(topic, schema, value, keyConverters);
    }

    byte[] valueWithSchema(String topic, Map<String, Object> schema, Object value) {
        return serializeWithSchema(topic, schema, value, valueConverters);
    }

    private byte[] serializeWithSchema(
            String topic, Map<String, Object> schema, Object value,
            ThreadLocal<JsonConverter> converters) {
        byte[] payload = serialize(topic, value, converters);
        SchemaIdentity identity = new SchemaIdentity(schema);
        Framing cached;
        synchronized (schemaFramings) {
            cached = schemaFramings.get(identity);
        }
        if (cached != null) {
            return cached.wrap(payload);
        }

        Map<String, Object> envelope = new LinkedHashMap<>(2);
        envelope.put("schema", schema);
        envelope.put("payload", value);
        byte[] exact = serialize(topic, envelope, converters);
        Framing inferred = Framing.infer(exact, payload);
        if (inferred != null) {
            synchronized (schemaFramings) {
                schemaFramings.put(identity, inferred);
                while (schemaFramings.size() > MAX_SCHEMA_FRAMINGS) {
                    SchemaIdentity eldest = schemaFramings.keySet().iterator().next();
                    schemaFramings.remove(eldest);
                }
            }
        }
        return exact;
    }

    private byte[] serialize(String topic, Object value, ThreadLocal<JsonConverter> converters) {
        if (!containsRawUtf8(value)) {
            return converters.get().fromConnectData(topic, null, value);
        }
        return jsonSerializers.get().serialize(topic, toJsonNode(value));
    }

    private static JsonConverter configuredConverter(boolean isKey) {
        JsonConverter converter = new JsonConverter();
        converter.configure(SCHEMALESS_CONFIG, isKey);
        return converter;
    }

    int schemaFramingCacheSize() {
        synchronized (schemaFramings) {
            return schemaFramings.size();
        }
    }

    private static boolean containsRawUtf8(Object value) {
        if (value instanceof RawUtf8String) {
            return true;
        }
        if (value instanceof Map<?, ?> map) {
            return map.values().stream().anyMatch(ParallelJsonSerializer::containsRawUtf8);
        }
        if (value instanceof Collection<?> collection) {
            return collection.stream().anyMatch(ParallelJsonSerializer::containsRawUtf8);
        }
        return false;
    }

    private static JsonNode toJsonNode(Object value) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        if (value == null) {
            return factory.nullNode();
        }
        if (value instanceof RawUtf8String raw) {
            return new RawUtf8TextNode(raw.bytes());
        }
        if (value instanceof String string) {
            return factory.textNode(string);
        }
        if (value instanceof Boolean bool) {
            return factory.booleanNode(bool);
        }
        if (value instanceof Byte number) {
            return factory.numberNode(number);
        }
        if (value instanceof Short number) {
            return factory.numberNode(number);
        }
        if (value instanceof Integer number) {
            return factory.numberNode(number);
        }
        if (value instanceof Long number) {
            return factory.numberNode(number);
        }
        if (value instanceof Float number) {
            return factory.numberNode(number);
        }
        if (value instanceof Double number) {
            return factory.numberNode(number);
        }
        if (value instanceof byte[] bytes) {
            return factory.binaryNode(bytes);
        }
        if (value instanceof Collection<?> collection) {
            var array = factory.arrayNode(collection.size());
            for (Object item : collection) {
                array.add(toJsonNode(item));
            }
            return array;
        }
        if (value instanceof Map<?, ?> map) {
            ObjectNode object = factory.objectNode();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new DataException("Schemaless JSON object keys must be strings");
                }
                object.set(key, toJsonNode(entry.getValue()));
            }
            return object;
        }
        throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");
    }

    private static final class RawUtf8TextNode extends ValueNode {
        private final byte[] bytes;

        private RawUtf8TextNode(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public JsonToken asToken() {
            return JsonToken.VALUE_STRING;
        }

        @Override
        public JsonNodeType getNodeType() {
            return JsonNodeType.STRING;
        }

        @Override
        public String asText() {
            return new String(bytes, StandardCharsets.UTF_8);
        }

        @Override
        public void serialize(JsonGenerator generator, SerializerProvider serializers) throws IOException {
            if (containsSupplementaryCodePoint(bytes)) {
                // JsonGenerator.writeString(String) escapes supplementary code points as the
                // same UTF-16 surrogate pair emitted by Kafka's JsonConverter. The raw UTF-8
                // API writes them literally, so retain the zero-decode path for the common BMP
                // case and use the compatible fallback only when a 4-byte UTF-8 sequence exists.
                generator.writeString(asText());
                return;
            }
            generator.writeUTF8String(bytes, 0, bytes.length);
        }

        private static boolean containsSupplementaryCodePoint(byte[] bytes) {
            for (byte value : bytes) {
                if ((value & 0xf8) == 0xf0) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof RawUtf8TextNode node && Arrays.equals(bytes, node.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    private static final class SchemaIdentity {
        private final Map<String, Object> schema;
        private final int hash;

        private SchemaIdentity(Map<String, Object> schema) {
            this.schema = schema;
            this.hash = System.identityHashCode(schema);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof SchemaIdentity identity && schema == identity.schema;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    private record Framing(byte[] prefix, byte[] suffix) {
        private Framing {
            prefix = prefix.clone();
            suffix = suffix.clone();
        }

        private static Framing infer(byte[] full, byte[] payload) {
            if (full == null || payload == null || full.length <= payload.length) {
                return null;
            }
            int payloadStart = full.length - payload.length - 1;
            if (payloadStart < 0 || full[full.length - 1] != '}') {
                return null;
            }
            for (int index = 0; index < payload.length; index++) {
                if (full[payloadStart + index] != payload[index]) {
                    return null;
                }
            }
            return new Framing(
                    Arrays.copyOfRange(full, 0, payloadStart),
                    Arrays.copyOfRange(full, payloadStart + payload.length, full.length));
        }

        private byte[] wrap(byte[] payload) {
            byte[] output = new byte[Math.addExact(
                    Math.addExact(prefix.length, payload.length), suffix.length)];
            System.arraycopy(prefix, 0, output, 0, prefix.length);
            System.arraycopy(payload, 0, output, prefix.length, payload.length);
            System.arraycopy(suffix, 0, output, prefix.length + payload.length, suffix.length);
            return output;
        }
    }
}
