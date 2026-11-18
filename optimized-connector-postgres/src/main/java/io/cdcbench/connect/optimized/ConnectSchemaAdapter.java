package io.cdcbench.connect.optimized;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Converts the version-pinned JSON schema model into Kafka Connect data for SMTs. */
final class ConnectSchemaAdapter {
    private ConnectSchemaAdapter() {
    }

    static AdaptedValue adapt(Map<String, Object> description, Object value) {
        Schema schema = schema(description);
        return new AdaptedValue(schema, value(description, schema, value));
    }

    private static Schema schema(Map<String, Object> description) {
        return schemaBuilder(description).build();
    }

    static SchemaBuilder schemaBuilder(Map<String, Object> description) {
        String type = requiredText(description, "type");
        SchemaBuilder builder = switch (type) {
            case "boolean" -> SchemaBuilder.bool();
            case "int8" -> SchemaBuilder.int8();
            case "int16" -> SchemaBuilder.int16();
            case "int32" -> SchemaBuilder.int32();
            case "int64" -> SchemaBuilder.int64();
            case "float", "float32" -> SchemaBuilder.float32();
            case "double", "float64" -> SchemaBuilder.float64();
            case "string" -> SchemaBuilder.string();
            case "bytes" -> SchemaBuilder.bytes();
            case "array" -> SchemaBuilder.array(schema(childMap(description, "items")));
            case "map" -> SchemaBuilder.map(
                    schema(childMap(description, "keys")),
                    schema(childMap(description, "values")));
            case "struct" -> struct(description);
            default -> throw new IllegalArgumentException("Unsupported Connect schema type " + type);
        };
        applyMetadata(builder, description);
        if (description.containsKey("default")) {
            Schema withoutDefault = builder.build();
            builder.defaultValue(value(
                    description, withoutDefault, description.get("default")));
        }
        return builder;
    }

    private static SchemaBuilder struct(Map<String, Object> description) {
        SchemaBuilder builder = SchemaBuilder.struct();
        Object fields = description.get("fields");
        if (!(fields instanceof List<?> definitions)) {
            throw new IllegalArgumentException("Connect struct schema has no fields");
        }
        for (Object item : definitions) {
            if (!(item instanceof Map<?, ?> raw)) {
                throw new IllegalArgumentException("Connect struct field is not an object");
            }
            Map<String, Object> field = stringMap(raw);
            builder.field(requiredText(field, "field"), schema(field));
        }
        return builder;
    }

    private static void applyMetadata(
            SchemaBuilder builder, Map<String, Object> description) {
        Object name = description.get("name");
        if (name != null) {
            builder.name(name.toString());
        }
        Object version = description.get("version");
        if (version instanceof Number number) {
            builder.version(number.intValue());
        }
        Object doc = description.get("doc");
        if (doc != null) {
            builder.doc(doc.toString());
        }
        Object parameters = description.get("parameters");
        if (parameters instanceof Map<?, ?> raw) {
            Map<String, String> converted = new LinkedHashMap<>();
            raw.forEach((key, value) -> converted.put(key.toString(), value.toString()));
            builder.parameters(converted);
        }
        if (Boolean.TRUE.equals(description.get("optional"))) {
            builder.optional();
        }
    }

    private static Object value(
            Map<String, Object> description, Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        return switch (requiredText(description, "type")) {
            case "struct" -> structValue(description, schema, value);
            case "array" -> arrayValue(description, schema, value);
            case "map" -> mapValue(description, schema, value);
            case "bytes" -> bytesValue(schema, value);
            case "int8" -> ((Number) value).byteValue();
            case "int16" -> ((Number) value).shortValue();
            case "int32" -> ((Number) value).intValue();
            case "int64" -> ((Number) value).longValue();
            case "float", "float32" -> ((Number) value).floatValue();
            case "double", "float64" -> ((Number) value).doubleValue();
            default -> value;
        };
    }

    private static Struct structValue(
            Map<String, Object> description, Schema schema, Object value) {
        if (!(value instanceof Map<?, ?> rawValue)) {
            throw new IllegalArgumentException("Connect struct value is not an object");
        }
        Map<String, Object> values = stringMap(rawValue);
        Object fields = description.get("fields");
        if (!(fields instanceof List<?> definitions)) {
            throw new IllegalArgumentException("Connect struct schema has no fields");
        }
        Struct struct = new Struct(schema);
        for (Object item : definitions) {
            if (!(item instanceof Map<?, ?> rawField)) {
                throw new IllegalArgumentException("Connect struct field is not an object");
            }
            Map<String, Object> field = stringMap(rawField);
            String name = requiredText(field, "field");
            if (values.containsKey(name)) {
                struct.put(name, value(field, schema.field(name).schema(), values.get(name)));
            }
        }
        return struct;
    }

    private static List<Object> arrayValue(
            Map<String, Object> description, Schema schema, Object value) {
        if (!(value instanceof List<?> values)) {
            throw new IllegalArgumentException("Connect array value is not a list");
        }
        Map<String, Object> item = childMap(description, "items");
        List<Object> converted = new ArrayList<>(values.size());
        for (Object element : values) {
            converted.add(value(item, schema.valueSchema(), element));
        }
        return Collections.unmodifiableList(converted);
    }

    private static Map<Object, Object> mapValue(
            Map<String, Object> description, Schema schema, Object value) {
        if (!(value instanceof Map<?, ?> values)) {
            throw new IllegalArgumentException("Connect map value is not an object");
        }
        Map<String, Object> key = childMap(description, "keys");
        Map<String, Object> item = childMap(description, "values");
        Map<Object, Object> converted = new LinkedHashMap<>();
        values.forEach((entryKey, entryValue) -> converted.put(
                value(key, schema.keySchema(), entryKey),
                value(item, schema.valueSchema(), entryValue)));
        return Collections.unmodifiableMap(converted);
    }

    static Map<String, Object> describe(Schema schema, String field) {
        Map<String, Object> description = new LinkedHashMap<>();
        description.put("type", switch (schema.type()) {
            case INT8 -> "int8";
            case INT16 -> "int16";
            case INT32 -> "int32";
            case INT64 -> "int64";
            case FLOAT32 -> "float";
            case FLOAT64 -> "double";
            case BOOLEAN -> "boolean";
            case STRING -> "string";
            case BYTES -> "bytes";
            case ARRAY -> "array";
            case MAP -> "map";
            case STRUCT -> "struct";
        });
        if (schema.name() != null) {
            description.put("name", schema.name());
        }
        if (schema.version() != null) {
            description.put("version", schema.version());
        }
        if (schema.doc() != null) {
            description.put("doc", schema.doc());
        }
        if (schema.parameters() != null && !schema.parameters().isEmpty()) {
            description.put("parameters", new LinkedHashMap<>(schema.parameters()));
        }
        description.put("optional", schema.isOptional());
        if (schema.type() == Schema.Type.ARRAY) {
            description.put("items", describe(schema.valueSchema(), null));
        } else if (schema.type() == Schema.Type.MAP) {
            description.put("keys", describe(schema.keySchema(), null));
            description.put("values", describe(schema.valueSchema(), null));
        } else if (schema.type() == Schema.Type.STRUCT) {
            List<Map<String, Object>> fields = new ArrayList<>();
            schema.fields().forEach(item -> fields.add(describe(item.schema(), item.name())));
            description.put("fields", fields);
        }
        if (schema.defaultValue() != null) {
            description.put("default", encode(schema, schema.defaultValue()));
        }
        if (field != null) {
            description.put("field", field);
        }
        return description;
    }

    static Object encode(Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        if ("org.apache.kafka.connect.data.Decimal".equals(schema.name())
                && value instanceof BigDecimal decimal) {
            return decimal.unscaledValue().toByteArray();
        }
        return switch (schema.type()) {
            case BYTES -> bytesValue(schema, value);
            case ARRAY -> {
                List<?> values = (List<?>) value;
                List<Object> encoded = new ArrayList<>(values.size());
                values.forEach(item -> encoded.add(encode(schema.valueSchema(), item)));
                yield Collections.unmodifiableList(encoded);
            }
            case MAP -> {
                Map<?, ?> values = (Map<?, ?>) value;
                Map<Object, Object> encoded = new LinkedHashMap<>();
                values.forEach((key, item) -> encoded.put(
                        encode(schema.keySchema(), key),
                        encode(schema.valueSchema(), item)));
                yield Collections.unmodifiableMap(encoded);
            }
            case STRUCT -> {
                Struct struct = (Struct) value;
                Map<String, Object> encoded = new LinkedHashMap<>();
                schema.fields().forEach(item -> encoded.put(
                        item.name(), encode(item.schema(), struct.get(item))));
                yield encoded;
            }
            default -> value;
        };
    }

    private static Object bytesValue(Schema schema, Object value) {
        byte[] bytes;
        if (value instanceof byte[] raw) {
            bytes = raw.clone();
        } else if (value instanceof ByteBuffer buffer) {
            ByteBuffer copy = buffer.duplicate();
            bytes = new byte[copy.remaining()];
            copy.get(bytes);
        } else if (value instanceof String encoded) {
            bytes = Base64.getDecoder().decode(encoded);
        } else {
            throw new IllegalArgumentException("Connect bytes value has unsupported type "
                    + value.getClass().getName());
        }
        if ("org.apache.kafka.connect.data.Decimal".equals(schema.name())) {
            String rawScale = schema.parameters().get("scale");
            int scale = Integer.parseInt(rawScale);
            return new BigDecimal(new BigInteger(bytes), scale);
        }
        return bytes;
    }

    private static Map<String, Object> childMap(
            Map<String, Object> description, String key) {
        Object value = description.get(key);
        if (!(value instanceof Map<?, ?> raw)) {
            throw new IllegalArgumentException("Connect schema " + key + " is not an object");
        }
        return stringMap(raw);
    }

    private static Map<String, Object> stringMap(Map<?, ?> raw) {
        Map<String, Object> converted = new LinkedHashMap<>();
        raw.forEach((key, value) -> converted.put(key.toString(), value));
        return converted;
    }

    private static String requiredText(Map<String, Object> value, String key) {
        Object text = value.get(key);
        if (text == null || text.toString().isBlank()) {
            throw new IllegalArgumentException("Connect schema has no " + key);
        }
        return text.toString();
    }

    record AdaptedValue(Schema schema, Object value) {
    }
}
