package io.debezium.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Clean-room implementation of the Debezium PostgreSQL event-unwrapping
 * configuration contract used by existing Kafka Connect deployments.
 */
public final class ExtractNewRecordState<R extends ConnectRecord<R>>
        implements Transformation<R> {
    private static final String DELETE_MODE = "delete.tombstone.handling.mode";
    private static final String DROP_TOMBSTONES = "drop.tombstones";
    private static final String ADD_FIELDS = "add.fields";
    private static final String DELETED_FIELD = "__deleted";
    private static final List<String> DELETE_MODES = List.of(
            "drop", "tombstone", "rewrite", "rewrite-with-tombstone", "delete-to-tombstone");
    private static final ConfigDef CONFIG = new ConfigDef()
            .define(DELETE_MODE, ConfigDef.Type.STRING, "drop",
                    ConfigDef.ValidString.in(DELETE_MODES.toArray(String[]::new)),
                    ConfigDef.Importance.MEDIUM,
                    "How delete records and their tombstones are represented")
            .define(DROP_TOMBSTONES, ConfigDef.Type.BOOLEAN, true,
                    ConfigDef.Importance.LOW, "Whether input tombstones are removed")
            .define(ADD_FIELDS, ConfigDef.Type.LIST, List.of(),
                    ConfigDef.Importance.LOW, "Envelope metadata fields added to the result");

    private final Map<Schema, Schema> schemas = new ConcurrentHashMap<>();
    private String deleteMode = "drop";
    private boolean dropTombstones = true;
    private List<String> addFields = List.of();

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return retainTombstone() ? record : null;
        }
        if (record.value() instanceof Struct envelope) {
            return unwrapSchemaful(record, envelope);
        }
        if (record.value() instanceof Map<?, ?> envelope) {
            return unwrapSchemaless(record, envelope);
        }
        return record;
    }

    private R unwrapSchemaful(R record, Struct envelope) {
        String operation = text(envelope.get("op"));
        boolean deleted = "d".equals(operation);
        if (deleted && dropsDeleteRecord()) {
            return null;
        }
        if (deleted && "delete-to-tombstone".equals(deleteMode)) {
            return newRecord(record, null, null);
        }
        Object selected = envelope.get(deleted ? "before" : "after");
        if (!(selected instanceof Struct row)) {
            return record;
        }
        Schema outputSchema = schemas.computeIfAbsent(row.schema(), this::outputSchema);
        Struct output = new Struct(outputSchema);
        for (Field field : row.schema().fields()) {
            output.put(field.name(), row.get(field));
        }
        if (rewritesDeletes()) {
            output.put(DELETED_FIELD, Boolean.toString(deleted));
        }
        Struct source = envelope.get("source") instanceof Struct value ? value : null;
        for (String field : addFields) {
            output.put(outputName(field), metadata(field, operation, source, null));
        }
        return newRecord(record, outputSchema, output);
    }

    private R unwrapSchemaless(R record, Map<?, ?> envelope) {
        String operation = text(envelope.get("op"));
        boolean deleted = "d".equals(operation);
        if (deleted && dropsDeleteRecord()) {
            return null;
        }
        if (deleted && "delete-to-tombstone".equals(deleteMode)) {
            return newRecord(record, null, null);
        }
        Object selected = envelope.get(deleted ? "before" : "after");
        if (!(selected instanceof Map<?, ?> row)) {
            return record;
        }
        Map<String, Object> output = new LinkedHashMap<>();
        row.forEach((key, value) -> output.put(String.valueOf(key), value));
        if (rewritesDeletes()) {
            output.put(DELETED_FIELD, Boolean.toString(deleted));
        }
        Map<?, ?> source = envelope.get("source") instanceof Map<?, ?> value ? value : null;
        for (String field : addFields) {
            output.put(outputName(field), metadata(field, operation, null, source));
        }
        return newRecord(record, null, output);
    }

    private Schema outputSchema(Schema rowSchema) {
        SchemaBuilder builder = SchemaBuilder.struct();
        if (rowSchema.name() != null) {
            builder.name(rowSchema.name());
        }
        if (rowSchema.doc() != null) {
            builder.doc(rowSchema.doc());
        }
        if (rowSchema.version() != null) {
            builder.version(rowSchema.version());
        }
        if (rowSchema.parameters() != null) {
            builder.parameters(rowSchema.parameters());
        }
        for (Field field : rowSchema.fields()) {
            builder.field(field.name(), field.schema());
        }
        if (rewritesDeletes()) {
            builder.field(DELETED_FIELD, SchemaBuilder.string().optional().build());
        }
        for (String field : addFields) {
            builder.field(outputName(field), metadataSchema(field));
        }
        return builder.build();
    }

    private static Schema metadataSchema(String field) {
        return "lsn".equals(sourceName(field))
                ? SchemaBuilder.int64().optional().build()
                : SchemaBuilder.string().optional().build();
    }

    private static Object metadata(String field, String operation,
                                   Struct sourceStruct, Map<?, ?> sourceMap) {
        String sourceName = sourceName(field);
        if ("op".equals(sourceName)) {
            return operation;
        }
        if (sourceStruct != null && sourceStruct.schema().field(sourceName) != null) {
            return sourceStruct.get(sourceName);
        }
        return sourceMap == null ? null : sourceMap.get(sourceName);
    }

    private R newRecord(R record, Schema valueSchema, Object value) {
        return record.newRecord(
                record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                valueSchema, value, record.timestamp(), record.headers());
    }

    private boolean dropsDeleteRecord() {
        return "drop".equals(deleteMode) || "tombstone".equals(deleteMode);
    }

    private boolean rewritesDeletes() {
        return "rewrite".equals(deleteMode) || "rewrite-with-tombstone".equals(deleteMode);
    }

    private boolean retainTombstone() {
        return !dropTombstones && !"rewrite".equals(deleteMode)
                || "tombstone".equals(deleteMode)
                || "rewrite-with-tombstone".equals(deleteMode);
    }

    @Override
    public ConfigDef config() {
        return CONFIG;
    }

    @Override
    public void close() {
        schemas.clear();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object configuredMode = configs.containsKey(DELETE_MODE)
                ? configs.get(DELETE_MODE) : "drop";
        deleteMode = String.valueOf(configuredMode);
        if (!DELETE_MODES.contains(deleteMode)) {
            throw new ConfigException(DELETE_MODE, deleteMode, "unsupported delete mode");
        }
        Object configuredDropTombstones = configs.containsKey(DROP_TOMBSTONES)
                ? configs.get(DROP_TOMBSTONES) : true;
        dropTombstones = Boolean.parseBoolean(String.valueOf(configuredDropTombstones));
        addFields = parseList(configs.get(ADD_FIELDS));
        schemas.clear();
    }

    private static List<String> parseList(Object configured) {
        if (configured == null) {
            return List.of();
        }
        List<String> fields = new ArrayList<>();
        if (configured instanceof List<?> values) {
            values.forEach(value -> addField(fields, String.valueOf(value)));
        } else {
            for (String value : String.valueOf(configured).split(",")) {
                addField(fields, value);
            }
        }
        return List.copyOf(fields);
    }

    private static void addField(List<String> fields, String value) {
        String field = value.trim();
        if (!field.isEmpty()) {
            String source = sourceName(field);
            if (!List.of("op", "table", "lsn").contains(source)) {
                throw new ConfigException(ADD_FIELDS, value, "unsupported metadata field");
            }
            fields.add(field);
        }
    }

    private static String sourceName(String field) {
        int separator = field.indexOf(':');
        return separator < 0 ? field : field.substring(0, separator);
    }

    private static String outputName(String field) {
        int separator = field.indexOf(':');
        return "__" + (separator < 0 ? field : field.substring(separator + 1));
    }

    private static String text(Object value) {
        return value == null ? null : value.toString();
    }
}
