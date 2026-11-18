package io.cdcbench.connect.optimized;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Parses the public Debezium signaling-table contract without depending on its runtime. */
final class SnapshotSignal {
    private static final ObjectMapper JSON = new ObjectMapper();

    private SnapshotSignal() {
    }

    static Optional<Request> parseKafka(
            String expectedKey, String key, String value) {
        if (!Objects.equals(expectedKey, key)) {
            return Optional.empty();
        }
        final JsonNode envelope;
        try {
            envelope = JSON.readTree(value);
        } catch (JsonProcessingException error) {
            throw new ConnectException("Kafka signal contains invalid JSON", error);
        }
        if (envelope == null || !envelope.isObject()) {
            throw new ConnectException("Kafka signal value must be a JSON object");
        }
        JsonNode type = envelope.get("type");
        JsonNode data = envelope.get("data");
        if (type == null || !type.isTextual() || type.textValue().isBlank()) {
            throw new ConnectException("Kafka signal requires a non-empty type");
        }
        if (data == null || !data.isObject()) {
            throw new ConnectException("Kafka signal requires a JSON object data value");
        }
        try {
            return parse(Map.of(
                    "type", type.textValue(),
                    "data", JSON.writeValueAsString(data)));
        } catch (JsonProcessingException error) {
            throw new ConnectException("Cannot normalize Kafka signal data", error);
        }
    }

    static Optional<Request> parse(Map<String, Object> row) {
        Action action = switch (String.valueOf(row.get("type"))) {
            case "execute-snapshot" -> Action.EXECUTE;
            case "stop-snapshot" -> Action.STOP;
            default -> null;
        };
        if (action == null) {
            return Optional.empty();
        }
        Object rawData = row.get("data");
        if (!(rawData instanceof String data) || data.isBlank()) {
            throw new ConnectException(action.signalName()
                    + " signal requires a non-empty JSON data value");
        }

        final JsonNode root;
        try {
            root = JSON.readTree(data);
        } catch (JsonProcessingException error) {
            throw new ConnectException(action.signalName()
                    + " signal contains invalid JSON data", error);
        }
        String rawType = optionalText(root, "type");
        if (rawType == null && action == Action.EXECUTE) {
            rawType = "incremental";
        }
        if (rawType == null) {
            throw new ConnectException(action.signalName()
                    + " signal requires a non-empty type");
        }
        Type type = switch (rawType.toLowerCase(Locale.ROOT)) {
            case "blocking" -> Type.BLOCKING;
            case "incremental" -> Type.INCREMENTAL;
            default -> throw new ConnectException(
                    action.signalName()
                            + " signal type must be blocking or incremental, got " + rawType);
        };
        if (action == Action.STOP && type != Type.INCREMENTAL) {
            throw new ConnectException("stop-snapshot supports only incremental snapshots");
        }

        JsonNode collectionsNode = root.get("data-collections");
        if (collectionsNode == null && action == Action.EXECUTE) {
            throw new ConnectException(
                    "execute-snapshot signal requires a data-collections array");
        }
        if (collectionsNode != null && !collectionsNode.isArray()) {
            throw new ConnectException(
                    action.signalName() + " signal data-collections must be an array");
        }
        List<String> collections = new ArrayList<>(
                collectionsNode == null ? 0 : collectionsNode.size());
        if (collectionsNode != null) {
            for (JsonNode item : collectionsNode) {
                if (!item.isTextual() || item.textValue().isBlank()) {
                    throw new ConnectException(action.signalName()
                            + " data-collections entries must be non-empty strings");
                }
                collections.add(qualifiedTable(item.textValue(), action).canonicalName());
            }
        }
        if (action == Action.EXECUTE && type == Type.BLOCKING && collections.isEmpty()) {
            throw new ConnectException(
                    "blocking execute-snapshot requires a non-empty data-collections array");
        }

        Map<String, String> additionalConditions = parseAdditionalConditions(root);
        if (action == Action.STOP && !additionalConditions.isEmpty()) {
            throw new ConnectException(
                    "stop-snapshot signal must not contain additional-conditions");
        }
        if (type == Type.BLOCKING && !additionalConditions.isEmpty()) {
            throw new ConnectException(
                    "additional-conditions are supported only for incremental snapshots");
        }
        for (String collection : additionalConditions.keySet()) {
            if (!collections.contains(collection)) {
                throw new ConnectException("additional-condition data collection " + collection
                        + " is not present in data-collections");
            }
        }
        return Optional.of(new Request(
                action, type, List.copyOf(collections), Map.copyOf(additionalConditions)));
    }

    private static Map<String, String> parseAdditionalConditions(JsonNode root) {
        JsonNode conditionsNode = root.get("additional-conditions");
        if (conditionsNode == null) {
            return Map.of();
        }
        if (!conditionsNode.isArray()) {
            throw new ConnectException("additional-conditions must be an array");
        }
        Map<String, String> conditions = new LinkedHashMap<>();
        for (JsonNode condition : conditionsNode) {
            if (!condition.isObject()) {
                throw new ConnectException("additional-conditions entries must be objects");
            }
            String collection = optionalText(condition, "data-collection");
            String filter = optionalText(condition, "filter");
            if (collection == null || filter == null) {
                throw new ConnectException("additional-condition requires non-empty "
                        + "data-collection and filter values");
            }
            String canonical = qualifiedTable(collection, Action.EXECUTE).canonicalName();
            if (conditions.putIfAbsent(canonical, filter) != null) {
                throw new ConnectException("additional-condition is duplicated for " + collection);
            }
        }
        return conditions;
    }

    private static QualifiedTable qualifiedTable(String value, Action action) {
        try {
            return QualifiedTable.parse(value);
        } catch (IllegalArgumentException error) {
            throw new ConnectException(action.signalName()
                    + " data collection must be an exact schema.table name: " + value, error);
        }
    }

    private static String optionalText(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isTextual() || value.textValue().isBlank()) {
            return null;
        }
        return value.textValue();
    }

    enum Action {
        EXECUTE("execute-snapshot"),
        STOP("stop-snapshot");

        private final String signalName;

        Action(String signalName) {
            this.signalName = signalName;
        }

        String signalName() {
            return signalName;
        }
    }

    enum Type {
        BLOCKING,
        INCREMENTAL
    }

    record Request(Action action, Type type, List<String> dataCollections,
                   Map<String, String> additionalConditions) {
        Request(Type type, List<String> dataCollections) {
            this(Action.EXECUTE, type, dataCollections, Map.of());
        }

        boolean stop() {
            return action == Action.STOP;
        }
    }
}
