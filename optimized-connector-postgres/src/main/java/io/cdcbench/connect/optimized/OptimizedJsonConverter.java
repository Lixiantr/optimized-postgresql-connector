package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

/**
 * Preserves the Kafka Connect JSON contract while allowing the native fast path
 * to hand the worker JSON that was already serialized by an official
 * {@link JsonConverter} in a materializer thread.
 */
public final class OptimizedJsonConverter implements Converter {
    private final JsonConverter delegate = new JsonConverter();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(configs, isKey);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema == null && value instanceof byte[] serializedJson) {
            return serializedJson;
        }
        return delegate.fromConnectData(topic, schema, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return delegate.toConnectData(topic, value);
    }

    @Override
    public ConfigDef config() {
        return delegate.config();
    }

    @Override
    public String version() {
        return delegate.version();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
