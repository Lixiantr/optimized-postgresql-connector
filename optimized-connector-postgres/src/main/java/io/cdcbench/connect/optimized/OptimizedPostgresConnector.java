package io.cdcbench.connect.optimized;

import io.cdcbench.connect.common.CandidateConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.ConnectorTransactionBoundaries;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class OptimizedPostgresConnector extends SourceConnector {
    private Map<String, String> properties;

    @Override
    public void start(Map<String, String> properties) {
        CandidateConfig config = new CandidateConfig(properties);
        config.validateSupportedConfiguration();
        FastPathSelector.select(properties);
        this.properties = Map.copyOf(properties);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return OptimizedPostgresTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (properties == null || maxTasks < 1) {
            return List.of();
        }
        List<Map<String, String>> configurations = new ArrayList<>(1);
        configurations.add(new HashMap<>(properties));
        return configurations;
    }

    @Override
    public void stop() {
        properties = null;
    }

    @Override
    public ConfigDef config() {
        return PostgresConfigSurface.configDef();
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> connectorConfig) {
        return ExactlyOnceSupport.SUPPORTED;
    }

    @Override
    public ConnectorTransactionBoundaries canDefineTransactionBoundaries(
            Map<String, String> connectorConfig) {
        return ConnectorTransactionBoundaries.SUPPORTED;
    }

    @Override
    public String version() {
        return Version.VALUE;
    }
}
