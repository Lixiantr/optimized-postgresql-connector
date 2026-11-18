package io.cdcbench.connect.optimized;

import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.ConnectorTransactionBoundaries;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class OptimizedPostgresConnectorTest {
    @Test
    void exposesThePinnedContractAndUsesOnlyTheNativeTask() {
        OptimizedPostgresConnector connector = new OptimizedPostgresConnector();

        assertEquals(OptimizedPostgresTask.class, connector.taskClass());
        assertEquals(125, connector.config().configKeys().size());
        assertNotNull(connector.config().configKeys().get("snapshot.mode"));
        assertNotNull(connector.config().configKeys().get("decimal.handling.mode"));
        assertNotNull(connector.config().configKeys().get("heartbeat.action.query"));
        assertEquals(ExactlyOnceSupport.SUPPORTED, connector.exactlyOnceSupport(Map.of()));
        assertEquals(ConnectorTransactionBoundaries.SUPPORTED,
                connector.canDefineTransactionBoundaries(Map.of()));
    }
}
