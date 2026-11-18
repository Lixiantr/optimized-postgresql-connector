package io.debezium.relational;

import io.debezium.spi.schema.DataCollectionId;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Minimal immutable identifier used by the public topic-naming SPI. */
public final class TableId implements DataCollectionId, Comparable<TableId> {
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final String id;

    public TableId(String catalogName, String schemaName, String tableName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.id = join(catalogName, schemaName, tableName);
    }

    public String catalog() {
        return catalogName;
    }

    public String schema() {
        return schemaName;
    }

    public String table() {
        return tableName;
    }

    @Override
    public String identifier() {
        return id;
    }

    @Override
    public List<String> parts() {
        return Arrays.asList(catalogName, schemaName, tableName);
    }

    @Override
    public List<String> databaseParts() {
        return Arrays.asList(catalogName, tableName);
    }

    @Override
    public List<String> schemaParts() {
        return Arrays.asList(schemaName, tableName);
    }

    @Override
    public int compareTo(TableId other) {
        return id.compareTo(other.id);
    }

    @Override
    public boolean equals(Object value) {
        return value instanceof TableId other
                && Objects.equals(catalogName, other.catalogName)
                && Objects.equals(schemaName, other.schemaName)
                && tableName.equals(other.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, schemaName, tableName);
    }

    @Override
    public String toString() {
        return id;
    }

    private static String join(String... parts) {
        StringBuilder result = new StringBuilder();
        for (String part : parts) {
            if (part == null || part.isEmpty()) {
                continue;
            }
            if (!result.isEmpty()) {
                result.append('.');
            }
            result.append(part);
        }
        return result.toString();
    }
}
