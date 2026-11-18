package io.debezium.spi.schema;

import java.util.List;

/** Public identifier contract used by topic-naming plugins. */
public interface DataCollectionId {
    String identifier();

    List<String> parts();

    List<String> databaseParts();

    List<String> schemaParts();
}
