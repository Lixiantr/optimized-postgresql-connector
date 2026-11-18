package io.debezium.spi.converter;

import java.util.OptionalInt;

/** PostgreSQL relational field contract exposed to external custom converters. */
public interface RelationalColumn extends ConvertedField {
    int jdbcType();

    int nativeType();

    String typeName();

    String typeExpression();

    OptionalInt length();

    OptionalInt scale();

    boolean isOptional();

    Object defaultValue();

    boolean hasDefaultValue();

    default String charsetName() {
        return null;
    }
}
