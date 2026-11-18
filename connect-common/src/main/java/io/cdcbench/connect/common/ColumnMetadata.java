package io.cdcbench.connect.common;

public record ColumnMetadata(
        boolean nullable,
        String defaultExpression,
        boolean key,
        String typeName,
        boolean array,
        long elementTypeOid,
        String elementTypeName) {}
