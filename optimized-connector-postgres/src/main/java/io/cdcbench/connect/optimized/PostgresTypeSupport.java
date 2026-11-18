package io.cdcbench.connect.optimized;

import java.util.Locale;
import java.util.Set;

/** Version-pinned classification of PostgreSQL types handled by Debezium 3.6. */
final class PostgresTypeSupport {
    private static final Set<String> KNOWN_TYPES = Set.of(
            "bool", "bytea", "char", "name", "int2", "int4", "int8", "oid",
            "float4", "float8", "numeric", "money",
            "bpchar", "varchar", "text", "citext",
            "date", "time", "timetz", "timestamp", "timestamptz", "interval",
            "uuid", "json", "jsonb", "xml",
            "inet", "cidr", "macaddr", "macaddr8",
            "bit", "varbit", "point",
            "int4range", "int8range", "numrange", "tsrange", "tstzrange",
            "daterange", "int4multirange", "int8multirange", "nummultirange",
            "tsmultirange", "tstzmultirange", "datemultirange",
            "hstore", "ltree", "geometry", "geography", "tsvector");

    private PostgresTypeSupport() {
    }

    static boolean isSupported(
            String typeName, String typeKind,
            boolean array, String elementTypeName, String elementTypeKind,
            String baseTypeName, String baseTypeKind) {
        if (array) {
            return scalarSupported(elementTypeName, elementTypeKind, null, null);
        }
        return scalarSupported(typeName, typeKind, baseTypeName, baseTypeKind);
    }

    private static boolean scalarSupported(
            String typeName, String typeKind, String baseTypeName, String baseTypeKind) {
        if ("e".equals(typeKind)) {
            return true;
        }
        if ("d".equals(typeKind) && baseTypeName != null) {
            return scalarSupported(baseTypeName, baseTypeKind, null, null);
        }
        return typeName != null
                && KNOWN_TYPES.contains(typeName.toLowerCase(Locale.ROOT));
    }
}
