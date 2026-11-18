package io.cdcbench.connect.optimized;

import org.apache.kafka.common.config.ConfigException;

import java.util.Objects;

/** A reversible PostgreSQL schema/table identifier, including quoted dots. */
record QualifiedTable(String schema, String table) implements Comparable<QualifiedTable> {
    QualifiedTable {
        requireIdentifier(schema, "schema");
        requireIdentifier(table, "table");
    }

    static QualifiedTable of(String schema, String table) {
        return new QualifiedTable(schema, table);
    }

    static QualifiedTable parse(String value, String property) {
        try {
            return parse(value);
        } catch (IllegalArgumentException error) {
            throw new ConfigException(property, value,
                    "must contain one exact schema.table name; quote identifiers "
                            + "containing dots with PostgreSQL double quotes");
        }
    }

    static QualifiedTable parse(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Qualified table is null");
        }
        Parser parser = new Parser(value.trim());
        String schema = parser.identifier();
        parser.separator();
        String table = parser.identifier();
        parser.end();
        return new QualifiedTable(schema, table);
    }

    String filterName() {
        return schema + "." + table;
    }

    String canonicalName() {
        return canonicalPart(schema) + "." + canonicalPart(table);
    }

    String sqlName() {
        return quote(schema) + "." + quote(table);
    }

    boolean matches(String namespace, String name) {
        return schema.equals(namespace) && table.equals(name);
    }

    @Override
    public int compareTo(QualifiedTable other) {
        int schemaOrder = schema.compareTo(other.schema);
        return schemaOrder == 0 ? table.compareTo(other.table) : schemaOrder;
    }

    @Override
    public String toString() {
        return canonicalName();
    }

    private static void requireIdentifier(String value, String kind) {
        Objects.requireNonNull(value, kind);
        if (value.isEmpty() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("Invalid PostgreSQL " + kind + " identifier");
        }
    }

    private static String canonicalPart(String identifier) {
        if (identifier.matches("[a-z_][a-z0-9_$]*")) {
            return identifier;
        }
        return quote(identifier);
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static final class Parser {
        private final String value;
        private int index;

        private Parser(String value) {
            this.value = value;
        }

        String identifier() {
            if (index >= value.length()) {
                throw malformed();
            }
            if (value.charAt(index) == '"') {
                return quotedIdentifier();
            }
            int start = index;
            char first = value.charAt(index);
            if (!(first == '_' || Character.isLetter(first))) {
                throw malformed();
            }
            index++;
            while (index < value.length()) {
                char character = value.charAt(index);
                if (!(character == '_' || character == '$'
                        || Character.isLetterOrDigit(character))) {
                    break;
                }
                index++;
            }
            return value.substring(start, index);
        }

        private String quotedIdentifier() {
            index++;
            StringBuilder result = new StringBuilder();
            while (index < value.length()) {
                char character = value.charAt(index++);
                if (character != '"') {
                    if (character == '\0') {
                        throw malformed();
                    }
                    result.append(character);
                    continue;
                }
                if (index < value.length() && value.charAt(index) == '"') {
                    result.append('"');
                    index++;
                    continue;
                }
                if (result.isEmpty()) {
                    throw malformed();
                }
                return result.toString();
            }
            throw malformed();
        }

        void separator() {
            if (index >= value.length() || value.charAt(index++) != '.') {
                throw malformed();
            }
        }

        void end() {
            if (index != value.length()) {
                throw malformed();
            }
        }

        private IllegalArgumentException malformed() {
            return new IllegalArgumentException("Malformed qualified table: " + value);
        }
    }
}
