package io.cdcbench.connect.optimized;

import io.cdcbench.connect.common.PgOutputMessage.Column;
import org.apache.kafka.common.config.ConfigException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Debezium-compatible value-only column truncation, masking and salted hashing. */
final class ColumnValueTransformers {
    private static final Pattern TRUNCATE_PROPERTY = Pattern.compile(
            "column\\.truncate\\.to\\.(\\d+)\\.chars");
    private static final Pattern MASK_PROPERTY = Pattern.compile(
            "column\\.mask\\.with\\.(\\d+)\\.chars");
    private static final Pattern HASH_V1_PROPERTY = Pattern.compile(
            "column\\.mask\\.hash\\.([^.]+)\\.with\\.salt\\.(.+)");
    private static final Pattern HASH_V2_PROPERTY = Pattern.compile(
            "column\\.mask\\.hash\\.v2\\.([^.]+)\\.with\\.salt\\.(.+)");

    private final List<Rule> truncationRules;
    private final List<Rule> maskRules;
    private final List<Rule> hashV1Rules;
    private final List<Rule> hashV2Rules;

    ColumnValueTransformers(Map<String, String> properties) {
        List<Rule> truncations = new ArrayList<>();
        List<Rule> masks = new ArrayList<>();
        List<Rule> hashesV1 = new ArrayList<>();
        List<Rule> hashesV2 = new ArrayList<>();
        properties.forEach((key, value) -> {
            Matcher truncate = TRUNCATE_PROPERTY.matcher(key);
            Matcher mask = MASK_PROPERTY.matcher(key);
            Matcher hashV2 = HASH_V2_PROPERTY.matcher(key);
            Matcher hashV1 = HASH_V1_PROPERTY.matcher(key);
            if (truncate.matches()) {
                truncations.add(new Rule(patterns(key, value),
                        new Truncate(parseLength(key, truncate.group(1)))));
            } else if (mask.matches()) {
                masks.add(new Rule(patterns(key, value),
                        new FixedMask(parseLength(key, mask.group(1)))));
            } else if (hashV2.matches()) {
                hashesV2.add(new Rule(patterns(key, value),
                        hash(key, hashV2.group(1), hashV2.group(2), true)));
            } else if (hashV1.matches()) {
                hashesV1.add(new Rule(patterns(key, value),
                        hash(key, hashV1.group(1), hashV1.group(2), false)));
            }
        });
        truncationRules = List.copyOf(truncations);
        maskRules = List.copyOf(masks);
        hashV1Rules = List.copyOf(hashesV1);
        hashV2Rules = List.copyOf(hashesV2);
    }

    static boolean isProperty(String key) {
        return TRUNCATE_PROPERTY.matcher(key).matches()
                || MASK_PROPERTY.matcher(key).matches()
                || HASH_V1_PROPERTY.matcher(key).matches()
                || HASH_V2_PROPERTY.matcher(key).matches();
    }

    Binding binding(String schema, String table, Column column, long baseTypeOid) {
        if (!isCharacterType(baseTypeOid)) {
            return null;
        }
        String qualifiedName = schema + "." + table + "." + column.name();
        Transform transform = firstMatch(qualifiedName, truncationRules);
        if (transform == null) {
            transform = firstMatch(qualifiedName, maskRules);
        }
        if (transform == null) {
            transform = firstMatch(qualifiedName, hashV1Rules);
        }
        if (transform == null) {
            transform = firstMatch(qualifiedName, hashV2Rules);
        }
        if (transform == null) {
            return null;
        }
        return new Binding(transform, maximumLength(column, baseTypeOid));
    }

    private static Transform firstMatch(String qualifiedName, List<Rule> rules) {
        for (Rule rule : rules) {
            if (rule.matches(qualifiedName)) {
                return rule.transform();
            }
        }
        return null;
    }

    private static boolean isCharacterType(long typeOid) {
        return typeOid == 18L || typeOid == 19L || typeOid == 25L
                || typeOid == 1042L || typeOid == 1043L;
    }

    private static int maximumLength(Column column, long baseTypeOid) {
        if ((baseTypeOid == 1042L || baseTypeOid == 1043L)
                && column.typeModifier() >= 4) {
            return column.typeModifier() - 4;
        }
        return Integer.MAX_VALUE;
    }

    private static int parseLength(String key, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException error) {
            throw new ConfigException(key, value, "character count must fit a non-negative integer");
        }
    }

    private static Transform hash(String key, String algorithm, String salt, boolean versionTwo) {
        try {
            MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException error) {
            throw new ConfigException(key, algorithm, "unsupported MessageDigest algorithm");
        }
        return new SaltedHash(algorithm, salt, versionTwo);
    }

    private static List<Pattern> patterns(String key, String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        List<Pattern> patterns = new ArrayList<>();
        for (String expression : value.split(",")) {
            String normalized = expression.trim();
            if (normalized.isEmpty()) {
                continue;
            }
            try {
                patterns.add(Pattern.compile(normalized));
            } catch (PatternSyntaxException error) {
                throw new ConfigException(key, expression,
                        "must contain valid Java regular expressions");
            }
        }
        return List.copyOf(patterns);
    }

    record Binding(Transform transform, int maximumLength) {
        Object convert(Object value) {
            return transform.apply(value, maximumLength);
        }

        Map<String, String> schemaParameters() {
            return transform.schemaParameters();
        }
    }

    private record Rule(List<Pattern> patterns, Transform transform) {
        boolean matches(String qualifiedName) {
            return patterns.stream().anyMatch(pattern -> pattern.matcher(qualifiedName).matches());
        }
    }

    private interface Transform {
        Object apply(Object value, int maximumLength);

        Map<String, String> schemaParameters();
    }

    private record Truncate(int length) implements Transform {
        @Override
        public Object apply(Object value, int maximumLength) {
            if (value == null) {
                return null;
            }
            String text = text(value);
            return text.length() <= length ? text : text.substring(0, length);
        }

        @Override
        public Map<String, String> schemaParameters() {
            return Map.of("truncateLength", Integer.toString(length));
        }
    }

    private record FixedMask(int length) implements Transform {
        @Override
        public Object apply(Object value, int maximumLength) {
            return "*".repeat(length);
        }

        @Override
        public Map<String, String> schemaParameters() {
            return Map.of("masked", "true");
        }
    }

    private record SaltedHash(String algorithm, String salt, boolean versionTwo)
            implements Transform {
        @Override
        public Object apply(Object value, int maximumLength) {
            if (value == null) {
                return null;
            }
            byte[] saltBytes = salt.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = versionTwo
                    ? text(value).getBytes(StandardCharsets.UTF_8)
                    : legacySerialized(text(value));
            try {
                MessageDigest digest = MessageDigest.getInstance(algorithm);
                digest.update(saltBytes);
                String pseudonym = HexFormat.of().formatHex(digest.digest(valueBytes));
                return pseudonym.length() <= maximumLength
                        ? pseudonym : pseudonym.substring(0, maximumLength);
            } catch (NoSuchAlgorithmException error) {
                throw new IllegalStateException("Validated hash algorithm disappeared", error);
            }
        }

        @Override
        public Map<String, String> schemaParameters() {
            return Map.of("masked", "true");
        }
    }

    private static String text(Object value) {
        if (value instanceof RawUtf8String raw) {
            return new String(raw.bytes(), StandardCharsets.UTF_8);
        }
        return value.toString();
    }

    private static byte[] legacySerialized(String value) {
        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream();
             ObjectOutputStream output = new ObjectOutputStream(buffer)) {
            output.writeObject(value);
            output.flush();
            return buffer.toByteArray();
        } catch (IOException error) {
            throw new IllegalStateException("Cannot serialize a String for legacy hash masking", error);
        }
    }
}
