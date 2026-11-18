package io.cdcbench.connect.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.cdcbench.connect.common.PgOutputMessage.Column;
import static io.cdcbench.connect.common.PgOutputMessage.TupleValue;
import static io.cdcbench.connect.common.PgOutputMessage.ValueKind;

import org.postgresql.util.HStoreConverter;

public final class PostgresValueConverter {
    private static final Pattern SIMPLE_STRING_DEFAULT =
            Pattern.compile("^'((?:''|[^'])*)'(?:::[A-Za-z0-9_ .\"\\[\\]]+)?$");
    private static final DateTimeFormatter DEBEZIUM_ZONED_TIMESTAMP =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DEBEZIUM_ZONED_TIME =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS'Z'");
    private static final LocalDate POSTGRES_DATE_EPOCH = LocalDate.of(1970, 1, 1);
    private static final Pattern INTERVAL_PART =
            Pattern.compile("([+-]?\\d+)\\s+(years?|mons?|days?)");
    private static final Pattern INTERVAL_TIME =
            Pattern.compile("([+-]?)(\\d+):(\\d{2}):(\\d{2}(?:\\.\\d+)?)");
    private static final Pattern POINT =
            Pattern.compile("^\\(([^,]+),([^,]+)\\)$");
    private static final BigDecimal MONTH_MICROS = new BigDecimal("2629800000000");
    private static final BigDecimal DAY_MICROS = new BigDecimal("86400000000");
    private static final int DATE_POSITIVE_INFINITY = -2_147_472_692;
    private static final int DATE_NEGATIVE_INFINITY = -2_147_472_691;
    private static final int ARRAY_DATE_POSITIVE_INFINITY = -622_191_234;
    private static final int ARRAY_DATE_NEGATIVE_INFINITY = -625_821_272;
    private static final long TIMESTAMP_POSITIVE_INFINITY = 9_223_372_036_825_200_000L;
    private static final long TIMESTAMP_NEGATIVE_INFINITY = -9_223_372_036_832_400_000L;
    private static final String ISO_DATE_POSITIVE_INFINITY = "-5877611-06-21Z";
    private static final String ISO_DATE_NEGATIVE_INFINITY = "-5877611-06-22Z";
    private static final String ISO_ARRAY_DATE_POSITIVE_INFINITY = "+292278994-08-16Z";
    private static final String ISO_ARRAY_DATE_NEGATIVE_INFINITY = "+292269055-12-02Z";
    private static final String ISO_TIMESTAMP_POSITIVE_INFINITY =
            "+294247-01-10T04:00:25.2Z";
    private static final String ISO_TIMESTAMP_NEGATIVE_INFINITY =
            "-290308-12-21T19:59:27.6Z";
    private static final String ARRAY_ZONED_TIMESTAMP_NEGATIVE_INFINITY =
            "+292269055-12-02T23:00:00.000000Z";

    private final String decimalMode;
    private final String binaryMode;
    private final String hstoreMode;
    private final String intervalMode;
    private final String timePrecisionMode;

    public PostgresValueConverter() {
        this("precise", "bytes", "json", "numeric", "adaptive");
    }

    public PostgresValueConverter(
            String decimalMode, String binaryMode, String hstoreMode, String intervalMode) {
        this(decimalMode, binaryMode, hstoreMode, intervalMode, "adaptive");
    }

    public PostgresValueConverter(
            String decimalMode, String binaryMode, String hstoreMode, String intervalMode,
            String timePrecisionMode) {
        this.decimalMode = decimalMode;
        this.binaryMode = binaryMode;
        this.hstoreMode = hstoreMode;
        this.intervalMode = intervalMode;
        this.timePrecisionMode = timePrecisionMode;
    }

    public Object convert(Column column, TupleValue value) {
        return convert(column, value, unknownMetadata(column));
    }

    public Object convert(Column column, TupleValue value, ColumnMetadata metadata) {
        if (value.kind() == ValueKind.NULL) {
            return null;
        }
        if (value.kind() == ValueKind.UNCHANGED_TOAST) {
            return UnchangedToast.INSTANCE;
        }
        if (value.kind() == ValueKind.BINARY) {
            return column.typeOid() == 17L
                    ? binaryValue(value.data()) : value.data().clone();
        }

        String text = new String(value.data(), StandardCharsets.UTF_8);
        return convertText(column, metadata, text);
    }

    /** Returns Debezium's byte representation for a PostgreSQL type without a mapping. */
    public Object convertUnknown(TupleValue value, ColumnMetadata metadata) {
        if (value.kind() == ValueKind.NULL) {
            return null;
        }
        if (value.kind() == ValueKind.UNCHANGED_TOAST) {
            return UnchangedToast.INSTANCE;
        }
        if (value.kind() == ValueKind.BINARY) {
            return value.data().clone();
        }
        if (!metadata.array()) {
            return value.data().clone();
        }
        String text = new String(value.data(), StandardCharsets.UTF_8);
        return new ArrayTextParser(text).parse(
                item -> item.getBytes(StandardCharsets.UTF_8));
    }

    public Object defaultValue(Column column, boolean nullable, String defaultExpression) {
        return defaultValue(column, new ColumnMetadata(
                nullable, defaultExpression, column.key(), null, false, 0L, null));
    }

    public Object defaultValue(Column column, ColumnMetadata metadata) {
        if (metadata.nullable()) {
            return null;
        }

        String literal = simpleLiteral(metadata.defaultExpression());
        if (literal != null) {
            try {
                return convertText(column, metadata, literal);
            } catch (RuntimeException ignored) {
                // Debezium falls back to the Connect type default when PostgreSQL's
                // expression cannot be represented as a schema default.
            }
        }

        return switch ((int) column.typeOid()) {
            case 16 -> false;
            case 20 -> 0L;
            case 21 -> (short) 0;
            case 23 -> 0;
            case 26 -> 0L;
            case 700 -> 0.0f;
            case 701 -> 0.0d;
            case 1700 -> decimalValue(column, BigDecimal.ZERO);
            case 1082 -> "isostring".equals(timePrecisionMode) ? "1970-01-01Z" : 0;
            case 1083 -> timeDefault(column);
            case 1114 -> "isostring".equals(timePrecisionMode)
                    ? "1970-01-01T00:00:00Z" : 0L;
            case 1184 -> "1970-01-01T00:00:00.000000Z";
            default -> "";
        };
    }

    private Object convertText(Column column, ColumnMetadata metadata, String text) {
        return convertText(column, metadata, text, false);
    }

    private Object convertText(
            Column column, ColumnMetadata metadata, String text, boolean arrayElement) {
        if (metadata.array()) {
            Column element = new Column(false, column.name() + "[]",
                    metadata.elementTypeOid(), column.typeModifier());
            ColumnMetadata elementMetadata = new ColumnMetadata(
                    true, null, false, metadata.elementTypeName(), false, 0L, null);
            return new ArrayTextParser(text).parse(
                    item -> convertText(element, elementMetadata, item, true));
        }
        if ("hstore".equals(metadata.typeName())) {
            return hstoreValue(text);
        }
        return switch ((int) column.typeOid()) {
            case 16 -> switch (text) {
                case "t", "true", "1" -> true;
                case "f", "false", "0" -> false;
                default -> throw new IllegalArgumentException("Invalid PostgreSQL boolean: " + text);
            };
            case 17 -> binaryValue(decodeBytea(text));
            case 20 -> Long.parseLong(text);
            case 21 -> Short.parseShort(text);
            case 23 -> Integer.parseInt(text);
            case 26 -> Long.parseUnsignedLong(text);
            case 700 -> Float.parseFloat(text);
            case 701 -> Double.parseDouble(text);
            case 790 -> moneyValue(moneyDecimal(text));
            case 1700 -> decimalValue(column, new BigDecimal(text));
            case 1082 -> dateValue(text, arrayElement);
            case 1083 -> timeValue(column, text, arrayElement);
            case 1114 -> timestampValue(column, text);
            case 1184 -> zonedTimestamp(text, arrayElement);
            case 1186 -> "string".equals(intervalMode)
                    ? intervalIso(text) : intervalMicros(text);
            case 1266 -> zonedTime(text);
            case 1560, 1562 -> bitValue(column, text);
            case 600 -> point(text);
            default -> text;
        };
    }

    private static ColumnMetadata unknownMetadata(Column column) {
        return new ColumnMetadata(true, null, column.key(), null, false, 0L, null);
    }

    private Object decimalValue(Column column, BigDecimal decimal) {
        if ("string".equals(decimalMode)) {
            return decimal.toPlainString();
        }
        if ("double".equals(decimalMode)) {
            return decimal.doubleValue();
        }
        int typeModifier = column.typeModifier();
        if (typeModifier < 0) {
            Map<String, Object> variable = new LinkedHashMap<>();
            variable.put("scale", decimal.scale());
            variable.put("value", decimal.unscaledValue().toByteArray());
            return variable;
        }
        int scale = (typeModifier - 4) & 0x7ff;
        return decimal.setScale(scale).unscaledValue().toByteArray();
    }

    private Object moneyValue(BigDecimal decimal) {
        if ("string".equals(decimalMode)) {
            return decimal.toPlainString();
        }
        if ("double".equals(decimalMode)) {
            return decimal.doubleValue();
        }
        return decimal.unscaledValue().toByteArray();
    }

    private Object dateValue(String text, boolean arrayElement) {
        if (text.equals("infinity")) {
            if ("isostring".equals(timePrecisionMode)) {
                return arrayElement
                        ? ISO_ARRAY_DATE_POSITIVE_INFINITY : ISO_DATE_POSITIVE_INFINITY;
            }
            return arrayElement ? ARRAY_DATE_POSITIVE_INFINITY : DATE_POSITIVE_INFINITY;
        }
        if (text.equals("-infinity")) {
            if ("isostring".equals(timePrecisionMode)) {
                return arrayElement
                        ? ISO_ARRAY_DATE_NEGATIVE_INFINITY : ISO_DATE_NEGATIVE_INFINITY;
            }
            return arrayElement ? ARRAY_DATE_NEGATIVE_INFINITY : DATE_NEGATIVE_INFINITY;
        }
        if ("isostring".equals(timePrecisionMode)) {
            return LocalDate.parse(text) + "Z";
        }
        return date(text);
    }

    private Object timeValue(Column column, String text, boolean arrayElement) {
        LocalTime time = LocalTime.parse(text);
        if (arrayElement) {
            time = time.truncatedTo(ChronoUnit.MILLIS);
        }
        if ("isostring".equals(timePrecisionMode)) {
            return isoTime(time);
        }
        long micros = time.toNanoOfDay() / 1_000L;
        if ("adaptive".equals(timePrecisionMode)) {
            if (temporalPrecision(column) <= 3) {
                return Math.toIntExact(micros / 1_000L);
            }
            return micros;
        }
        return switch (timePrecisionMode) {
            case "connect" -> Math.toIntExact(micros / 1_000L);
            case "nanoseconds" -> Math.multiplyExact(micros, 1_000L);
            default -> micros;
        };
    }

    private Object timestampValue(Column column, String text) {
        if ("isostring".equals(timePrecisionMode)) {
            return switch (text) {
                case "infinity" -> ISO_TIMESTAMP_POSITIVE_INFINITY;
                case "-infinity" -> ISO_TIMESTAMP_NEGATIVE_INFINITY;
                default -> text.replace(' ', 'T') + "Z";
            };
        }
        if ("nanoseconds".equals(timePrecisionMode)) {
            if (text.equals("infinity")) {
                return Long.MAX_VALUE;
            }
            if (text.equals("-infinity")) {
                return Long.MIN_VALUE;
            }
        }
        long micros = localDateTimeMicros(text);
        return switch (timePrecisionMode) {
            case "connect" -> Math.floorDiv(micros, 1_000L);
            case "nanoseconds" -> Math.multiplyExact(micros, 1_000L);
            case "adaptive", "adaptive_time_microseconds" ->
                    temporalPrecision(column) <= 3 ? micros / 1_000L : micros;
            default -> micros;
        };
    }

    private Object timeDefault(Column column) {
        if ("isostring".equals(timePrecisionMode)) {
            return "00:00:00Z";
        }
        if ("connect".equals(timePrecisionMode)
                || "adaptive".equals(timePrecisionMode) && temporalPrecision(column) <= 3) {
            return 0;
        }
        return 0L;
    }

    private static int temporalPrecision(Column column) {
        return column.typeModifier() < 0 ? 6 : column.typeModifier();
    }

    private static String isoTime(LocalTime time) {
        String value = time.toString();
        return (value.length() == 5 ? value + ":00" : value) + "Z";
    }

    private static byte[] decodeBytea(String text) {
        if (text.startsWith("\\x")) {
            return HexFormat.of().parseHex(text.substring(2));
        }
        return text.getBytes(StandardCharsets.ISO_8859_1);
    }

    private Object binaryValue(byte[] value) {
        return switch (binaryMode) {
            case "base64" -> Base64.getEncoder().encodeToString(value);
            case "base64-url-safe" -> Base64.getUrlEncoder().encodeToString(value);
            case "hex" -> HexFormat.of().formatHex(value);
            default -> value.clone();
        };
    }

    private static Object bitValue(Column column, String text) {
        if (column.typeOid() == 1560L && column.typeModifier() == 1) {
            return switch (text) {
                case "1" -> true;
                case "0" -> false;
                default -> throw new IllegalArgumentException("Invalid PostgreSQL bit(1): " + text);
            };
        }
        if (!text.matches("[01]+")) {
            throw new IllegalArgumentException("Invalid PostgreSQL bit string: " + text);
        }
        byte[] bigEndian = new BigInteger(text, 2).toByteArray();
        int first = bigEndian.length > 1 && bigEndian[0] == 0 ? 1 : 0;
        byte[] littleEndian = new byte[bigEndian.length - first];
        for (int index = first; index < bigEndian.length; index++) {
            littleEndian[bigEndian.length - 1 - index] = bigEndian[index];
        }
        return littleEndian;
    }

    private static BigDecimal moneyDecimal(String text) {
        boolean negative = text.indexOf('-') >= 0
                || text.strip().startsWith("(") && text.strip().endsWith(")");
        String digits = text.replaceAll("[^0-9.,]", "");
        int decimal = Math.max(digits.lastIndexOf('.'), digits.lastIndexOf(','));
        String normalized;
        if (decimal >= 0 && digits.length() - decimal - 1 == 2) {
            normalized = digits.substring(0, decimal).replaceAll("[.,]", "")
                    + "." + digits.substring(decimal + 1);
        } else {
            normalized = digits.replaceAll("[.,]", "");
        }
        return new BigDecimal((negative ? "-" : "") + normalized).setScale(2);
    }

    private static long intervalMicros(String text) {
        BigDecimal total = BigDecimal.ZERO;
        Matcher parts = INTERVAL_PART.matcher(text);
        while (parts.find()) {
            long amount = Long.parseLong(parts.group(1));
            String unit = parts.group(2);
            if (unit.startsWith("year")) {
                total = total.add(MONTH_MICROS.multiply(BigDecimal.valueOf(Math.multiplyExact(amount, 12L))));
            } else if (unit.startsWith("mon")) {
                total = total.add(MONTH_MICROS.multiply(BigDecimal.valueOf(amount)));
            } else {
                total = total.add(DAY_MICROS.multiply(BigDecimal.valueOf(amount)));
            }
        }
        Matcher time = INTERVAL_TIME.matcher(text);
        if (time.find()) {
            int sign = "-".equals(time.group(1)) ? -1 : 1;
            BigDecimal seconds = BigDecimal.valueOf(Long.parseLong(time.group(2)))
                    .multiply(BigDecimal.valueOf(3600L))
                    .add(BigDecimal.valueOf(Long.parseLong(time.group(3))).multiply(BigDecimal.valueOf(60L)))
                    .add(new BigDecimal(time.group(4)))
                    .movePointRight(6)
                    .multiply(BigDecimal.valueOf(sign));
            total = total.add(seconds);
        }
        return total.longValueExact();
    }

    private static String intervalIso(String text) {
        long years = 0L;
        long months = 0L;
        long days = 0L;
        Matcher parts = INTERVAL_PART.matcher(text);
        while (parts.find()) {
            long amount = Long.parseLong(parts.group(1));
            String unit = parts.group(2);
            if (unit.startsWith("year")) {
                years = Math.addExact(years, amount);
            } else if (unit.startsWith("mon")) {
                months = Math.addExact(months, amount);
            } else {
                days = Math.addExact(days, amount);
            }
        }

        long hours = 0L;
        long minutes = 0L;
        BigDecimal seconds = BigDecimal.ZERO;
        Matcher time = INTERVAL_TIME.matcher(text);
        if (time.find()) {
            int sign = "-".equals(time.group(1)) ? -1 : 1;
            hours = Math.multiplyExact(Long.parseLong(time.group(2)), sign);
            minutes = Math.multiplyExact(Long.parseLong(time.group(3)), sign);
            seconds = new BigDecimal(time.group(4)).multiply(BigDecimal.valueOf(sign));
        }

        return new StringBuilder("P")
                .append(years).append('Y')
                .append(months).append('M')
                .append(days).append("DT")
                .append(hours).append('H')
                .append(minutes).append('M')
                .append(seconds.stripTrailingZeros().toPlainString()).append('S')
                .toString();
    }

    private static String zonedTime(String text) {
        String normalized = text.trim();
        if (normalized.matches(".*[+-]\\d{2}$")) {
            normalized += ":00";
        }
        try {
            OffsetTime time = OffsetTime.parse(normalized).withOffsetSameInstant(ZoneOffset.UTC);
            return DEBEZIUM_ZONED_TIME.format(time);
        } catch (DateTimeParseException error) {
            throw new IllegalArgumentException("Invalid PostgreSQL timetz: " + text, error);
        }
    }

    private static Map<String, Object> point(String text) {
        Matcher matcher = POINT.matcher(text);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid PostgreSQL point: " + text);
        }
        double x = Double.parseDouble(matcher.group(1));
        double y = Double.parseDouble(matcher.group(2));
        byte[] wkb = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN)
                .put((byte) 1)
                .putInt(1)
                .putDouble(x)
                .putDouble(y)
                .array();
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("wkb", wkb);
        value.put("srid", null);
        value.put("x", x);
        value.put("y", y);
        return value;
    }

    private Object hstoreValue(String text) {
        Map<String, String> parsed = new LinkedHashMap<>(HStoreConverter.fromString(text));
        if ("map".equals(hstoreMode)) {
            return parsed;
        }
        return hstoreJson(parsed);
    }

    private static String hstoreJson(Map<String, String> parsed) {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : parsed.entrySet()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            appendJsonString(json, entry.getKey());
            json.append(':');
            if (entry.getValue() == null) {
                json.append("null");
            } else {
                appendJsonString(json, entry.getValue());
            }
        }
        return json.append('}').toString();
    }

    private static void appendJsonString(StringBuilder target, String value) {
        target.append('"');
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"' -> target.append("\\\"");
                case '\\' -> target.append("\\\\");
                case '\b' -> target.append("\\b");
                case '\f' -> target.append("\\f");
                case '\n' -> target.append("\\n");
                case '\r' -> target.append("\\r");
                case '\t' -> target.append("\\t");
                default -> {
                    if (character < 0x20) {
                        target.append(String.format("\\u%04x", (int) character));
                    } else {
                        target.append(character);
                    }
                }
            }
        }
        target.append('"');
    }

    private static final class ArrayTextParser {
        private final String input;
        private int index;

        private ArrayTextParser(String input) {
            this.input = input;
        }

        private List<Object> parse(Function<String, Object> converter) {
            if (input.startsWith("[")) {
                int assignment = input.indexOf('=');
                if (assignment < 0) {
                    throw malformed();
                }
                index = assignment + 1;
            }
            List<Object> result = array(converter);
            if (index != input.length()) {
                throw malformed();
            }
            return result;
        }

        private List<Object> array(Function<String, Object> converter) {
            require('{');
            List<Object> values = new ArrayList<>();
            if (peek('}')) {
                index++;
                return values;
            }
            while (true) {
                if (peek('{')) {
                    // Debezium PostgreSQL 3.6 emits null for nested arrays in
                    // schemaless JSON. Still parse the nested value strictly.
                    array(converter);
                    values.add(null);
                } else {
                    values.add(element(converter));
                }
                if (peek(',')) {
                    index++;
                    continue;
                }
                require('}');
                return values;
            }
        }

        private Object element(Function<String, Object> converter) {
            if (peek('"')) {
                return converter.apply(quoted());
            }
            int start = index;
            while (index < input.length() && input.charAt(index) != ',' && input.charAt(index) != '}') {
                if (input.charAt(index) == '\\') {
                    index++;
                    if (index >= input.length()) {
                        throw malformed();
                    }
                }
                index++;
            }
            String value = input.substring(start, index).trim();
            if (value.equals("NULL")) {
                return null;
            }
            return converter.apply(unescape(value));
        }

        private String quoted() {
            require('"');
            StringBuilder value = new StringBuilder();
            while (index < input.length()) {
                char character = input.charAt(index++);
                if (character == '"') {
                    return value.toString();
                }
                if (character == '\\') {
                    if (index >= input.length()) {
                        throw malformed();
                    }
                    character = input.charAt(index++);
                }
                value.append(character);
            }
            throw malformed();
        }

        private static String unescape(String value) {
            StringBuilder result = new StringBuilder(value.length());
            boolean escaped = false;
            for (int index = 0; index < value.length(); index++) {
                char character = value.charAt(index);
                if (escaped) {
                    result.append(character);
                    escaped = false;
                } else if (character == '\\') {
                    escaped = true;
                } else {
                    result.append(character);
                }
            }
            if (escaped) {
                throw new IllegalArgumentException("Malformed PostgreSQL array: " + value);
            }
            return result.toString();
        }

        private boolean peek(char expected) {
            return index < input.length() && input.charAt(index) == expected;
        }

        private void require(char expected) {
            if (!peek(expected)) {
                throw malformed();
            }
            index++;
        }

        private IllegalArgumentException malformed() {
            return new IllegalArgumentException(
                    "Malformed PostgreSQL array at byte " + index + ": " + input);
        }
    }

    private static long localDateTimeMicros(String text) {
        if (text.equals("infinity")) {
            return TIMESTAMP_POSITIVE_INFINITY;
        }
        if (text.equals("-infinity")) {
            return TIMESTAMP_NEGATIVE_INFINITY;
        }
        LocalDateTime dateTime = LocalDateTime.parse(text.replace(' ', 'T'));
        Instant instant = dateTime.toInstant(ZoneOffset.UTC);
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L),
                instant.getNano() / 1_000L);
    }

    private static String zonedTimestamp(String text, boolean arrayElement) {
        if (arrayElement && text.equals("-infinity")) {
            return ARRAY_ZONED_TIMESTAMP_NEGATIVE_INFINITY;
        }
        if (text.equals("infinity") || text.equals("-infinity")) {
            return text;
        }
        String normalized = text.trim().replace(' ', 'T');
        if (normalized.matches(".*[+-]\\d{2}$")) {
            normalized += ":00";
        }
        try {
            Instant instant = OffsetDateTime.parse(normalized).toInstant();
            long micros = Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L),
                    instant.getNano() / 1_000L);
            Instant truncated = Instant.ofEpochSecond(
                    Math.floorDiv(micros, 1_000_000L),
                    Math.floorMod(micros, 1_000_000L) * 1_000L);
            return DEBEZIUM_ZONED_TIMESTAMP.format(truncated);
        } catch (DateTimeParseException error) {
            throw new IllegalArgumentException("Invalid PostgreSQL timestamptz: " + text, error);
        }
    }

    private static int date(String text) {
        return switch (text) {
            case "infinity" -> DATE_POSITIVE_INFINITY;
            case "-infinity" -> DATE_NEGATIVE_INFINITY;
            default -> Math.toIntExact(
                    ChronoUnit.DAYS.between(POSTGRES_DATE_EPOCH, LocalDate.parse(text)));
        };
    }

    private static String simpleLiteral(String expression) {
        if (expression == null) {
            return null;
        }
        Matcher matcher = SIMPLE_STRING_DEFAULT.matcher(expression.trim());
        return matcher.matches() ? matcher.group(1).replace("''", "'") : null;
    }

    public enum UnchangedToast {
        INSTANCE
    }
}
