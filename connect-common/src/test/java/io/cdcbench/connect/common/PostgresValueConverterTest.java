package io.cdcbench.connect.common;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PostgresValueConverterTest {
    private final PostgresValueConverter converter = new PostgresValueConverter();

    @Test
    void matchesDebeziumZonedTimestampFormatting() {
        PgOutputMessage.Column column = new PgOutputMessage.Column(false, "source_ts", 1184, -1);
        Object value = converter.convert(column, text("2026-08-01 09:16:02.189777+00"));
        assertEquals("2026-08-01T09:16:02.189777Z", value);
    }

    @Test
    void usesLiteralAndTypeDefaultsForPartialDeleteTuple() {
        PgOutputMessage.Column eventType = new PgOutputMessage.Column(false, "event_type", 25, -1);
        PgOutputMessage.Column timestamp = new PgOutputMessage.Column(false, "source_ts", 1184, -1);
        PgOutputMessage.Column payload = new PgOutputMessage.Column(false, "payload", 3802, -1);

        assertEquals("insert", converter.defaultValue(eventType, false, "'insert'::text"));
        assertEquals("1970-01-01T00:00:00.000000Z",
                converter.defaultValue(timestamp, false, "clock_timestamp()"));
        assertEquals("", converter.defaultValue(payload, false, null));
    }

    @Test
    void encodesFixedAndVariableScaleNumericsLikeDebeziumDecimalTypes() {
        PgOutputMessage.Column fixed = new PgOutputMessage.Column(
                false, "amount", 1700, 4 + (20 << 16) + 6);
        PgOutputMessage.Column variable = new PgOutputMessage.Column(false, "measurement", 1700, -1);

        assertArrayEquals(new BigInteger("123456789012345678").toByteArray(),
                (byte[]) converter.convert(fixed, text("123456789012.345678")));
        assertArrayEquals(new byte[]{-1},
                (byte[]) converter.convert(fixed, text("-0.000001")));
        Map<?, ?> variableValue = (Map<?, ?>) converter.convert(variable, text("1.2345"));
        assertEquals(4, variableValue.get("scale"));
        assertArrayEquals(new BigInteger("12345").toByteArray(), (byte[]) variableValue.get("value"));
    }

    @Test
    void matchesDebeziumSpecialScalarRepresentations() {
        assertEquals(true, converter.convert(
                new PgOutputMessage.Column(false, "flag", 1560, 1), text("1")));
        assertArrayEquals(new byte[]{0x55, 0x01}, (byte[]) converter.convert(
                new PgOutputMessage.Column(false, "bits", 1560, 9), text("101010101")));
        assertArrayEquals(new byte[]{0x0b}, (byte[]) converter.convert(
                new PgOutputMessage.Column(false, "bits", 1562, 6), text("001011")));
        assertEquals(37_091_106_123_456L, converter.convert(
                new PgOutputMessage.Column(false, "duration", 1186, -1),
                text("1 year 2 mons 3 days 04:05:06.123456")));
        assertEquals("01:05:06.123456Z", converter.convert(
                new PgOutputMessage.Column(false, "zoned_time", 1266, 6),
                text("04:05:06.123456+03")));
        assertArrayEquals(new BigInteger("123456").toByteArray(), (byte[]) converter.convert(
                new PgOutputMessage.Column(false, "price", 790, -1), text("$1,234.56")));
    }

    @Test
    void parsesArraysAndDynamicHstoreType() {
        PgOutputMessage.Column integers = new PgOutputMessage.Column(false, "values", 1007, -1);
        ColumnMetadata integerArray = new ColumnMetadata(false, null, false, "_int4", true, 23, "int4");
        assertEquals(List.of(1, -2, 3), converter.convert(integers, text("{1,-2,3}"), integerArray));
        assertEquals(Arrays.asList(null, null), converter.convert(
                integers, text("{{1,2},{3,4}}"), integerArray));

        PgOutputMessage.Column strings = new PgOutputMessage.Column(false, "values", 1009, -1);
        ColumnMetadata textArray = new ColumnMetadata(false, null, false, "_text", true, 25, "text");
        assertEquals(List.of("alpha", "comma,value", "quote\"value"), converter.convert(
                strings, text("{alpha,\"comma,value\",\"quote\\\"value\"}"), textArray));

        PgOutputMessage.Column hstore = new PgOutputMessage.Column(false, "labels", 99999, -1);
        ColumnMetadata hstoreType = new ColumnMetadata(false, null, false, "hstore", false, 0, null);
        assertEquals("{\"alpha\":\"one\",\"beta\":\"two\"}", converter.convert(
                hstore, text("\"beta\"=>\"two\", \"alpha\"=>\"one\""), hstoreType));
    }

    @Test
    void preservesUnknownScalarAndArrayValuesAsUtf8Bytes() {
        ColumnMetadata scalar = new ColumnMetadata(
                true, null, false, "cube", false, 0, null);
        ColumnMetadata array = new ColumnMetadata(
                true, null, false, "_cube", true, 99_999, "cube");

        assertArrayEquals("(1, 2)".getBytes(StandardCharsets.UTF_8),
                (byte[]) converter.convertUnknown(text("(1, 2)"), scalar));
        @SuppressWarnings("unchecked")
        List<byte[]> values = (List<byte[]>) converter.convertUnknown(
                text("{\"(1, 2)\",NULL,\"(3, 4)\"}"), array);
        assertArrayEquals("(1, 2)".getBytes(StandardCharsets.UTF_8), values.get(0));
        assertEquals(null, values.get(1));
        assertArrayEquals("(3, 4)".getBytes(StandardCharsets.UTF_8), values.get(2));
    }

    @Test
    void preservesPinnedDebeziumInfinityAndNegativeScaleSemantics() {
        PgOutputMessage.Column date = new PgOutputMessage.Column(false, "date", 1082, -1);
        PgOutputMessage.Column timestamp = new PgOutputMessage.Column(false, "timestamp", 1114, -1);
        PgOutputMessage.Column timestamptz = new PgOutputMessage.Column(false, "timestamptz", 1184, -1);
        assertEquals(-2_147_472_692, converter.convert(date, text("infinity")));
        assertEquals(-2_147_472_691, converter.convert(date, text("-infinity")));
        assertEquals(9_223_372_036_825_200_000L, converter.convert(timestamp, text("infinity")));
        assertEquals(-9_223_372_036_832_400_000L, converter.convert(timestamp, text("-infinity")));
        assertEquals("infinity", converter.convert(timestamptz, text("infinity")));

        PgOutputMessage.Column negativeScale = new PgOutputMessage.Column(
                false, "rounded", 1700, 4 + (6 << 16) + 2046);
        assertArrayEquals(new BigDecimal("123500").setScale(2046).unscaledValue().toByteArray(),
                (byte[]) converter.convert(negativeScale, text("123500")));
    }

    @Test
    void appliesStringHexMapAndIntervalStringModes() {
        PostgresValueConverter modes = new PostgresValueConverter(
                "string", "hex", "map", "string");
        PgOutputMessage.Column fixed = new PgOutputMessage.Column(
                false, "amount", 1700, 4 + (10 << 16) + 3);
        PgOutputMessage.Column variable = new PgOutputMessage.Column(
                false, "arbitrary", 1700, -1);
        PgOutputMessage.Column bytes = new PgOutputMessage.Column(
                false, "bytes_value", 17, -1);
        PgOutputMessage.Column money = new PgOutputMessage.Column(
                false, "price", 790, -1);
        PgOutputMessage.Column hstore = new PgOutputMessage.Column(
                false, "properties", 99999, -1);
        ColumnMetadata hstoreType = new ColumnMetadata(
                true, null, false, "hstore", false, 0, null);
        PgOutputMessage.Column interval = new PgOutputMessage.Column(
                false, "duration", 1186, -1);

        assertEquals("12345.678", modes.convert(fixed, text("12345.678")));
        assertEquals("0.000000000123", modes.convert(
                variable, text("0.000000000123")));
        assertEquals("fbff00", modes.convert(bytes, text("\\xfbff00")));
        assertEquals("1234.56", modes.convert(money, text("$1,234.56")));
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("nil", null);
        expected.put("alpha", "one");
        assertEquals(expected, modes.convert(
                hstore, text("\"alpha\"=>\"one\", \"nil\"=>NULL"), hstoreType));
        assertEquals("P1Y2M3DT4H5M6.789S", modes.convert(interval,
                text("1 year 2 mons 3 days 04:05:06.789")));
        assertEquals("P0Y0M-2DT0H0M1.5S", modes.convert(interval,
                text("-2 days +00:00:01.5")));
        assertEquals("P0Y0M0DT0H0M0S", modes.convert(interval, text("00:00:00")));
    }

    @Test
    void appliesDoubleAndBase64Modes() {
        PgOutputMessage.Column numeric = new PgOutputMessage.Column(
                false, "amount", 1700, -1);
        PgOutputMessage.Column bytes = new PgOutputMessage.Column(
                false, "bytes_value", 17, -1);
        PostgresValueConverter urlSafe = new PostgresValueConverter(
                "double", "base64-url-safe", "json", "numeric");
        PostgresValueConverter base64 = new PostgresValueConverter(
                "precise", "base64", "json", "numeric");

        assertEquals(1.23e-10, urlSafe.convert(numeric, text("0.000000000123")));
        assertEquals("-_8A", urlSafe.convert(bytes, text("\\xfbff00")));
        assertEquals("-_8=", urlSafe.convert(bytes, text("\\xfbff")));
        assertEquals("+/8A", base64.convert(bytes, text("\\xfbff00")));
        assertEquals("+/8=", base64.convert(bytes, text("\\xfbff")));
    }

    @Test
    void appliesAdaptiveConnectIsoMicroAndNanoTimePrecisionModes() {
        PgOutputMessage.Column time3 = new PgOutputMessage.Column(
                false, "time_3", 1083, 3);
        PgOutputMessage.Column time6 = new PgOutputMessage.Column(
                false, "time_6", 1083, 6);
        PgOutputMessage.Column timestamp3 = new PgOutputMessage.Column(
                false, "timestamp_3", 1114, 3);
        PgOutputMessage.Column timestamp6 = new PgOutputMessage.Column(
                false, "timestamp_6", 1114, 6);
        PgOutputMessage.Column date = new PgOutputMessage.Column(
                false, "date_value", 1082, -1);

        PostgresValueConverter adaptive = temporal("adaptive");
        assertEquals(14_706_123, adaptive.convert(time3, text("04:05:06.123")));
        assertEquals(14_706_123_456L,
                adaptive.convert(time6, text("04:05:06.123456")));
        assertEquals(1_786_421_106_123L, adaptive.convert(
                timestamp3, text("2026-08-11 04:05:06.123")));
        assertEquals(1_786_421_106_123_456L, adaptive.convert(
                timestamp6, text("2026-08-11 04:05:06.123456")));

        PostgresValueConverter adaptiveTimeMicros = temporal(
                "adaptive_time_microseconds");
        assertEquals(14_706_123_000L, adaptiveTimeMicros.convert(
                time3, text("04:05:06.123")));
        assertEquals(1_786_421_106_123L, adaptiveTimeMicros.convert(
                timestamp3, text("2026-08-11 04:05:06.123")));

        PostgresValueConverter connect = temporal("connect");
        assertEquals(14_706_123, connect.convert(time6, text("04:05:06.123456")));
        assertEquals(1_786_421_106_123L, connect.convert(
                timestamp6, text("2026-08-11 04:05:06.123456")));

        PostgresValueConverter iso = temporal("isostring");
        assertEquals("2026-08-11Z", iso.convert(date, text("2026-08-11")));
        assertEquals("04:05:06.123456Z", iso.convert(
                time6, text("04:05:06.123456")));
        assertEquals("2026-08-11T04:05:06.123456Z", iso.convert(
                timestamp6, text("2026-08-11 04:05:06.123456")));

        assertEquals(14_706_123_456L, temporal("microseconds").convert(
                time6, text("04:05:06.123456")));
        assertEquals(14_706_123_456_000L, temporal("nanoseconds").convert(
                time6, text("04:05:06.123456")));
        assertEquals(1_786_421_106_123_456_000L, temporal("nanoseconds").convert(
                timestamp6, text("2026-08-11 04:05:06.123456")));
    }

    @Test
    void matchesPinnedTemporalArrayInfinityAndPreEpochSemantics() {
        PgOutputMessage.Column date = new PgOutputMessage.Column(
                false, "date_value", 1082, -1);
        PgOutputMessage.Column timestamp6 = new PgOutputMessage.Column(
                false, "timestamp_6", 1114, 6);
        PgOutputMessage.Column dateArray = new PgOutputMessage.Column(
                false, "date_values", 1182, -1);
        PgOutputMessage.Column timeArray = new PgOutputMessage.Column(
                false, "time_values", 1183, 6);
        PgOutputMessage.Column timestamptzArray = new PgOutputMessage.Column(
                false, "timestamptz_values", 1185, 6);
        ColumnMetadata dates = new ColumnMetadata(
                true, null, false, "_date", true, 1082, "date");
        ColumnMetadata times = new ColumnMetadata(
                true, null, false, "_time", true, 1083, "time");
        ColumnMetadata timestamptz = new ColumnMetadata(
                true, null, false, "_timestamptz", true, 1184, "timestamptz");

        PostgresValueConverter adaptive = temporal("adaptive");
        assertEquals(Arrays.asList(-622_191_234, null, -625_821_272),
                adaptive.convert(dateArray,
                        text("{infinity,NULL,-infinity}"), dates));
        assertEquals(List.of(14_706_123_000L, 86_399_999_000L),
                adaptive.convert(timeArray,
                        text("{04:05:06.123456,23:59:59.999999}"), times));

        PostgresValueConverter iso = temporal("isostring");
        assertEquals("-5877611-06-21Z", iso.convert(date, text("infinity")));
        assertEquals("+294247-01-10T04:00:25.2Z",
                iso.convert(timestamp6, text("infinity")));
        assertEquals(List.of("+292278994-08-16Z", "+292269055-12-02Z"),
                iso.convert(dateArray, text("{infinity,-infinity}"), dates));
        assertEquals(List.of("04:05:06.123Z", "00:00:00Z"),
                iso.convert(timeArray, text("{04:05:06.123456,00:00:00}"), times));
        assertEquals(List.of("+292269055-12-02T23:00:00.000000Z"),
                iso.convert(timestamptzArray, text("{-infinity}"), timestamptz));

        assertEquals(-1L, temporal("connect").convert(
                timestamp6, text("1969-12-31 23:59:59.999999")));
        assertEquals(Long.MAX_VALUE, temporal("nanoseconds").convert(
                timestamp6, text("infinity")));
        assertEquals(Long.MIN_VALUE, temporal("nanoseconds").convert(
                timestamp6, text("-infinity")));
    }

    private static PostgresValueConverter temporal(String mode) {
        return new PostgresValueConverter(
                "precise", "bytes", "json", "numeric", mode);
    }

    private static PgOutputMessage.TupleValue text(String value) {
        return new PgOutputMessage.TupleValue(PgOutputMessage.ValueKind.TEXT,
                value.getBytes(StandardCharsets.UTF_8));
    }
}
