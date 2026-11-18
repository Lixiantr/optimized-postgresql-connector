package io.cdcbench.connect.optimized;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class ConnectSchemaAdapterTest {
    @Test
    void createsNamedStructsAndConnectDecimalValuesForSmts() {
        Map<String, Object> decimal = new LinkedHashMap<>();
        decimal.put("type", "bytes");
        decimal.put("name", "org.apache.kafka.connect.data.Decimal");
        decimal.put("version", 1);
        decimal.put("parameters", Map.of("scale", "2"));
        decimal.put("field", "amount");
        decimal.put("default", BigInteger.ZERO.toByteArray());

        Map<String, Object> id = new LinkedHashMap<>();
        id.put("type", "int32");
        id.put("field", "id");

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("type", "struct");
        row.put("name", "server.public.events.Value");
        row.put("optional", true);
        row.put("field", "after");
        row.put("fields", List.of(id, decimal));

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("type", "struct");
        envelope.put("name", "server.public.events.Envelope");
        envelope.put("version", 2);
        envelope.put("fields", List.of(row));

        Map<String, Object> after = new LinkedHashMap<>();
        after.put("id", 7);
        after.put("amount", new BigInteger("12345").toByteArray());
        ConnectSchemaAdapter.AdaptedValue adapted = ConnectSchemaAdapter.adapt(
                envelope, Map.of("after", after));

        Struct root = (Struct) adapted.value();
        Struct convertedRow = root.getStruct("after");
        assertEquals("server.public.events.Envelope", adapted.schema().name());
        assertEquals(2, adapted.schema().version());
        assertEquals(7, convertedRow.getInt32("id"));
        assertEquals(new BigDecimal("123.45"), convertedRow.get("amount"));
        assertEquals(new BigDecimal("0.00"),
                convertedRow.schema().field("amount").schema().defaultValue());
    }

    @Test
    void preservesNullArrayElementsAndRawBytes() {
        Map<String, Object> bytes = new LinkedHashMap<>();
        bytes.put("type", "bytes");
        bytes.put("optional", true);
        Map<String, Object> array = new LinkedHashMap<>();
        array.put("type", "array");
        array.put("items", bytes);

        byte[] value = new byte[]{1, 2, 3};
        ConnectSchemaAdapter.AdaptedValue adapted = ConnectSchemaAdapter.adapt(
                array, java.util.Arrays.asList(value, null));
        List<?> converted = (List<?>) adapted.value();

        assertArrayEquals(value, (byte[]) converted.get(0));
        assertEquals(null, converted.get(1));
    }

    @Test
    void roundTripsMapsProducedByCustomConverters() {
        Schema schema = SchemaBuilder.map(
                        Schema.STRING_SCHEMA,
                        SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).build())
                .name("io.cdcbench.test.MapValue")
                .parameter("converter", "map-test")
                .build();
        Map<String, List<Integer>> source = new LinkedHashMap<>();
        source.put("first", java.util.Arrays.asList(1, null, 3));

        ConnectSchemaAdapter.AdaptedValue adapted = ConnectSchemaAdapter.adapt(
                ConnectSchemaAdapter.describe(schema, null),
                ConnectSchemaAdapter.encode(schema, source));

        assertEquals("io.cdcbench.test.MapValue", adapted.schema().name());
        assertEquals("map-test", adapted.schema().parameters().get("converter"));
        assertEquals(source, adapted.value());
    }

    @Test
    void roundTripsStructAndDecimalValuesProducedByCustomConverters() {
        Schema schema = SchemaBuilder.struct()
                .name("io.cdcbench.test.StructValue")
                .field("amount", Decimal.builder(2).optional().build())
                .field("label", Schema.STRING_SCHEMA)
                .build();
        Struct source = new Struct(schema)
                .put("amount", new BigDecimal("123.45"))
                .put("label", "converted");

        ConnectSchemaAdapter.AdaptedValue adapted = ConnectSchemaAdapter.adapt(
                ConnectSchemaAdapter.describe(schema, null),
                ConnectSchemaAdapter.encode(schema, source));
        Struct converted = (Struct) adapted.value();

        assertEquals(new BigDecimal("123.45"), converted.get("amount"));
        assertEquals("converted", converted.getString("label"));
    }

    @Test
    void acceptsCanonicalJsonFloatNamesAndPreservesDocumentation() {
        Map<String, Object> floatDescription = new LinkedHashMap<>();
        floatDescription.put("type", "float");
        floatDescription.put("doc", "single precision");
        Map<String, Object> doubleDescription = Map.of("type", "double");

        ConnectSchemaAdapter.AdaptedValue floatValue = ConnectSchemaAdapter.adapt(
                floatDescription, 1.25d);
        ConnectSchemaAdapter.AdaptedValue doubleValue = ConnectSchemaAdapter.adapt(
                doubleDescription, 2.5f);

        assertEquals(Schema.Type.FLOAT32, floatValue.schema().type());
        assertEquals(1.25f, floatValue.value());
        assertEquals("single precision", floatValue.schema().doc());
        assertEquals("float", ConnectSchemaAdapter.describe(
                floatValue.schema(), null).get("type"));
        assertEquals("single precision", ConnectSchemaAdapter.describe(
                floatValue.schema(), null).get("doc"));
        assertEquals(Schema.Type.FLOAT64, doubleValue.schema().type());
        assertEquals(2.5d, doubleValue.value());
    }
}
