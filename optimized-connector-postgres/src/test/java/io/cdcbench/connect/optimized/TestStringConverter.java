package io.cdcbench.connect.optimized;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.sql.Types;
import java.util.Locale;
import java.util.Properties;

/** Test-only public SPI converter loaded reflectively by selector and adapter tests. */
public final class TestStringConverter
        implements CustomConverter<SchemaBuilder, RelationalColumn> {
    private String prefix;

    @Override
    public void configure(Properties properties) {
        prefix = properties.getProperty("prefix", "test:");
    }

    @Override
    public void converterFor(
            RelationalColumn field,
            ConverterRegistration<SchemaBuilder> registration) {
        if (field.jdbcType() != Types.VARCHAR
                || !"public.events".equals(field.dataCollection())
                || !"payload".equals(field.name())) {
            return;
        }
        SchemaBuilder schema = SchemaBuilder.string()
                .name("io.cdcbench.test.PrefixedText")
                .parameter("converter", "test-string");
        if (field.isOptional()) {
            schema.optional();
        }
        registration.register(schema, value -> prefix
                + value.toString().toUpperCase(Locale.ROOT));
    }
}
