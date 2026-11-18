package io.cdcbench.compatibility.converter;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.sql.Types;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Pattern;

/** External compatibility fixture for Debezium's public custom-converter SPI. */
public final class PrefixingTextConverter
        implements CustomConverter<SchemaBuilder, RelationalColumn> {
    private String prefix;
    private Pattern columnPattern;

    @Override
    public void configure(Properties properties) {
        prefix = properties.getProperty("prefix", "converted:");
        columnPattern = Pattern.compile(properties.getProperty("column.regex", ".*"));
    }

    @Override
    public void converterFor(
            RelationalColumn field,
            ConverterRegistration<SchemaBuilder> registration) {
        String qualifiedColumn = field.dataCollection() + "." + field.name();
        if (field.jdbcType() != Types.VARCHAR
                || !columnPattern.matcher(qualifiedColumn).matches()) {
            return;
        }
        SchemaBuilder schema = SchemaBuilder.string()
                .name("io.cdcbench.compatibility.PrefixedText")
                .parameter("converter", "prefixing-text");
        if (field.isOptional()) {
            schema.optional();
        }
        if (field.hasDefaultValue() && field.defaultValue() != null) {
            schema.defaultValue(convert(field.defaultValue()));
        }
        registration.register(schema, this::convert);
    }

    private String convert(Object value) {
        return prefix + value.toString().toUpperCase(Locale.ROOT);
    }
}
