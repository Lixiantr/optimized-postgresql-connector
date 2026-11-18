package io.debezium.spi.converter;

import java.util.Properties;

/** Public extension contract retained for Debezium custom-converter compatibility. */
public interface CustomConverter<S, F extends ConvertedField> {
    void configure(Properties properties);

    void converterFor(F field, ConverterRegistration<S> registration);

    @FunctionalInterface
    interface Converter {
        Object convert(Object value);
    }

    @FunctionalInterface
    interface ConverterRegistration<S> {
        void register(S schema, Converter converter);
    }
}
