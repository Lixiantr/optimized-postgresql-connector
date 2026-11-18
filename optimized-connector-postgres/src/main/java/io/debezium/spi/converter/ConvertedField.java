package io.debezium.spi.converter;

/** Clean-room binary-compatible form of the Debezium 3.6 public converter SPI. */
public interface ConvertedField {
    String name();

    String dataCollection();
}
