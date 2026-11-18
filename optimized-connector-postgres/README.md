# Capturing PostgreSQL changes

This module implements a Kafka Connect source connector. It uses PostgreSQL logical replication, a dedicated replication slot, and a publication that includes the tables to capture.

## Using the connector with Kafka Connect

Build the JAR and prepare the plugin directory. Run the following commands from the repository root in a POSIX shell.

```sh
mvn -B -ntp clean verify
mkdir -p plugin/optimized-postgres-connector
cp optimized-connector-postgres/target/optimized-postgres-connector.jar LICENSE NOTICE \
  plugin/optimized-postgres-connector/
mvn -B -ntp org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy \
  -N -Dartifact=org.postgresql:postgresql:42.7.11 \
  -DoutputDirectory="$PWD/plugin/optimized-postgres-connector"
```

Place the prepared directory under `plugin.path` on each worker and restart Kafka Connect. See the [connector reference](../documentation/modules/ROOT/pages/connectors/postgresql.adoc) for PostgreSQL setup, a JSON example, and a configuration comparison.

## Testing

Maven runs `*Test` unit tests during `clean verify`.

The Kafka integration suite checks signal delivery, offset commits, and consumer shutdown. Run it against a dedicated Kafka 4.3.0 test broker. The suite creates test topics.

```sh
mvn -B -ntp install
CDC_TEST_KAFKA_BOOTSTRAP=127.0.0.1:9092 mvn -B -ntp \
  -pl optimized-connector-postgres -Dtest=KafkaSignalChannelKafkaIT test
```
