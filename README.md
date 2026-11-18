# Optimized PostgreSQL Connector

A standalone Java connector for Kafka Connect. It reads PostgreSQL WAL changes through `pgoutput` and publishes events to Kafka. One Kafka Connect task reads the replication slot, and internal workers process records in parallel while preserving output order.

The project aims to increase PostgreSQL CDC throughput and reduce CPU cost per event while retaining a Debezium-compatible event format for supported configurations.

## Test results

The benchmark compared the connector with Debezium PostgreSQL **3.6.0.Final** while draining a pre-generated WAL backlog into Kafka.

| JSON event size | Debezium, events/s | Optimized, events/s | Throughput gain | CPU per event |
| --- | ---: | ---: | ---: | ---: |
| ≈1 KiB | 18,342 | 25,128 | +38.8% | −37.2% |
| ≈10 KiB | 12,609 | 17,225 | +36.5% | −47.3% |
| ≈1 MiB | 154 | 269 | +75.7% | −49.2% |

Each profile ran for 10 measured rounds with a 4-vCPU Kafka Connect limit and schemaless JSON. The optimized connector used one internal worker with raw UTF-8 enabled. The measurement window ran from the first to the last Kafka record using `LogAppendTime`. The throughput columns show mean rates. The percentage columns use the mean of the paired ratios across rounds.

The same test suite passed event parity with Debezium, configuration contract checks, offset compatibility across upgrades, distributed source exactly-once delivery, PostgreSQL 17 failover, and two one-hour stability runs.

The comparison applies to the measured configuration. A separate clean build of the source in this repository passed 195 unit tests.

## Usage

Requires Java 21, Kafka Connect 4.3.x, and PostgreSQL with logical replication. The connector class is `io.cdcbench.connect.optimized.OptimizedPostgresConnector`.

- [PostgreSQL preparation, installation, and configuration](documentation/modules/ROOT/pages/connectors/postgresql.adoc)
- [Operations and troubleshooting](documentation/modules/ROOT/pages/operations.adoc)
- [Upgrades and rollback](documentation/modules/ROOT/pages/upgrading.adoc)

## Build and test

Run `mvn -B -ntp clean verify` from the repository root. Maven builds the Java modules, runs the unit tests, and writes the connector JAR to `optimized-connector-postgres/target/`.

See the [PostgreSQL module README](optimized-connector-postgres/README.md) for plugin packaging and integration tests.

## License

[Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for component notices.
