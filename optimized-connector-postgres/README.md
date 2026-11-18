# Захват изменений PostgreSQL

Модуль реализует source connector для Kafka Connect. Для работы используются логическая репликация PostgreSQL, отдельный replication slot и публикация с нужными таблицами.

## Использование с Kafka Connect

Соберите JAR и подготовьте каталог плагина. Выполните команды из корня репозитория в POSIX-оболочке.

```sh
mvn -B -ntp clean verify
mkdir -p plugin/optimized-postgres-connector
cp optimized-connector-postgres/target/optimized-postgres-connector.jar LICENSE NOTICE \
  plugin/optimized-postgres-connector/
mvn -B -ntp org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy \
  -N -Dartifact=org.postgresql:postgresql:42.7.11 \
  -DoutputDirectory="$PWD/plugin/optimized-postgres-connector"
```

Скопируйте подготовленный каталог в `plugin.path` на каждом worker и перезапустите Kafka Connect. Настройки PostgreSQL, JSON-пример и сравнение параметров приведены в [справочнике](../documentation/modules/ROOT/pages/connectors/postgresql.adoc).

## Тестирование

Unit-тесты `*Test` выполняются в Maven при `clean verify`.

Интеграционный набор Kafka проверяет доставку сигнала, подтверждение его позиции и закрытие consumer. Запускайте его с отдельным тестовым брокером Kafka 4.3.0. Набор создаёт тестовые topics.

```sh
mvn -B -ntp install
CDC_TEST_KAFKA_BOOTSTRAP=127.0.0.1:9092 mvn -B -ntp \
  -pl optimized-connector-postgres -Dtest=KafkaSignalChannelKafkaIT test
```
