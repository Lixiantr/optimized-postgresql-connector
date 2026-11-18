# Optimized PostgreSQL Connector

Самостоятельный Java-коннектор для Kafka Connect. Он читает изменения из WAL PostgreSQL через `pgoutput` и публикует события в Kafka. Одна Connect task читает слот репликации, а внутренние потоки параллельно обрабатывают записи с сохранением порядка выдачи.

Цель проекта — повысить пропускную способность PostgreSQL CDC и снизить расход CPU на событие, сохранив совместимый с Debezium формат событий для поддерживаемых конфигураций.

## Результаты тестов

Сравнение с Debezium PostgreSQL **3.6.0.Final** при вычитывании заранее подготовленного WAL в Kafka.

| Размер JSON-события | Debezium, событий/с | Optimized, событий/с | Прирост throughput | CPU на событие |
| --- | ---: | ---: | ---: | ---: |
| ≈1 КиБ | 18 342 | 25 128 | +38,8% | −37,2% |
| ≈10 КиБ | 12 609 | 17 225 | +36,5% | −47,3% |
| ≈1 МиБ | 154 | 269 | +75,7% | −49,2% |

По 10 измеряемых раундов на профиль, Kafka Connect ограничен 4 vCPU, JSON без схем. Для Optimized использован один внутренний worker с включённым raw UTF-8. Скорость измерялась между первой и последней записью в Kafka по `LogAppendTime`. Абсолютные значения — средние скорости, проценты — средние парные отношения по раундам.

В том же наборе проверок успешно прошли сравнение выходных событий с Debezium, проверка конфигурационного контракта, совместимость offsets при обновлении, distributed source exactly-once, переключение PostgreSQL 17 и два часовых прогона на стабильность.

Сравнительные результаты относятся к измеренной конфигурации. Для исходников в этом репозитории отдельно выполнена чистая сборка с 195 unit-тестами.

## Использование

Требуются Java 21, Kafka Connect 4.3.x и PostgreSQL с логической репликацией. Класс коннектора — `io.cdcbench.connect.optimized.OptimizedPostgresConnector`.

- [Подготовка PostgreSQL, установка и настройка](documentation/modules/ROOT/pages/connectors/postgresql.adoc)
- [Эксплуатация и диагностика](documentation/modules/ROOT/pages/operations.adoc)
- [Обновление и откат](documentation/modules/ROOT/pages/upgrading.adoc)

## Сборка и тестирование

Из корня репозитория выполните `mvn -B -ntp clean verify`. Maven соберёт Java-модули, запустит unit-тесты и положит JAR коннектора в `optimized-connector-postgres/target/`.

[README PostgreSQL-модуля](optimized-connector-postgres/README.md) содержит инструкции по сборке каталога плагина и запуску интеграционных тестов.

## Лицензия

[Apache License 2.0](LICENSE). Сведения о компонентах указаны в [NOTICE](NOTICE).
