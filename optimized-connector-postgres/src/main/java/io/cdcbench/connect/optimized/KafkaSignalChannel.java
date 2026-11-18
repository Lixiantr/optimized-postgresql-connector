package io.cdcbench.connect.optimized;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class KafkaSignalChannel implements AutoCloseable {
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5L);

    private final KafkaConsumer<String, String> consumer;
    private final String expectedKey;
    private final long pollTimeoutMs;

    KafkaSignalChannel(OptimizedSettings settings, String topicPrefix) {
        expectedKey = topicPrefix;
        pollTimeoutMs = settings.signalKafkaPollTimeoutMs();
        Map<String, Object> properties = new HashMap<>(settings.signalConsumerProperties());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                settings.signalKafkaBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, settings.signalKafkaGroupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(settings.signalKafkaTopic(topicPrefix)));
    }

    KafkaSignalChannel(KafkaConsumer<String, String> consumer, String expectedKey,
                       long pollTimeoutMs) {
        this.consumer = consumer;
        this.expectedKey = expectedKey;
        this.pollTimeoutMs = pollTimeoutMs;
    }

    synchronized Batch poll() {
        ConsumerRecords<String, String> records = consumer.poll(
                Duration.ofMillis(pollTimeoutMs));
        List<SnapshotSignal.Request> requests = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            SnapshotSignal.parseKafka(expectedKey, record.key(), record.value())
                    .ifPresent(requests::add);
        }
        return new Batch(List.copyOf(requests), !records.isEmpty());
    }

    synchronized void commit() {
        consumer.commitSync();
    }

    void wakeup() {
        consumer.wakeup();
    }

    @Override
    public synchronized void close() {
        consumer.close(CloseOptions.timeout(CLOSE_TIMEOUT));
    }

    record Batch(List<SnapshotSignal.Request> requests, boolean hadRecords) {}
}
