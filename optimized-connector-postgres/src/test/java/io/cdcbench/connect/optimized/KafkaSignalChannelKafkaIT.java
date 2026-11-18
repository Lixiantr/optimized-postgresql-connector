package io.cdcbench.connect.optimized;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Explicit integration suite: -Dtest=KafkaSignalChannelKafkaIT against an isolated broker. */
final class KafkaSignalChannelKafkaIT {
    @Test
    void consumesAndCommitsRealSignalThenClosesWithinTimeoutBudget() throws Exception {
        String bootstrap = bootstrap();
        String topic = "close-contract-" + UUID.randomUUID();
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.createTopics(java.util.List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                "bootstrap.servers", bootstrap,
                "key.serializer", StringSerializer.class.getName(),
                "value.serializer", StringSerializer.class.getName()))) {
            producer.send(new ProducerRecord<>(topic, "candidate",
                    "{\"type\":\"stop-snapshot\",\"data\":{\"type\":\"INCREMENTAL\"}}"))
                    .get(30, TimeUnit.SECONDS);
        }
        KafkaSignalChannel channel = new KafkaSignalChannel(settings(bootstrap, topic), "candidate");
        try {
            KafkaSignalChannel.Batch batch;
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            do {
                batch = channel.poll();
            } while (!batch.hadRecords() && System.nanoTime() < deadline);
            assertTrue(batch.hadRecords());
            assertEquals(1, batch.requests().size());
            channel.commit();
        } finally {
            long start = System.nanoTime();
            channel.close();
            assertTrue(System.nanoTime() - start < Duration.ofSeconds(8).toNanos());
        }
    }

    @Test
    void closesAfterWakeupExceptionWithoutSwallowingThePollFailure() {
        KafkaSignalChannel channel = new KafkaSignalChannel(
                settings(bootstrap(), "close-wakeup-" + UUID.randomUUID()), "candidate");
        try {
            channel.wakeup();
            assertThrows(WakeupException.class, channel::poll);
        } finally {
            channel.close();
        }
    }

    private static String bootstrap() {
        String value = Objects.requireNonNull(System.getenv("CDC_TEST_KAFKA_BOOTSTRAP"),
                "CDC_TEST_KAFKA_BOOTSTRAP must point to the isolated integration broker");
        assertFalse(value.isBlank());
        return value;
    }

    private static OptimizedSettings settings(String bootstrap, String topic) {
        return new OptimizedSettings(Map.of(
                "signal.enabled.channels", "kafka",
                "signal.kafka.bootstrap.servers", bootstrap,
                "signal.kafka.topic", topic,
                "signal.kafka.groupId", topic + "-group",
                "signal.kafka.poll.timeout.ms", "100"));
    }
}
