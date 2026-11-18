package io.cdcbench.connect.optimized;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class KafkaSignalChannelCloseTest {
    @Test
    void preservesFiveSecondTimeoutAndDefaultGroupMembershipOperation() {
        try (RecordingConsumer consumer = new RecordingConsumer()) {
            KafkaSignalChannel channel = new KafkaSignalChannel(consumer, "test", 100);
            channel.close();

            assertEquals(Duration.ofSeconds(5), consumer.options.timeout().orElseThrow());
            assertEquals(CloseOptions.timeout(Duration.ofSeconds(5)).groupMembershipOperation(),
                    consumer.options.groupMembershipOperation());
            assertEquals(1, consumer.calls.get());
        }
    }

    @Test
    void propagatesConsumerFailureWithoutRetryOrSuppression() {
        try (RecordingConsumer consumer = new RecordingConsumer()) {
            RuntimeException failure = new IllegalStateException("injected close failure");
            consumer.failure = failure;
            KafkaSignalChannel channel = new KafkaSignalChannel(consumer, "test", 100);

            assertSame(failure, assertThrows(IllegalStateException.class, channel::close));
            assertEquals(1, consumer.calls.get());
        }
    }

    @Test
    void retainsSynchronizationAndSerializesConcurrentCloseCalls() throws Exception {
        assertTrue(Modifier.isSynchronized(
                KafkaSignalChannel.class.getDeclaredMethod("close").getModifiers()));
        try (RecordingConsumer consumer = new RecordingConsumer();
             var executor = Executors.newFixedThreadPool(2)) {
            KafkaSignalChannel channel = new KafkaSignalChannel(consumer, "test", 100);
            CountDownLatch start = new CountDownLatch(1);
            var first = executor.submit(() -> {
                assertTrue(start.await(5, TimeUnit.SECONDS));
                channel.close();
                return null;
            });
            var second = executor.submit(() -> {
                assertTrue(start.await(5, TimeUnit.SECONDS));
                channel.close();
                return null;
            });
            start.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
            assertEquals(2, consumer.calls.get());
            assertEquals(1, consumer.maximumConcurrent.get());
        }
    }

    private static final class RecordingConsumer extends KafkaConsumer<String, String> {
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicInteger concurrent = new AtomicInteger();
        private final AtomicInteger maximumConcurrent = new AtomicInteger();
        private CloseOptions options;
        private RuntimeException failure;

        RecordingConsumer() {
            super(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1",
                    ConsumerConfig.GROUP_ID_CONFIG, "close-contract-test",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));
        }

        @Override
        public void close(CloseOptions options) {
            int active = concurrent.incrementAndGet();
            maximumConcurrent.accumulateAndGet(active, Math::max);
            try {
                this.options = options;
                calls.incrementAndGet();
                if (failure != null) {
                    throw failure;
                }
            } finally {
                concurrent.decrementAndGet();
            }
        }

        @Override
        public void close() {
            super.close(CloseOptions.timeout(Duration.ofSeconds(5)));
        }
    }
}
