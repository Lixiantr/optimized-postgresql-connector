package io.cdcbench.connect.optimized;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class OrderedWorkQueueTest {
    @Test
    void restoresBatchAndChunkOrderAfterParallelCompletion() throws Exception {
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondBatchFinished = new CountDownLatch(1);

        try (OrderedWorkQueue<String, String> queue = new OrderedWorkQueue<>(2, "ordered-test-")) {
            queue.submit("first", 2, 20L, List.of(
                    () -> {
                        assertTrue(releaseFirst.await(5, TimeUnit.SECONDS));
                        return List.of("a");
                    },
                    () -> List.of("b")));
            queue.submit("second", 1, 10L, List.of(() -> {
                secondBatchFinished.countDown();
                return List.of("c");
            }));

            assertTrue(secondBatchFinished.await(5, TimeUnit.SECONDS));
            assertTrue(queue.pollReady().isEmpty(), "a later batch must not overtake the head");

            releaseFirst.countDown();
            assertTrue(queue.awaitHead(5, TimeUnit.SECONDS));
            OrderedWorkQueue.CompletedBatch<String, String> first = queue.pollReady().orElseThrow();
            OrderedWorkQueue.CompletedBatch<String, String> second = queue.pollReady().orElseThrow();

            assertEquals("first", first.metadata());
            assertEquals(List.of("a", "b"), first.items());
            assertEquals("second", second.metadata());
            assertEquals(List.of("c"), second.items());
            assertEquals(0, queue.pendingItems());
            assertEquals(0, queue.pendingBatches());
            assertEquals(0L, queue.pendingBytes());
        }
    }

    @Test
    void executesDirectlyWithOneWorker() {
        try (OrderedWorkQueue<String, Integer> queue = new OrderedWorkQueue<>(1, "direct-test-")) {
            queue.submit("direct", 3, 30L, List.of(() -> List.of(1, 2, 3)));

            OrderedWorkQueue.CompletedBatch<String, Integer> completed = queue.pollReady().orElseThrow();
            assertEquals(List.of(1, 2, 3), completed.items());
        }
    }

    @Test
    void permitsExplicitlySkippedItemsWithoutRelaxingTheUpperBound() throws Exception {
        try (OrderedWorkQueue<String, Integer> queue = new OrderedWorkQueue<>(2, "skip-test-")) {
            queue.submit("partial", 2, 2L, true, List.of(
                    () -> List.of(1),
                    List::of));

            assertTrue(queue.awaitHead(5, TimeUnit.SECONDS));
            OrderedWorkQueue.CompletedBatch<String, Integer> completed =
                    queue.pollReady().orElseThrow();
            assertEquals("partial", completed.metadata());
            assertEquals(List.of(1), completed.items());
        }

        try (OrderedWorkQueue<String, Integer> queue = new OrderedWorkQueue<>(1, "overflow-test-")) {
            queue.submit("overflow", 1, 1L, true, List.of(() -> List.of(1, 2)));
            ConnectException error = assertThrows(ConnectException.class, queue::pollReady);
            assertTrue(error.getMessage().contains("2 records for a batch of 1"));
        }
    }

    @Test
    void propagatesWorkerFailures() {
        try (OrderedWorkQueue<String, String> queue = new OrderedWorkQueue<>(1, "failure-test-")) {
            queue.submit("failure", 1, 10L, List.of(() -> {
                throw new IllegalStateException("broken value");
            }));

            assertThrows(ConnectException.class, queue::pollReady);
        }
    }

    @Test
    void exposesBatchArrivalAndBackpressureCapacity() throws Exception {
        CountDownLatch release = new CountDownLatch(1);

        try (OrderedWorkQueue<String, String> queue = new OrderedWorkQueue<>(2, "capacity-test-")) {
            assertFalse(queue.awaitBatch(1, TimeUnit.MILLISECONDS));

            queue.submit("batch", 1, 1024L, List.of(() -> {
                assertTrue(release.await(5, TimeUnit.SECONDS));
                return List.of("value");
            }));

            assertTrue(queue.awaitBatch(1, TimeUnit.MILLISECONDS));
            assertFalse(queue.awaitCapacity(1, 1, 1024L, 1, TimeUnit.MILLISECONDS));

            release.countDown();
            assertTrue(queue.awaitHead(5, TimeUnit.SECONDS));
            assertEquals(List.of("value"), queue.pollReady().orElseThrow().items());
            assertTrue(queue.awaitCapacity(1, 1, 1024L, 1, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    void appliesBackpressureToEstimatedBytes() throws Exception {
        CountDownLatch release = new CountDownLatch(1);

        try (OrderedWorkQueue<String, String> queue = new OrderedWorkQueue<>(2, "bytes-test-")) {
            queue.submit("large", 1, 2_000_000L, List.of(() -> {
                assertTrue(release.await(5, TimeUnit.SECONDS));
                return List.of("value");
            }));

            assertEquals(2_000_000L, queue.pendingBytes());
            assertFalse(queue.awaitCapacity(16, 100, 2_000_000L, 1, TimeUnit.MILLISECONDS));

            release.countDown();
            assertTrue(queue.awaitHead(5, TimeUnit.SECONDS));
            queue.pollReady().orElseThrow();
            assertEquals(0L, queue.pendingBytes());
            assertTrue(queue.awaitCapacity(16, 100, 2_000_000L, 1, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    void streamsLargeBatchesInOrderWithBoundedParallelLookahead() throws Exception {
        CountDownLatch releaseFirst = new CountDownLatch(1);

        try (OrderedWorkQueue<String, Integer> queue =
                     new OrderedWorkQueue<>(2, "stream-test-")) {
            queue.submitStreaming("large", 5, 100L, false, 5, index -> () -> {
                if (index == 0) {
                    assertTrue(releaseFirst.await(5, TimeUnit.SECONDS));
                }
                return List.of(index);
            });

            assertTrue(queue.pollReady().isEmpty(), "chunk 1 must not overtake chunk 0");
            releaseFirst.countDown();
            List<Integer> output = new java.util.ArrayList<>();
            boolean complete = false;
            while (!complete) {
                assertTrue(queue.awaitHead(5, TimeUnit.SECONDS));
                OrderedWorkQueue.CompletedBatch<String, Integer> chunk =
                        queue.pollReady().orElseThrow();
                output.addAll(chunk.items());
                complete = chunk.finalChunk();
            }

            assertEquals(List.of(0, 1, 2, 3, 4), output);
            assertEquals(0, queue.pendingItems());
            assertEquals(0L, queue.pendingBytes());
        }
    }
}
