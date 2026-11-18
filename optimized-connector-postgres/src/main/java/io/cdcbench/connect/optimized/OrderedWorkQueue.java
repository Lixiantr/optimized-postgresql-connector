package io.cdcbench.connect.optimized;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

final class OrderedWorkQueue<M, T> implements AutoCloseable {
    private final ExecutorService executor;
    private final int workers;
    private final Deque<PendingBatch<M, T>> batches = new ArrayDeque<>();
    private int pendingItems;
    private long pendingBytes;
    private boolean closed;

    OrderedWorkQueue(int workers, String threadNamePrefix) {
        if (workers < 1) {
            throw new IllegalArgumentException("workers must be positive");
        }
        this.workers = workers;
        executor = workers == 1 ? null : Executors.newFixedThreadPool(
                workers, namedThreadFactory(threadNamePrefix));
    }

    synchronized void submit(M metadata, int itemCount, long byteCount, List<Callable<List<T>>> chunks) {
        submit(metadata, itemCount, byteCount, false, chunks);
    }

    synchronized void submit(M metadata, int itemCount, long byteCount,
                             boolean allowFewerItems, List<Callable<List<T>>> chunks) {
        if (closed) {
            throw new ConnectException("Ordered worker queue is closed");
        }
        if (itemCount < 1 || chunks.isEmpty()) {
            throw new IllegalArgumentException("A work batch must contain at least one item and one chunk");
        }
        if (byteCount < 1L) {
            throw new IllegalArgumentException("A work batch must account for at least one byte");
        }

        List<Future<List<T>>> futures = new ArrayList<>(chunks.size());
        for (Callable<List<T>> chunk : chunks) {
            if (executor == null) {
                futures.add(callDirectly(chunk));
            } else {
                futures.add(executor.submit(chunk));
            }
        }
        batches.addLast(new PendingBatch<>(
                metadata, itemCount, byteCount, allowFewerItems, List.copyOf(futures),
                null, chunks.size(), chunks.size(), 0, false));
        pendingItems = Math.addExact(pendingItems, itemCount);
        pendingBytes = Math.addExact(pendingBytes, byteCount);
        notifyAll();
    }

    synchronized void submitStreaming(M metadata, int itemCount, long byteCount,
                                      boolean allowFewerItems, int chunkCount,
                                      IntFunction<Callable<List<T>>> chunkFactory) {
        if (closed) {
            throw new ConnectException("Ordered worker queue is closed");
        }
        if (itemCount < 1 || chunkCount < 1) {
            throw new IllegalArgumentException(
                    "A streaming work batch must contain at least one item and one chunk");
        }
        if (byteCount < 1L) {
            throw new IllegalArgumentException("A work batch must account for at least one byte");
        }

        PendingBatch<M, T> batch = new PendingBatch<>(
                metadata, itemCount, byteCount, allowFewerItems, new ArrayList<>(),
                chunkFactory, chunkCount, 0, 0, true);
        scheduleStreaming(batch);
        batches.addLast(batch);
        pendingItems = Math.addExact(pendingItems, itemCount);
        pendingBytes = Math.addExact(pendingBytes, byteCount);
        notifyAll();
    }

    synchronized Optional<CompletedBatch<M, T>> pollReady() {
        PendingBatch<M, T> head = batches.peekFirst();
        if (head == null) {
            return Optional.empty();
        }
        if (!head.streaming()) {
            if (head.futures().stream().anyMatch(future -> !future.isDone())) {
                return Optional.empty();
            }
            batches.removeFirst();
            pendingItems -= head.itemCount();
            pendingBytes -= head.byteCount();
            notifyAll();
            return Optional.of(complete(head));
        }

        if (head.futures().isEmpty() || !head.futures().getFirst().isDone()) {
            return Optional.empty();
        }
        Future<List<T>> future = head.futures().removeFirst();
        List<T> output = completedItems(future);
        head.emittedItems = Math.addExact(head.emittedItems, output.size());
        head.completedChunks++;
        boolean finalChunk = head.completedChunks == head.chunkCount();
        if (finalChunk) {
            verifyItemCount(head, head.emittedItems);
            batches.removeFirst();
            pendingItems -= head.itemCount();
            pendingBytes -= head.byteCount();
        } else {
            scheduleStreaming(head);
        }
        notifyAll();
        return Optional.of(new CompletedBatch<>(
                head.metadata(), List.copyOf(output), finalChunk));
    }

    synchronized boolean awaitBatch(long timeout, TimeUnit unit) throws InterruptedException {
        long remainingNanos = unit.toNanos(timeout);
        long deadline = System.nanoTime() + remainingNanos;
        while (batches.isEmpty() && !closed && remainingNanos > 0L) {
            TimeUnit.NANOSECONDS.timedWait(this, remainingNanos);
            remainingNanos = deadline - System.nanoTime();
        }
        return !batches.isEmpty();
    }

    synchronized boolean awaitCapacity(int maxBatches, int maxItems, long maxBytes,
                                       long timeout, TimeUnit unit)
            throws InterruptedException {
        long remainingNanos = unit.toNanos(timeout);
        long deadline = System.nanoTime() + remainingNanos;
        while (!closed
                && (batches.size() >= maxBatches
                || pendingItems >= maxItems
                || pendingBytes >= maxBytes)
                && remainingNanos > 0L) {
            TimeUnit.NANOSECONDS.timedWait(this, remainingNanos);
            remainingNanos = deadline - System.nanoTime();
        }
        return !closed && batches.size() < maxBatches && pendingItems < maxItems
                && pendingBytes < maxBytes;
    }

    synchronized boolean awaitHead(long timeout, TimeUnit unit) throws InterruptedException {
        PendingBatch<M, T> head = batches.peekFirst();
        if (head == null) {
            return false;
        }
        long remainingNanos = unit.toNanos(timeout);
        long deadline = System.nanoTime() + remainingNanos;
        List<Future<List<T>>> awaited = head.streaming()
                ? List.of(head.futures().getFirst()) : head.futures();
        for (Future<List<T>> future : awaited) {
            if (future.isDone()) {
                continue;
            }
            if (remainingNanos <= 0L) {
                return false;
            }
            try {
                future.get(remainingNanos, TimeUnit.NANOSECONDS);
            } catch (TimeoutException ignored) {
                return false;
            } catch (ExecutionException error) {
                throw workerFailure(error.getCause());
            }
            remainingNanos = deadline - System.nanoTime();
        }
        return true;
    }

    synchronized int pendingItems() {
        return pendingItems;
    }

    synchronized int pendingBatches() {
        return batches.size();
    }

    synchronized long pendingBytes() {
        return pendingBytes;
    }

    synchronized void reset() {
        for (PendingBatch<M, T> batch : batches) {
            for (Future<List<T>> future : batch.futures()) {
                future.cancel(true);
            }
        }
        batches.clear();
        pendingItems = 0;
        pendingBytes = 0L;
        notifyAll();
    }

    @Override
    public void close() {
        ExecutorService executorToClose;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            reset();
            executorToClose = executor;
        }
        if (executorToClose != null) {
            executorToClose.shutdownNow();
            try {
                executorToClose.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static <T> Future<List<T>> callDirectly(Callable<List<T>> chunk) {
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        try {
            future.complete(List.copyOf(chunk.call()));
        } catch (Throwable error) {
            future.completeExceptionally(error);
        }
        return future;
    }

    private static <M, T> CompletedBatch<M, T> complete(PendingBatch<M, T> batch) {
        List<T> output = new ArrayList<>(batch.itemCount());
        for (Future<List<T>> future : batch.futures()) {
            try {
                output.addAll(future.get());
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new ConnectException("Interrupted while collecting ordered worker output", error);
            } catch (ExecutionException error) {
                throw workerFailure(error.getCause());
            }
        }
        verifyItemCount(batch, output.size());
        return new CompletedBatch<>(batch.metadata(), List.copyOf(output), true);
    }

    private void scheduleStreaming(PendingBatch<M, T> batch) {
        while (batch.nextChunk < batch.chunkCount()
                && batch.futures().size() < workers) {
            Callable<List<T>> chunk = batch.chunkFactory().apply(batch.nextChunk++);
            batch.futures().add(executor == null
                    ? callDirectly(chunk) : executor.submit(chunk));
        }
    }

    private static <T> List<T> completedItems(Future<List<T>> future) {
        try {
            return future.get();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while collecting ordered worker output", error);
        } catch (ExecutionException error) {
            throw workerFailure(error.getCause());
        }
    }

    private static void verifyItemCount(PendingBatch<?, ?> batch, int actual) {
        if (actual > batch.itemCount()
                || (!batch.allowFewerItems() && actual != batch.itemCount())) {
            throw new ConnectException("Parallel materialization produced " + actual
                    + " records for a batch of " + batch.itemCount());
        }
    }

    private static ConnectException workerFailure(Throwable cause) {
        if (cause instanceof ConnectException connectException) {
            return connectException;
        }
        return new ConnectException("Parallel record materialization failed", cause);
    }

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, prefix + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    record CompletedBatch<M, T>(M metadata, List<T> items, boolean finalChunk) {
    }

    private static final class PendingBatch<M, T> {
        private final M metadata;
        private final int itemCount;
        private final long byteCount;
        private final boolean allowFewerItems;
        private final List<Future<List<T>>> futures;
        private final IntFunction<Callable<List<T>>> chunkFactory;
        private final int chunkCount;
        private final boolean streaming;
        private int nextChunk;
        private int completedChunks;
        private int emittedItems;

        PendingBatch(M metadata, int itemCount, long byteCount,
                     boolean allowFewerItems, List<Future<List<T>>> futures,
                     IntFunction<Callable<List<T>>> chunkFactory, int chunkCount,
                     int nextChunk, int completedChunks, boolean streaming) {
            this.metadata = metadata;
            this.itemCount = itemCount;
            this.byteCount = byteCount;
            this.allowFewerItems = allowFewerItems;
            this.futures = futures;
            this.chunkFactory = chunkFactory;
            this.chunkCount = chunkCount;
            this.nextChunk = nextChunk;
            this.completedChunks = completedChunks;
            this.streaming = streaming;
        }

        M metadata() { return metadata; }
        int itemCount() { return itemCount; }
        long byteCount() { return byteCount; }
        boolean allowFewerItems() { return allowFewerItems; }
        List<Future<List<T>>> futures() { return futures; }
        IntFunction<Callable<List<T>>> chunkFactory() { return chunkFactory; }
        int chunkCount() { return chunkCount; }
        boolean streaming() { return streaming; }
    }
}
