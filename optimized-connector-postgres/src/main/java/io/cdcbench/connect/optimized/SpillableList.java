package io.cdcbench.connect.optimized;

import org.apache.kafka.connect.errors.ConnectException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * Append-only list which keeps ordinary transactions in memory and transparently
 * moves large ones to a delete-on-close file.  Disk blocks are also the units of
 * parallel materialization, so a multi-gigabyte transaction never has to be read
 * back into the heap as one object graph.
 */
final class SpillableList<T> extends AbstractList<T> implements AutoCloseable {
    interface Codec<T> {
        void write(DataOutput output, T value) throws IOException;
        T read(DataInput input) throws IOException;
    }

    record Range(int fromIndex, int toIndex, long estimatedBytes) {}

    private final Codec<T> codec;
    private final Path directory;
    private final long residentLimitBytes;
    private final long blockLimitBytes;
    private final int blockLimitItems;
    private final List<T> memory = new ArrayList<>();
    private final List<Long> memoryEstimates = new ArrayList<>();
    private final List<T> pending = new ArrayList<>();
    private final List<Long> pendingEstimates = new ArrayList<>();
    private final List<Block> blocks = new ArrayList<>();
    private volatile CachedBlock<T> cache;

    private FileChannel channel;
    private Path path;
    private long residentBytes;
    private long pendingBytes;
    private long estimatedBytes;
    private long writePosition;
    private int size;
    private boolean sealed;
    private boolean closed;

    SpillableList(Codec<T> codec, Path directory, long residentLimitBytes,
                  long blockLimitBytes, int blockLimitItems) {
        this.codec = codec;
        this.directory = directory;
        this.residentLimitBytes = positive(residentLimitBytes, "residentLimitBytes");
        this.blockLimitBytes = positive(blockLimitBytes, "blockLimitBytes");
        if (blockLimitItems < 1) {
            throw new IllegalArgumentException("blockLimitItems must be positive");
        }
        this.blockLimitItems = blockLimitItems;
    }

    void append(T value, long estimate) {
        ensureWritable();
        positive(estimate, "estimate");
        try {
            estimatedBytes = Math.addExact(estimatedBytes, estimate);
            size = Math.addExact(size, 1);
        } catch (ArithmeticException error) {
            throw new ConnectException("Transaction change accounting overflow", error);
        }

        if (channel == null && residentBytes + estimate <= residentLimitBytes) {
            memory.add(value);
            memoryEstimates.add(estimate);
            residentBytes += estimate;
            return;
        }
        if (channel == null) {
            openSpillFile();
            for (int index = 0; index < memory.size(); index++) {
                appendPending(memory.get(index), memoryEstimates.get(index));
            }
            memory.clear();
            memoryEstimates.clear();
            residentBytes = 0L;
        }
        appendPending(value, estimate);
    }

    void seal() {
        if (closed) {
            throw new ConnectException("Transaction change store is closed");
        }
        if (!sealed && channel != null) {
            flushPending();
            try {
                channel.force(false);
            } catch (IOException error) {
                throw storageFailure("Cannot flush transaction spill file", error);
            }
        }
        sealed = true;
    }

    /**
     * Discards an append-only suffix while the list is still writable.
     * Streamed PostgreSQL transactions use this for ROLLBACK TO SAVEPOINT:
     * the protocol identifies the first aborted subtransaction and every
     * change after that marker must disappear, including changes already
     * written to the spill file.
     */
    void truncate(int newSize) {
        ensureWritable();
        if (newSize < 0 || newSize > size) {
            throw new IndexOutOfBoundsException(newSize);
        }
        if (newSize == size) {
            return;
        }

        if (channel == null) {
            truncateTail(memory, newSize);
            truncateTail(memoryEstimates, newSize);
            residentBytes = sum(memoryEstimates);
            estimatedBytes = residentBytes;
            size = newSize;
            return;
        }

        cache = null;
        int persistedSize = blocks.isEmpty() ? 0 : blocks.getLast().toIndex();
        if (newSize >= persistedSize) {
            int pendingSize = newSize - persistedSize;
            truncateTail(pending, pendingSize);
            truncateTail(pendingEstimates, pendingSize);
            pendingBytes = sum(pendingEstimates);
            estimatedBytes = persistedEstimate() + pendingBytes;
            size = newSize;
            return;
        }

        pending.clear();
        pendingEstimates.clear();
        pendingBytes = 0L;

        Block partial = null;
        for (Block block : blocks) {
            if (newSize > block.fromIndex() && newSize < block.toIndex()) {
                partial = block;
                break;
            }
        }
        List<T> retainedPartial = List.of();
        List<Long> retainedEstimates = List.of();
        long nextWritePosition;
        if (partial != null) {
            int retained = newSize - partial.fromIndex();
            retainedPartial = List.copyOf(readBlock(partial).subList(0, retained));
            retainedEstimates = List.copyOf(partial.estimates().subList(0, retained));
            nextWritePosition = partial.offset();
        } else {
            nextWritePosition = 0L;
            for (Block block : blocks) {
                if (block.toIndex() <= newSize) {
                    nextWritePosition = block.endOffset();
                }
            }
        }

        final int retainedBlockBoundary = partial == null ? newSize : partial.fromIndex();
        blocks.removeIf(block -> block.fromIndex() >= retainedBlockBoundary);
        try {
            channel.truncate(nextWritePosition);
        } catch (IOException error) {
            throw storageFailure("Cannot truncate transaction spill file", error);
        }
        writePosition = nextWritePosition;
        pending.addAll(retainedPartial);
        pendingEstimates.addAll(retainedEstimates);
        pendingBytes = sum(pendingEstimates);
        estimatedBytes = persistedEstimate() + pendingBytes;
        size = newSize;
    }

    boolean spilled() {
        return channel != null;
    }

    long estimatedBytes() {
        return estimatedBytes;
    }

    long estimatedBytes(int fromIndex, int toIndex) {
        checkRange(fromIndex, toIndex);
        if (fromIndex == toIndex) {
            return 0L;
        }
        if (channel == null) {
            long total = 0L;
            for (int index = fromIndex; index < toIndex; index++) {
                total = Math.addExact(total, memoryEstimates.get(index));
            }
            return total;
        }
        seal();
        long total = 0L;
        for (Block block : blocks) {
            if (block.toIndex() <= fromIndex || block.fromIndex() >= toIndex) {
                continue;
            }
            if (block.fromIndex() >= fromIndex && block.toIndex() <= toIndex) {
                total = Math.addExact(total, block.estimatedBytes());
            } else {
                int start = Math.max(fromIndex, block.fromIndex());
                int end = Math.min(toIndex, block.toIndex());
                for (int index = start; index < end; index++) {
                    total = Math.addExact(total,
                            block.estimates().get(index - block.fromIndex()));
                }
            }
        }
        return total;
    }

    List<Range> ranges(int fromIndex, int toIndex) {
        checkRange(fromIndex, toIndex);
        if (fromIndex == toIndex) {
            return List.of();
        }
        seal();
        List<Range> ranges = new ArrayList<>();
        if (channel == null) {
            int start = fromIndex;
            int count = 0;
            long bytes = 0L;
            for (int index = fromIndex; index < toIndex; index++) {
                long estimate = memoryEstimates.get(index);
                if (count > 0 && (count >= blockLimitItems
                        || bytes + estimate > blockLimitBytes)) {
                    ranges.add(new Range(start, index, bytes));
                    start = index;
                    count = 0;
                    bytes = 0L;
                }
                count++;
                bytes = Math.addExact(bytes, estimate);
            }
            ranges.add(new Range(start, toIndex, bytes));
            return List.copyOf(ranges);
        }
        for (Block block : blocks) {
            int start = Math.max(fromIndex, block.fromIndex());
            int end = Math.min(toIndex, block.toIndex());
            if (start < end) {
                ranges.add(new Range(start, end,
                        start == block.fromIndex() && end == block.toIndex()
                                ? block.estimatedBytes() : estimatedBytes(start, end)));
            }
        }
        return List.copyOf(ranges);
    }

    List<T> readRange(Range range) {
        checkRange(range.fromIndex(), range.toIndex());
        if (channel == null) {
            return List.copyOf(memory.subList(range.fromIndex(), range.toIndex()));
        }
        seal();
        Block block = findBlock(range.fromIndex());
        if (range.toIndex() > block.toIndex()) {
            throw new IllegalArgumentException("A read range must not span spill blocks");
        }
        List<T> values = readBlock(block);
        return List.copyOf(values.subList(
                range.fromIndex() - block.fromIndex(),
                range.toIndex() - block.fromIndex()));
    }

    @Override
    public T get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException(index);
        }
        if (closed) {
            throw new ConnectException("Transaction change store is closed");
        }
        if (channel == null) {
            return memory.get(index);
        }
        seal();
        Block block = findBlock(index);
        CachedBlock<T> cached = cache;
        if (cached == null || cached.block() != block) {
            synchronized (this) {
                cached = cache;
                if (cached == null || cached.block() != block) {
                    cached = new CachedBlock<>(block, readBlock(block));
                    cache = cached;
                }
            }
        }
        return cached.values().get(index - block.fromIndex());
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        cache = null;
        memory.clear();
        memoryEstimates.clear();
        pending.clear();
        pendingEstimates.clear();
        blocks.clear();
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
                // Best effort continues with the explicit path removal below.
            }
            channel = null;
        }
        if (path != null) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ignored) {
                // DELETE_ON_CLOSE is the primary crash-safe cleanup mechanism.
            }
            path = null;
        }
    }

    private void appendPending(T value, long estimate) {
        if (!pending.isEmpty() && (pending.size() >= blockLimitItems
                || pendingBytes + estimate > blockLimitBytes)) {
            flushPending();
        }
        pending.add(value);
        pendingEstimates.add(estimate);
        pendingBytes = Math.addExact(pendingBytes, estimate);
        if (pending.size() >= blockLimitItems || pendingBytes >= blockLimitBytes) {
            flushPending();
        }
    }

    private void flushPending() {
        if (pending.isEmpty()) {
            return;
        }
        int fromIndex = blocks.isEmpty() ? 0 : blocks.getLast().toIndex();
        long blockOffset = writePosition;
        for (T value : pending) {
            byte[] encoded = encode(value);
            ByteBuffer header = ByteBuffer.allocate(Integer.BYTES).putInt(encoded.length).flip();
            writeFully(header, writePosition);
            writePosition += Integer.BYTES;
            writeFully(ByteBuffer.wrap(encoded), writePosition);
            writePosition += encoded.length;
        }
        blocks.add(new Block(blockOffset, writePosition, fromIndex,
                fromIndex + pending.size(), pendingBytes,
                List.copyOf(pendingEstimates)));
        pending.clear();
        pendingEstimates.clear();
        pendingBytes = 0L;
    }

    private void openSpillFile() {
        try {
            Files.createDirectories(directory);
            path = Files.createTempFile(directory, "transaction-", ".wal");
            channel = FileChannel.open(path, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE);
        } catch (IOException error) {
            throw storageFailure("Cannot create transaction spill file in " + directory, error);
        }
    }

    private byte[] encode(T value) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             DataOutputStream output = new DataOutputStream(bytes)) {
            codec.write(output, value);
            output.flush();
            return bytes.toByteArray();
        } catch (IOException error) {
            throw storageFailure("Cannot encode spilled transaction change", error);
        }
    }

    private List<T> readBlock(Block block) {
        long length = block.endOffset() - block.offset();
        if (length > Integer.MAX_VALUE) {
            throw new ConnectException("Transaction spill block exceeds the Java buffer limit");
        }
        ByteBuffer encoded = ByteBuffer.allocate(Math.toIntExact(length));
        readFully(encoded, block.offset());
        encoded.flip();
        List<T> values = new ArrayList<>(block.toIndex() - block.fromIndex());
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(encoded.array(), 0, encoded.limit()))) {
            while (input.available() > 0) {
                int itemLength = input.readInt();
                if (itemLength < 0 || itemLength > input.available()) {
                    throw new EOFException("Invalid transaction spill record length " + itemLength);
                }
                byte[] item = input.readNBytes(itemLength);
                try (DataInputStream itemInput = new DataInputStream(
                        new ByteArrayInputStream(item))) {
                    values.add(codec.read(itemInput));
                    if (itemInput.available() != 0) {
                        throw new IOException("Trailing bytes in transaction spill record");
                    }
                }
            }
        } catch (IOException error) {
            throw storageFailure("Cannot decode transaction spill block", error);
        }
        if (values.size() != block.toIndex() - block.fromIndex()) {
            throw new ConnectException("Transaction spill block item count mismatch");
        }
        return List.copyOf(values);
    }

    private Block findBlock(int index) {
        int low = 0;
        int high = blocks.size() - 1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            Block block = blocks.get(middle);
            if (index < block.fromIndex()) {
                high = middle - 1;
            } else if (index >= block.toIndex()) {
                low = middle + 1;
            } else {
                return block;
            }
        }
        throw new ConnectException("Missing transaction spill block for item " + index);
    }

    private void writeFully(ByteBuffer buffer, long position) {
        try {
            while (buffer.hasRemaining()) {
                int written = channel.write(buffer, position);
                if (written < 0) {
                    throw new EOFException("Transaction spill file closed during write");
                }
                position += written;
            }
        } catch (IOException error) {
            throw storageFailure("Cannot write transaction spill file", error);
        }
    }

    private void readFully(ByteBuffer buffer, long position) {
        try {
            while (buffer.hasRemaining()) {
                int read = channel.read(buffer, position);
                if (read < 0) {
                    throw new EOFException("Unexpected end of transaction spill file");
                }
                position += read;
            }
        } catch (IOException error) {
            throw storageFailure("Cannot read transaction spill file", error);
        }
    }

    private void ensureWritable() {
        if (closed) {
            throw new ConnectException("Transaction change store is closed");
        }
        if (sealed) {
            throw new ConnectException("Transaction change store is already sealed");
        }
    }

    private long persistedEstimate() {
        long total = 0L;
        for (Block block : blocks) {
            total = Math.addExact(total, block.estimatedBytes());
        }
        return total;
    }

    private static long sum(List<Long> values) {
        long total = 0L;
        for (long value : values) {
            total = Math.addExact(total, value);
        }
        return total;
    }

    private static <V> void truncateTail(List<V> values, int retained) {
        values.subList(retained, values.size()).clear();
    }

    private void checkRange(int fromIndex, int toIndex) {
        if (fromIndex < 0 || toIndex < fromIndex || toIndex > size) {
            throw new IndexOutOfBoundsException(fromIndex + ".." + toIndex);
        }
    }

    private static long positive(long value, String name) {
        if (value < 1L) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static ConnectException storageFailure(String message, IOException error) {
        return new ConnectException(message, error);
    }

    private record Block(long offset, long endOffset, int fromIndex, int toIndex,
                         long estimatedBytes, List<Long> estimates) {}

    private record CachedBlock<T>(Block block, List<T> values) {}
}
