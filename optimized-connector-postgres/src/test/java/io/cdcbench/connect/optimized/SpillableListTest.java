package io.cdcbench.connect.optimized;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SpillableListTest {
    @TempDir
    Path directory;

    @Test
    void spillsIntoBoundedOrderedRangesAndDeletesStorage() throws Exception {
        SpillableList.Codec<String> codec = new SpillableList.Codec<>() {
            @Override
            public void write(DataOutput output, String value) throws IOException {
                output.writeUTF(value);
            }

            @Override
            public String read(DataInput input) throws IOException {
                return input.readUTF();
            }
        };

        try (SpillableList<String> values = new SpillableList<>(
                codec, directory, 10L, 12L, 2)) {
            values.append("zero", 6L);
            values.append("one", 6L);
            values.append("two", 6L);
            values.append("three", 6L);
            values.seal();

            assertTrue(values.spilled());
            assertEquals(4, values.size());
            assertEquals(24L, values.estimatedBytes());
            assertEquals(List.of("zero", "one", "two", "three"),
                    List.of(values.get(0), values.get(1), values.get(2), values.get(3)));
            assertEquals(List.of(
                            new SpillableList.Range(0, 2, 12L),
                            new SpillableList.Range(2, 4, 12L)),
                    values.ranges(0, 4));
            assertEquals(List.of(
                            new SpillableList.Range(1, 2, 6L),
                            new SpillableList.Range(2, 3, 6L)),
                    values.ranges(1, 3));
            // On Unix DELETE_ON_CLOSE unlinks the open file immediately; reading it
            // through the channel above proves its lifetime without relying on
            // platform-specific directory visibility.
        }

        try (var files = Files.list(directory)) {
            assertTrue(files.toList().isEmpty());
        }
    }

    @Test
    void keepsSmallTransactionsInMemory() {
        SpillableList.Codec<Integer> codec = new SpillableList.Codec<>() {
            public void write(DataOutput output, Integer value) throws IOException {
                output.writeInt(value);
            }

            public Integer read(DataInput input) throws IOException {
                return input.readInt();
            }
        };
        try (SpillableList<Integer> values = new SpillableList<>(
                codec, directory, 1024L, 128L, 4)) {
            values.append(1, 10L);
            values.append(2, 10L);
            values.seal();
            assertFalse(values.spilled());
            assertEquals(List.of(1, 2), List.of(values.get(0), values.get(1)));
        }
    }

    @Test
    void truncatesAnInMemorySuffixAndAcceptsNewValues() {
        SpillableList.Codec<Integer> codec = integerCodec();
        try (SpillableList<Integer> values = new SpillableList<>(
                codec, directory, 1024L, 128L, 4)) {
            values.append(1, 10L);
            values.append(2, 20L);
            values.append(3, 30L);

            values.truncate(1);
            values.append(4, 40L);
            values.seal();

            assertEquals(2, values.size());
            assertEquals(50L, values.estimatedBytes());
            assertEquals(List.of(1, 4), List.of(values.get(0), values.get(1)));
        }
    }

    @Test
    void truncatesPersistedAndPendingSpillDataAndAcceptsNewValues() {
        SpillableList.Codec<Integer> codec = integerCodec();
        try (SpillableList<Integer> values = new SpillableList<>(
                codec, directory, 1L, 20L, 2)) {
            values.append(1, 10L);
            values.append(2, 10L);
            values.append(3, 10L);
            values.append(4, 10L);
            values.append(5, 10L);

            values.truncate(3);
            values.append(6, 15L);
            values.seal();

            assertTrue(values.spilled());
            assertEquals(4, values.size());
            assertEquals(45L, values.estimatedBytes());
            assertEquals(List.of(1, 2, 3, 6),
                    List.of(values.get(0), values.get(1), values.get(2), values.get(3)));
        }
    }

    @Test
    void truncatesAtACompletedSpillBlockBoundary() {
        SpillableList.Codec<Integer> codec = integerCodec();
        try (SpillableList<Integer> values = new SpillableList<>(
                codec, directory, 1L, 20L, 2)) {
            for (int value = 1; value <= 6; value++) {
                values.append(value, 10L);
            }

            values.truncate(2);
            values.append(7, 10L);
            values.seal();

            assertEquals(List.of(1, 2, 7),
                    List.of(values.get(0), values.get(1), values.get(2)));
            assertEquals(30L, values.estimatedBytes());
        }
    }

    private static SpillableList.Codec<Integer> integerCodec() {
        return new SpillableList.Codec<>() {
            public void write(DataOutput output, Integer value) throws IOException {
                output.writeInt(value);
            }

            public Integer read(DataInput input) throws IOException {
                return input.readInt();
            }
        };
    }
}
