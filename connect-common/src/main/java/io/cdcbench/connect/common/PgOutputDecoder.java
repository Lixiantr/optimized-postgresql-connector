package io.cdcbench.connect.common;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.cdcbench.connect.common.PgOutputMessage.*;

public final class PgOutputDecoder {
    private static final int MAX_FIELD_BYTES = 256 * 1024 * 1024;

    public PgOutputMessage decode(ByteBuffer source) {
        return decode(source, false);
    }

    public PgOutputMessage decode(ByteBuffer source, boolean streamedPayload) {
        ByteBuffer input = source.slice().order(ByteOrder.BIG_ENDIAN);
        require(input, 1, "message tag");
        char tag = (char) Byte.toUnsignedInt(input.get());

        PgOutputMessage message = switch (tag) {
            case 'B' -> decodeBegin(input);
            case 'C' -> decodeCommit(input);
            case 'R' -> decodeRelation(input, streamedPayload);
            case 'Y' -> decodeType(input, streamedPayload);
            case 'I' -> decodeInsert(input, streamedPayload);
            case 'U' -> decodeUpdate(input, streamedPayload);
            case 'D' -> decodeDelete(input, streamedPayload);
            case 'T' -> decodeTruncate(input, streamedPayload);
            // ORIGIN is emitted at a stream boundary and, unlike relation/type/
            // DML/message payloads, never carries the streamed subtransaction XID.
            case 'O' -> decodeOrigin(input);
            case 'M' -> decodeLogicalMessage(input, streamedPayload);
            case 'S' -> decodeStreamStart(input);
            case 'E' -> new StreamStop();
            case 'c' -> decodeStreamCommit(input);
            case 'A' -> decodeStreamAbort(input);
            case 'b' -> decodeBeginPrepare(input);
            case 'P' -> decodePrepare(input);
            case 'K' -> decodeCommitPrepared(input);
            case 'r' -> decodeRollbackPrepared(input);
            case 'p' -> decodeStreamPrepare(input);
            default -> throw malformed("unsupported pgoutput message tag '" + tag + "'");
        };

        if (input.hasRemaining()) {
            throw malformed("message '" + tag + "' has " + input.remaining() + " trailing bytes");
        }
        return message;
    }

    private static Begin decodeBegin(ByteBuffer input) {
        require(input, 20, "BEGIN");
        return new Begin(input.getLong(), input.getLong(), unsignedInt(input));
    }

    private static Commit decodeCommit(ByteBuffer input) {
        require(input, 25, "COMMIT");
        int flags = zeroFlags(input, "COMMIT");
        return new Commit(flags, input.getLong(), input.getLong(), input.getLong());
    }

    private static Relation decodeRelation(ByteBuffer input, boolean streamedPayload) {
        Long streamedTransactionId = streamedTransactionId(input, streamedPayload);
        long relationId = unsignedInt(input);
        String namespace = cString(input, "relation namespace");
        String name = cString(input, "relation name");
        require(input, 3, "relation metadata");
        int replicaIdentity = Byte.toUnsignedInt(input.get());
        if (replicaIdentity != 'd' && replicaIdentity != 'n'
                && replicaIdentity != 'f' && replicaIdentity != 'i') {
            throw malformed("invalid replica identity '" + (char) replicaIdentity + "'");
        }
        int columnCount = Short.toUnsignedInt(input.getShort());
        List<Column> columns = new ArrayList<>(columnCount);
        for (int index = 0; index < columnCount; index++) {
            require(input, 1, "column flags");
            int flags = Byte.toUnsignedInt(input.get());
            if ((flags & ~0x01) != 0) {
                throw malformed("invalid column flags " + flags);
            }
            boolean key = (flags & 0x01) != 0;
            String columnName = cString(input, "column name");
            require(input, 8, "column type");
            columns.add(new Column(key, columnName, unsignedInt(input), input.getInt()));
        }
        return new Relation(streamedTransactionId, relationId, namespace, name,
                replicaIdentity, List.copyOf(columns));
    }

    private static Type decodeType(ByteBuffer input, boolean streamedPayload) {
        Long streamedTransactionId = streamedTransactionId(input, streamedPayload);
        return new Type(streamedTransactionId, unsignedInt(input),
                cString(input, "type namespace"), cString(input, "type name"));
    }

    private static Insert decodeInsert(ByteBuffer input, boolean streamedPayload) {
        Long streamedTransactionId = streamedTransactionId(input, streamedPayload);
        long relationId = unsignedInt(input);
        expect(input, 'N', "INSERT new tuple");
        return new Insert(streamedTransactionId, relationId, tuple(input));
    }

    private static Update decodeUpdate(ByteBuffer input, boolean streamedPayload) {
        Long streamedTransactionId = streamedTransactionId(input, streamedPayload);
        long relationId = unsignedInt(input);
        require(input, 1, "UPDATE tuple tag");
        char tupleTag = (char) Byte.toUnsignedInt(input.get());
        Character oldKind = null;
        TupleData oldTuple = null;
        if (tupleTag == 'K' || tupleTag == 'O') {
            oldKind = tupleTag;
            oldTuple = tuple(input);
            require(input, 1, "UPDATE new tuple tag");
            tupleTag = (char) Byte.toUnsignedInt(input.get());
        }
        if (tupleTag != 'N') {
            throw malformed("UPDATE expected new tuple tag 'N', got '" + tupleTag + "'");
        }
        return new Update(streamedTransactionId, relationId, oldKind, oldTuple, tuple(input));
    }

    private static Delete decodeDelete(ByteBuffer input, boolean streamedPayload) {
        Long streamedTransactionId = streamedTransactionId(input, streamedPayload);
        long relationId = unsignedInt(input);
        require(input, 1, "DELETE old tuple tag");
        char oldKind = (char) Byte.toUnsignedInt(input.get());
        if (oldKind != 'K' && oldKind != 'O') {
            throw malformed("DELETE expected old tuple tag 'K' or 'O', got '" + oldKind + "'");
        }
        return new Delete(streamedTransactionId, relationId, oldKind, tuple(input));
    }

    private static Truncate decodeTruncate(ByteBuffer input, boolean streamedPayload) {
        Long streamedTransactionId = streamedTransactionId(input, streamedPayload);
        require(input, 5, "TRUNCATE");
        int relationCount = input.getInt();
        if (relationCount < 0 || relationCount > 1_000_000) {
            throw malformed("invalid TRUNCATE relation count " + relationCount);
        }
        int options = Byte.toUnsignedInt(input.get());
        if ((options & ~0x03) != 0) {
            throw malformed("invalid TRUNCATE options " + options);
        }
        require(input, Math.multiplyExact(relationCount, 4), "TRUNCATE relation ids");
        List<Long> relationIds = new ArrayList<>(relationCount);
        for (int index = 0; index < relationCount; index++) {
            relationIds.add(unsignedInt(input));
        }
        return new Truncate(streamedTransactionId, options, List.copyOf(relationIds));
    }

    private static Origin decodeOrigin(ByteBuffer input) {
        require(input, 8, "ORIGIN LSN");
        return new Origin(input.getLong(), cString(input, "origin name"));
    }

    private static LogicalMessage decodeLogicalMessage(ByteBuffer input,
                                                       boolean streamedPayload) {
        Long streamedTransactionId = streamedTransactionId(input, streamedPayload);
        require(input, 9, "logical message");
        int flags = Byte.toUnsignedInt(input.get());
        if (flags > 1) {
            throw malformed("invalid logical message flags " + flags);
        }
        boolean transactional = flags == 1;
        long lsn = input.getLong();
        String prefix = cString(input, "logical message prefix");
        byte[] content = sizedBytes(input, "logical message content");
        return new LogicalMessage(streamedTransactionId, transactional, lsn, prefix, content);
    }

    private static Long streamedTransactionId(ByteBuffer input, boolean streamedPayload) {
        return streamedPayload ? unsignedInt(input) : null;
    }

    private static StreamStart decodeStreamStart(ByteBuffer input) {
        require(input, 5, "STREAM START");
        long transactionId = unsignedInt(input);
        int firstSegment = Byte.toUnsignedInt(input.get());
        if (firstSegment > 1) {
            throw malformed("invalid STREAM START first-segment flag " + firstSegment);
        }
        return new StreamStart(transactionId, firstSegment == 1);
    }

    private static StreamCommit decodeStreamCommit(ByteBuffer input) {
        require(input, 29, "STREAM COMMIT");
        long transactionId = unsignedInt(input);
        int flags = zeroFlags(input, "STREAM COMMIT");
        return new StreamCommit(transactionId, flags, input.getLong(), input.getLong(), input.getLong());
    }

    private static StreamAbort decodeStreamAbort(ByteBuffer input) {
        require(input, 8, "STREAM ABORT");
        long transactionId = unsignedInt(input);
        long subtransactionId = unsignedInt(input);
        if (!input.hasRemaining()) {
            return new StreamAbort(transactionId, subtransactionId, null, null);
        }
        require(input, 16, "STREAM ABORT parallel fields");
        return new StreamAbort(transactionId, subtransactionId, input.getLong(), input.getLong());
    }

    private static BeginPrepare decodeBeginPrepare(ByteBuffer input) {
        require(input, 28, "BEGIN PREPARE");
        return new BeginPrepare(input.getLong(), input.getLong(), input.getLong(), unsignedInt(input),
                cString(input, "prepared transaction GID"));
    }

    private static Prepare decodePrepare(ByteBuffer input) {
        require(input, 29, "PREPARE");
        int flags = zeroFlags(input, "PREPARE");
        return new Prepare(flags, input.getLong(), input.getLong(), input.getLong(), unsignedInt(input),
                cString(input, "prepared transaction GID"));
    }

    private static CommitPrepared decodeCommitPrepared(ByteBuffer input) {
        require(input, 29, "COMMIT PREPARED");
        int flags = zeroFlags(input, "COMMIT PREPARED");
        return new CommitPrepared(flags, input.getLong(), input.getLong(), input.getLong(),
                unsignedInt(input), cString(input, "prepared transaction GID"));
    }

    private static RollbackPrepared decodeRollbackPrepared(ByteBuffer input) {
        require(input, 37, "ROLLBACK PREPARED");
        int flags = zeroFlags(input, "ROLLBACK PREPARED");
        return new RollbackPrepared(flags, input.getLong(), input.getLong(), input.getLong(),
                input.getLong(), unsignedInt(input), cString(input, "prepared transaction GID"));
    }

    private static StreamPrepare decodeStreamPrepare(ByteBuffer input) {
        require(input, 29, "STREAM PREPARE");
        int flags = zeroFlags(input, "STREAM PREPARE");
        return new StreamPrepare(flags, input.getLong(), input.getLong(), input.getLong(),
                unsignedInt(input), cString(input, "prepared transaction GID"));
    }

    private static TupleData tuple(ByteBuffer input) {
        require(input, 2, "tuple column count");
        int columnCount = Short.toUnsignedInt(input.getShort());
        List<TupleValue> values = new ArrayList<>(columnCount);
        for (int index = 0; index < columnCount; index++) {
            require(input, 1, "tuple value kind");
            char kind = (char) Byte.toUnsignedInt(input.get());
            values.add(switch (kind) {
                case 'n' -> TupleValue.nullValue();
                case 'u' -> TupleValue.unchangedToast();
                case 't' -> new TupleValue(ValueKind.TEXT, sizedBytes(input, "text tuple value"));
                case 'b' -> new TupleValue(ValueKind.BINARY, sizedBytes(input, "binary tuple value"));
                default -> throw malformed("unknown tuple value kind '" + kind + "'");
            });
        }
        return new TupleData(List.copyOf(values));
    }

    private static byte[] sizedBytes(ByteBuffer input, String field) {
        require(input, 4, field + " length");
        int length = input.getInt();
        if (length < 0 || length > MAX_FIELD_BYTES) {
            throw malformed("invalid " + field + " length " + length);
        }
        require(input, length, field);
        byte[] value = new byte[length];
        input.get(value);
        return value;
    }

    private static String cString(ByteBuffer input, String field) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(32);
        while (input.hasRemaining()) {
            byte value = input.get();
            if (value == 0) {
                try {
                    return StandardCharsets.UTF_8.newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .decode(ByteBuffer.wrap(bytes.toByteArray()))
                            .toString();
                } catch (CharacterCodingException error) {
                    throw malformed(field + " is not valid UTF-8");
                }
            }
            if (bytes.size() >= MAX_FIELD_BYTES) {
                throw malformed(field + " exceeds maximum length");
            }
            bytes.write(value);
        }
        throw malformed(field + " is not null terminated");
    }

    private static long unsignedInt(ByteBuffer input) {
        require(input, 4, "unsigned int");
        return Integer.toUnsignedLong(input.getInt());
    }

    private static int zeroFlags(ByteBuffer input, String field) {
        require(input, 1, field + " flags");
        int flags = Byte.toUnsignedInt(input.get());
        if (flags != 0) {
            throw malformed(field + " flags must be zero, got " + flags);
        }
        return flags;
    }

    private static void expect(ByteBuffer input, char expected, String field) {
        require(input, 1, field);
        char actual = (char) Byte.toUnsignedInt(input.get());
        if (actual != expected) {
            throw malformed(field + " expected '" + expected + "', got '" + actual + "'");
        }
    }

    private static void require(ByteBuffer input, int bytes, String field) {
        if (bytes < 0 || input.remaining() < bytes) {
            throw malformed("truncated " + field + ": need " + bytes + ", have " + input.remaining());
        }
    }

    private static IllegalArgumentException malformed(String message) {
        return new IllegalArgumentException("Malformed pgoutput message: " + message);
    }
}
