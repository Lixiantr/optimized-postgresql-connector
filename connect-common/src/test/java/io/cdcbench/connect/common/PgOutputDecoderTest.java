package io.cdcbench.connect.common;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PgOutputDecoderTest {
    private final PgOutputDecoder decoder = new PgOutputDecoder();

    @Test
    void decodesRelationAndKeyFlags() throws Exception {
        ByteArrayOutputStream bytes = message('R');
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(42);
            cString(out, "public");
            cString(out, "events");
            out.writeByte('d');
            out.writeShort(2);
            column(out, true, "id", 20, -1);
            column(out, false, "payload", 3802, -1);
        }

        PgOutputMessage.Relation relation = assertInstanceOf(
                PgOutputMessage.Relation.class, decoder.decode(ByteBuffer.wrap(bytes.toByteArray())));
        assertEquals(42L, relation.relationId());
        assertEquals("public.events", relation.qualifiedName());
        assertEquals(2, relation.columns().size());
        assertEquals(true, relation.columns().getFirst().key());
        assertEquals(3802L, relation.columns().get(1).typeOid());
    }

    @Test
    void decodesInsertTuple() throws Exception {
        ByteArrayOutputStream bytes = message('I');
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(42);
            out.writeByte('N');
            out.writeShort(3);
            text(out, "100");
            out.writeByte('n');
            out.writeByte('u');
        }

        PgOutputMessage.Insert insert = assertInstanceOf(
                PgOutputMessage.Insert.class, decoder.decode(ByteBuffer.wrap(bytes.toByteArray())));
        assertEquals(42L, insert.relationId());
        assertEquals(3, insert.newTuple().values().size());
        assertEquals(PgOutputMessage.ValueKind.TEXT, insert.newTuple().values().getFirst().kind());
        assertEquals("100", new String(insert.newTuple().values().getFirst().data(), StandardCharsets.UTF_8));
        assertEquals(PgOutputMessage.ValueKind.NULL, insert.newTuple().values().get(1).kind());
        assertEquals(PgOutputMessage.ValueKind.UNCHANGED_TOAST, insert.newTuple().values().get(2).kind());
    }

    @Test
    void decodesStreamedInsertTransactionIdBeforeRelationId() throws Exception {
        ByteArrayOutputStream bytes = message('I');
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(4242);
            out.writeInt(42);
            out.writeByte('N');
            out.writeShort(1);
            text(out, "100");
        }

        PgOutputMessage.Insert insert = assertInstanceOf(
                PgOutputMessage.Insert.class,
                decoder.decode(ByteBuffer.wrap(bytes.toByteArray()), true));
        assertEquals(4242L, insert.streamedTransactionId());
        assertEquals(42L, insert.relationId());
    }

    @Test
    void doesNotConsumeAnXidPrefixFromOriginInsideAStream() throws Exception {
        ByteArrayOutputStream bytes = message('O');
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeLong(0x0102030405060708L);
            cString(out, "upstream");
        }

        PgOutputMessage.Origin origin = assertInstanceOf(
                PgOutputMessage.Origin.class,
                decoder.decode(ByteBuffer.wrap(bytes.toByteArray()), true));
        assertEquals(0x0102030405060708L, origin.originLsn());
        assertEquals("upstream", origin.name());
        assertEquals(null, origin.streamedTransactionId());
    }

    @Test
    void decodesUpdateWithOldKeyTuple() throws Exception {
        ByteArrayOutputStream bytes = message('U');
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(42);
            out.writeByte('K');
            out.writeShort(1);
            text(out, "100");
            out.writeByte('N');
            out.writeShort(2);
            text(out, "101");
            text(out, "updated");
        }

        PgOutputMessage.Update update = assertInstanceOf(
                PgOutputMessage.Update.class, decoder.decode(ByteBuffer.wrap(bytes.toByteArray())));
        assertEquals('K', update.oldTupleKind());
        assertEquals(1, update.oldTuple().values().size());
        assertEquals(2, update.newTuple().values().size());
    }

    @Test
    void rejectsTruncatedFieldInsteadOfReadingPastBuffer() {
        ByteBuffer truncated = ByteBuffer.wrap(new byte[]{'I', 0, 0, 0, 42, 'N', 0, 1, 't', 0, 0, 0, 10, 1});
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> decoder.decode(truncated));
        assertEquals(true, error.getMessage().contains("truncated text tuple value"));
    }

    @Test
    void decodesStreamingProtocolBoundaries() throws Exception {
        ByteArrayOutputStream startBytes = message('S');
        try (DataOutputStream out = new DataOutputStream(startBytes)) {
            out.writeInt(-1);
            out.writeByte(1);
        }
        PgOutputMessage.StreamStart start = assertInstanceOf(PgOutputMessage.StreamStart.class,
                decoder.decode(ByteBuffer.wrap(startBytes.toByteArray())));
        assertEquals(4_294_967_295L, start.transactionId());
        assertEquals(true, start.firstSegment());

        PgOutputMessage.StreamStop stop = assertInstanceOf(PgOutputMessage.StreamStop.class,
                decoder.decode(ByteBuffer.wrap(new byte[]{'E'})));
        assertEquals(new PgOutputMessage.StreamStop(), stop);

        ByteArrayOutputStream commitBytes = message('c');
        try (DataOutputStream out = new DataOutputStream(commitBytes)) {
            out.writeInt(7);
            out.writeByte(0);
            out.writeLong(100);
            out.writeLong(110);
            out.writeLong(120);
        }
        PgOutputMessage.StreamCommit commit = assertInstanceOf(PgOutputMessage.StreamCommit.class,
                decoder.decode(ByteBuffer.wrap(commitBytes.toByteArray())));
        assertEquals(7L, commit.transactionId());
        assertEquals(110L, commit.endLsn());

        ByteArrayOutputStream abortV2Bytes = message('A');
        try (DataOutputStream out = new DataOutputStream(abortV2Bytes)) {
            out.writeInt(7);
            out.writeInt(8);
        }
        PgOutputMessage.StreamAbort abortV2 = assertInstanceOf(PgOutputMessage.StreamAbort.class,
                decoder.decode(ByteBuffer.wrap(abortV2Bytes.toByteArray())));
        assertEquals(null, abortV2.abortLsn());

        ByteArrayOutputStream abortV4Bytes = message('A');
        try (DataOutputStream out = new DataOutputStream(abortV4Bytes)) {
            out.writeInt(7);
            out.writeInt(8);
            out.writeLong(130);
            out.writeLong(140);
        }
        PgOutputMessage.StreamAbort abortV4 = assertInstanceOf(PgOutputMessage.StreamAbort.class,
                decoder.decode(ByteBuffer.wrap(abortV4Bytes.toByteArray())));
        assertEquals(130L, abortV4.abortLsn());
        assertEquals(140L, abortV4.abortTimeMicros());
    }

    @Test
    void decodesTwoPhaseProtocolBoundaries() throws Exception {
        PgOutputMessage.BeginPrepare begin = assertInstanceOf(PgOutputMessage.BeginPrepare.class,
                decoder.decode(preparedMessage('b', false, false)));
        assertEquals(10L, begin.prepareLsn());
        assertEquals("gid-1", begin.gid());

        PgOutputMessage.Prepare prepare = assertInstanceOf(PgOutputMessage.Prepare.class,
                decoder.decode(preparedMessage('P', true, false)));
        assertEquals(20L, prepare.endLsn());

        PgOutputMessage.CommitPrepared commit = assertInstanceOf(PgOutputMessage.CommitPrepared.class,
                decoder.decode(preparedMessage('K', true, false)));
        assertEquals(30L, commit.commitTimeMicros());

        PgOutputMessage.StreamPrepare stream = assertInstanceOf(PgOutputMessage.StreamPrepare.class,
                decoder.decode(preparedMessage('p', true, false)));
        assertEquals(40L, stream.transactionId());

        PgOutputMessage.RollbackPrepared rollback = assertInstanceOf(
                PgOutputMessage.RollbackPrepared.class,
                decoder.decode(preparedMessage('r', true, true)));
        assertEquals(35L, rollback.rollbackTimeMicros());
        assertEquals("gid-1", rollback.gid());
    }

    @Test
    void rejectsInvalidFlagsUtf8AndTrailingBytes() throws Exception {
        IllegalArgumentException flags = assertThrows(IllegalArgumentException.class,
                () -> decoder.decode(ByteBuffer.wrap(new byte[]{'S', 0, 0, 0, 1, 2})));
        assertEquals(true, flags.getMessage().contains("first-segment flag"));

        ByteArrayOutputStream invalidUtf8 = message('Y');
        try (DataOutputStream out = new DataOutputStream(invalidUtf8)) {
            out.writeInt(1);
            out.write(new byte[]{(byte) 0xc3, (byte) 0x28, 0});
            cString(out, "type");
        }
        IllegalArgumentException utf8 = assertThrows(IllegalArgumentException.class,
                () -> decoder.decode(ByteBuffer.wrap(invalidUtf8.toByteArray())));
        assertEquals(true, utf8.getMessage().contains("not valid UTF-8"));

        IllegalArgumentException trailing = assertThrows(IllegalArgumentException.class,
                () -> decoder.decode(ByteBuffer.wrap(new byte[]{'E', 1})));
        assertEquals(true, trailing.getMessage().contains("trailing bytes"));
    }

    private static ByteArrayOutputStream message(char tag) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(tag);
        return bytes;
    }

    private static ByteBuffer preparedMessage(char tag, boolean flags, boolean rollback)
            throws IOException {
        ByteArrayOutputStream bytes = message(tag);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            if (flags) {
                out.writeByte(0);
            }
            out.writeLong(10);
            out.writeLong(20);
            out.writeLong(30);
            if (rollback) {
                out.writeLong(35);
            }
            out.writeInt(40);
            cString(out, "gid-1");
        }
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    private static void cString(DataOutputStream out, String value) throws IOException {
        out.write(value.getBytes(StandardCharsets.UTF_8));
        out.writeByte(0);
    }

    private static void column(DataOutputStream out, boolean key, String name, int oid, int modifier)
            throws IOException {
        out.writeByte(key ? 1 : 0);
        cString(out, name);
        out.writeInt(oid);
        out.writeInt(modifier);
    }

    private static void text(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeByte('t');
        out.writeInt(bytes.length);
        out.write(bytes);
    }
}
