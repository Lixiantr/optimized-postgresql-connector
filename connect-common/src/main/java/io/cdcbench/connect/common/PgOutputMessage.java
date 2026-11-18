package io.cdcbench.connect.common;

import java.util.List;

public sealed interface PgOutputMessage {
    record Begin(long finalLsn, long commitTimeMicros, long transactionId) implements PgOutputMessage {}

    record Commit(int flags, long commitLsn, long endLsn, long commitTimeMicros)
            implements PgOutputMessage {}

    record Relation(Long streamedTransactionId, long relationId,
                    String namespace, String name, int replicaIdentity,
                    List<Column> columns) implements PgOutputMessage {
        public Relation(long relationId, String namespace, String name, int replicaIdentity,
                        List<Column> columns) {
            this(null, relationId, namespace, name, replicaIdentity, columns);
        }

        public String qualifiedName() {
            return namespace + "." + name;
        }
    }

    record Type(Long streamedTransactionId, long typeId, String namespace, String name)
            implements PgOutputMessage {
        public Type(long typeId, String namespace, String name) {
            this(null, typeId, namespace, name);
        }
    }

    record Insert(Long streamedTransactionId, long relationId, TupleData newTuple)
            implements PgOutputMessage {
        public Insert(long relationId, TupleData newTuple) {
            this(null, relationId, newTuple);
        }
    }

    record Update(Long streamedTransactionId, long relationId, Character oldTupleKind,
                  TupleData oldTuple, TupleData newTuple) implements PgOutputMessage {
        public Update(long relationId, Character oldTupleKind, TupleData oldTuple,
                      TupleData newTuple) {
            this(null, relationId, oldTupleKind, oldTuple, newTuple);
        }
    }

    record Delete(Long streamedTransactionId, long relationId, char oldTupleKind,
                  TupleData oldTuple) implements PgOutputMessage {
        public Delete(long relationId, char oldTupleKind, TupleData oldTuple) {
            this(null, relationId, oldTupleKind, oldTuple);
        }
    }

    record Truncate(Long streamedTransactionId, int options, List<Long> relationIds)
            implements PgOutputMessage {
        public Truncate(int options, List<Long> relationIds) {
            this(null, options, relationIds);
        }
    }

    record Origin(Long streamedTransactionId, long originLsn, String name)
            implements PgOutputMessage {
        public Origin(long originLsn, String name) {
            this(null, originLsn, name);
        }
    }

    record LogicalMessage(Long streamedTransactionId, boolean transactional, long lsn,
                          String prefix, byte[] content) implements PgOutputMessage {
        public LogicalMessage(boolean transactional, long lsn, String prefix, byte[] content) {
            this(null, transactional, lsn, prefix, content);
        }
    }

    record StreamStart(long transactionId, boolean firstSegment) implements PgOutputMessage {}

    record StreamStop() implements PgOutputMessage {}

    record StreamCommit(long transactionId, int flags, long commitLsn, long endLsn,
                        long commitTimeMicros) implements PgOutputMessage {}

    record StreamAbort(long transactionId, long subtransactionId, Long abortLsn,
                       Long abortTimeMicros) implements PgOutputMessage {}

    record BeginPrepare(long prepareLsn, long endLsn, long prepareTimeMicros,
                        long transactionId, String gid) implements PgOutputMessage {}

    record Prepare(int flags, long prepareLsn, long endLsn, long prepareTimeMicros,
                   long transactionId, String gid) implements PgOutputMessage {}

    record CommitPrepared(int flags, long commitLsn, long endLsn, long commitTimeMicros,
                          long transactionId, String gid) implements PgOutputMessage {}

    record RollbackPrepared(int flags, long prepareEndLsn, long rollbackEndLsn,
                            long prepareTimeMicros, long rollbackTimeMicros,
                            long transactionId, String gid) implements PgOutputMessage {}

    record StreamPrepare(int flags, long prepareLsn, long endLsn, long prepareTimeMicros,
                         long transactionId, String gid) implements PgOutputMessage {}

    record Column(boolean key, String name, long typeOid, int typeModifier) {}

    enum ValueKind {
        NULL,
        UNCHANGED_TOAST,
        TEXT,
        BINARY
    }

    record TupleValue(ValueKind kind, byte[] data) {
        public static TupleValue nullValue() {
            return new TupleValue(ValueKind.NULL, null);
        }

        public static TupleValue unchangedToast() {
            return new TupleValue(ValueKind.UNCHANGED_TOAST, null);
        }
    }

    record TupleData(List<TupleValue> values) {}
}
