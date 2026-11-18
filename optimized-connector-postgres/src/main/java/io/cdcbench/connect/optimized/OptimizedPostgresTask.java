package io.cdcbench.connect.optimized;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cdcbench.connect.common.CandidateConfig;
import io.cdcbench.connect.common.ColumnMetadata;
import io.cdcbench.connect.common.DebeziumEnvelope;
import io.cdcbench.connect.common.PgOutputDecoder;
import io.cdcbench.connect.common.PgOutputMessage;
import io.cdcbench.connect.common.PostgresValueConverter;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.TransactionContext;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.ReplicationSlotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.cdcbench.connect.common.PgOutputMessage.*;
import static io.cdcbench.connect.common.PostgresValueConverter.UnchangedToast;

public final class OptimizedPostgresTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(OptimizedPostgresTask.class);
    private static final Object LOGICAL_MESSAGE_COLLECTION_IDENTITY = new Object();
    private static final String LOGICAL_MESSAGE_DATA_COLLECTION =
            "io.debezium.connector.postgresql.LogicalDecodingMessageMonitor$"
                    + "LogicalDecodingMessageId@"
                    + Integer.toHexString(System.identityHashCode(
                    LOGICAL_MESSAGE_COLLECTION_IDENTITY));
    private static final long RECONNECT_BACKOFF_MILLIS = 1_000L;
    private static final int ASYNC_READER_THRESHOLD_BYTES = 64 * 1024;
    private static final int RAW_UTF8_THRESHOLD_BYTES = 8 * 1024;
    private static final long POSTGRES_TEXT_OID = 25L;
    private static final long POSTGRES_DATE_OID = 1082L;
    private static final long POSTGRES_TIME_OID = 1083L;
    private static final long POSTGRES_TIMESTAMP_OID = 1114L;
    private static final long POSTGRES_TIMESTAMPTZ_OID = 1184L;
    private static final long POSTGRES_TIMETZ_OID = 1266L;
    private static final long TRANSACTION_METADATA_BYTES = 1_024L;
    private static final ObjectMapper OFFSET_JSON = new ObjectMapper()
            .enable(DeserializationFeature.USE_LONG_FOR_INTS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private final PgOutputDecoder decoder = new PgOutputDecoder();
    private PostgresValueConverter valueConverter;
    private DebeziumSchemaFactory schemaFactory;
    private final ParallelJsonSerializer jsonSerializer = new ParallelJsonSerializer();
    private final Map<Long, RelationContext> relations = new HashMap<>();
    private final Map<Long, PreparedTransaction> preparedTransactions = new LinkedHashMap<>();
    private final Map<Long, Transaction> streamedTransactions = new LinkedHashMap<>();
    private final Set<Transaction> liveTransactions = Collections.synchronizedSet(
            Collections.newSetFromMap(new IdentityHashMap<>()));
    private final AtomicLong latestProcessedLsn = new AtomicLong();
    private final AtomicLong latestReceivedLsn = new AtomicLong();

    private CandidateConfig config;
    private OptimizedSettings optimizedSettings;
    private OrderedWorkQueue<CommittedTransaction, SourceRecord> workQueue;
    private Connection controlConnection;
    private Connection heartbeatActionConnection;
    private Connection replicationConnection;
    private PGReplicationStream replicationStream;
    private Map<String, Object> sourcePartition;
    private DataCollectionFilter dataCollectionFilter;
    private ColumnFilter columnFilter;
    private ColumnValueTransformers columnValueTransformers;
    private SourceTypePropagator sourceTypePropagator;
    private EventProcessingFailureHandlingMode eventFailureHandlingMode;
    private TopicNames topicNames;
    private CustomConverters customConverters;
    private Set<QualifiedTable> includedTables;
    private Set<String> skippedOperations;
    private volatile Transaction transaction;
    private volatile Long streamedTransactionId;
    private volatile boolean stopping;
    private volatile RuntimeException readerFailure;
    private Thread readerThread;
    private long previousLsn;
    private long lastAcknowledgedLsn;
    private long lastAcknowledgedEmittedLsn;
    private volatile long replayCheckpointLsn = -1L;
    private volatile long replaySkipThroughLsn;
    private volatile long replayResumeTransactionId = -1L;
    private volatile int replayResumeChangeCount;
    private long lastDeliveredLsn;
    private long lastDeliveredEmittedLsn;
    private long lastDeliveredResumeTransactionId = -1L;
    private int lastDeliveredResumeChangeCount;
    private long lastAcknowledgedResumeTransactionId = -1L;
    private int lastAcknowledgedResumeChangeCount;
    private long reconnectNotBeforeNanos;
    private long nextReplicationStatusNanos;
    private long transactionSequence;
    private long nextHeartbeatNanos;
    private long latestCommitTimeMicros;
    private volatile SnapshotState snapshotState;
    private volatile SnapshotSignal.Request blockingSnapshotRequest;
    private volatile DeferredTransaction deferredTransaction;
    private volatile IncrementalSnapshotState incrementalSnapshotState;
    private volatile IncrementalWindow incrementalWindow;
    private volatile boolean incrementalChunkSetup;
    private volatile boolean kafkaSignalPending;
    private final ArrayDeque<SnapshotSignal.Request> pendingKafkaSignals = new ArrayDeque<>();
    private KafkaSignalChannel kafkaSignalChannel;
    private TransactionContext transactionContext;
    private boolean connectorTransactionBoundaries;
    private boolean kafkaSignalCommitPending;
    private boolean kafkaSignalCheckpointReady;
    private long incrementalWindowSequence;

    @Override
    public String version() {
        return Version.VALUE;
    }

    @Override
    public void start(Map<String, String> properties) {
        config = new CandidateConfig(properties);
        config.validateSupportedConfiguration();
        optimizedSettings = new OptimizedSettings(properties);
        ValueHandlingModes valueHandlingModes = new ValueHandlingModes(properties);
        TimePrecisionMode timePrecisionMode = new TimePrecisionMode(properties);
        eventFailureHandlingMode = EventProcessingFailureHandlingMode.from(properties);
        valueConverter = valueHandlingModes.converter(timePrecisionMode);
        schemaFactory = new DebeziumSchemaFactory(
                valueConverter, valueHandlingModes, timePrecisionMode);
        if (optimizedSettings.incrementalSnapshotChunkSize() > config.maxQueueSize()) {
            throw new ConnectException(OptimizedSettings.INCREMENTAL_SNAPSHOT_CHUNK_SIZE
                    + " must not exceed max.queue.size on the native path");
        }
        if (FastPathSelector.select(properties) != FastPathSelector.Path.FAST) {
            throw new ConnectException("Optimized PostgreSQL fast task received a configuration outside its contract");
        }
        connectorTransactionBoundaries = "connector".equals(
                properties.getOrDefault(TRANSACTION_BOUNDARY_CONFIG, "poll"));
        transactionContext = connectorTransactionBoundaries ? context.transactionContext() : null;
        if (connectorTransactionBoundaries && transactionContext == null) {
            throw new ConnectException("transaction.boundary=connector requires a Kafka Connect "
                    + "exactly-once transaction context");
        }
        workQueue = new OrderedWorkQueue<>(optimizedSettings.processingWorkers(),
                "cdc-optimized-materializer-");
        stopping = false;
        readerFailure = null;
        reconnectNotBeforeNanos = 0L;
        transactionSequence = 0L;
        incrementalWindowSequence = 0L;
        latestCommitTimeMicros = currentPostgresMicros();
        sourcePartition = Map.of("server", config.topicPrefix());
        dataCollectionFilter = new DataCollectionFilter(properties);
        columnFilter = new ColumnFilter(properties);
        columnValueTransformers = new ColumnValueTransformers(properties);
        sourceTypePropagator = new SourceTypePropagator(config.database(), properties);
        topicNames = new TopicNames(properties);
        customConverters = new CustomConverters(properties);
        includedTables = Set.of();
        skippedOperations = parseSkippedOperations(properties.getOrDefault("skipped.operations", "t"));

        StoredOffset storedOffset = storedOffset();
        long storedLsn = storedOffset.lsn();
        long storedEmittedLsn = storedOffset.lastEmittedLsn();
        previousLsn = storedEmittedLsn;
        latestProcessedLsn.set(storedLsn);
        latestReceivedLsn.set(storedLsn);
        lastAcknowledgedLsn = storedLsn;
        lastAcknowledgedEmittedLsn = storedEmittedLsn;
        replaySkipThroughLsn = storedEmittedLsn;
        replayResumeTransactionId = storedOffset.resumeTransactionId();
        replayResumeChangeCount = storedOffset.resumeChangeCount();
        lastDeliveredLsn = storedLsn;
        lastDeliveredEmittedLsn = storedEmittedLsn;
        lastDeliveredResumeTransactionId = replayResumeTransactionId;
        lastDeliveredResumeChangeCount = replayResumeChangeCount;
        lastAcknowledgedResumeTransactionId = replayResumeTransactionId;
        lastAcknowledgedResumeChangeCount = replayResumeChangeCount;
        replayCheckpointLsn = storedEmittedLsn > storedLsn ? storedLsn : -1L;
        preparedTransactions.clear();
        nextHeartbeatNanos = heartbeatDeadline(System.nanoTime());

        try {
            boolean initialSnapshot = "initial".equals(config.snapshotMode())
                    && (!storedOffset.present() || storedOffset.incompleteInitialSnapshot());
            openConnections(storedLsn, initialSnapshot, initialSnapshot);
            if (optimizedSettings.readOnly()) {
                verifyReadOnlyServerVersion();
            }
            if (!initialSnapshot && storedOffset.incompleteBlockingSnapshot()) {
                blockingSnapshotRequest = new SnapshotSignal.Request(
                        SnapshotSignal.Type.BLOCKING, storedOffset.snapshotCollections());
            } else if (!initialSnapshot && storedOffset.incompleteIncrementalSnapshot()) {
                incrementalSnapshotState = restoreIncrementalState(
                        storedOffset.incrementalSnapshotState());
            }
            if (optimizedSettings.kafkaSignalingEnabled()) {
                kafkaSignalChannel = new KafkaSignalChannel(
                        optimizedSettings, config.topicPrefix());
            }
        } catch (Exception error) {
            stop();
            throw new ConnectException("Cannot start optimized PostgreSQL replication task", error);
        }
    }

    private void verifyReadOnlyServerVersion() throws SQLException {
        try (Statement statement = controlConnection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT current_setting('server_version_num')::integer")) {
            if (!result.next() || result.getInt(1) < 130000) {
                throw new ConnectException(
                        "read.only incremental snapshots require PostgreSQL 13 or later");
            }
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (stopping) {
            return null;
        }
        try {
            pollKafkaSignals();
        } catch (WakeupException error) {
            if (stopping) {
                return null;
            }
            throw error;
        }
        applyPendingKafkaSignals();
        if (snapshotState != null) {
            return markConnectorTransactionBoundary(pollSnapshot());
        }
        if (blockingSnapshotRequest != null && workQueue.pendingBatches() == 0
                && transaction == null) {
            throwReaderFailure();
            try {
                openBlockingSnapshot(blockingSnapshotRequest);
            } catch (SQLException error) {
                throw new ConnectException("Cannot open the PostgreSQL blocking snapshot", error);
            }
            if (snapshotState != null) {
                return markConnectorTransactionBoundary(pollSnapshot());
            }
        }
        if (incrementalSnapshotState != null && incrementalWindow == null
                && transaction == null
                && workQueue.pendingBatches() == 0) {
            try {
                openNextIncrementalWindow();
            } catch (SQLException error) {
                throw new ConnectException("Cannot read the next PostgreSQL incremental snapshot chunk", error);
            }
        }
        closeReadOnlyWindowIfCaughtUp(latestReceivedLsn.get());
        List<SourceRecord> records = readerThread == null
                ? pollSynchronous()
                : pollAsynchronous();
        records = appendHeartbeatIfDue(records);
        markKafkaSignalCheckpoint(records);
        return records;
    }

    private List<SourceRecord> appendHeartbeatIfDue(List<SourceRecord> records) {
        long intervalMillis = config.heartbeatIntervalMs();
        if (snapshotState != null || intervalMillis <= 0L || System.nanoTime() < nextHeartbeatNanos
                || workQueue.pendingBatches() > 0
                || records != null && records.size() >= config.maxBatchSize()) {
            return records;
        }

        executeHeartbeatAction();
        long timestamp = System.currentTimeMillis();
        long emittedLsn = latestProcessedLsn.get();
        long offsetLsn = safeCheckpointLsn(emittedLsn);
        String topic = topicNames.heartbeatTopic();
        Map<String, Object> key = Map.of("serverName", config.topicPrefix());
        Map<String, Object> value = Map.of("ts_ms", timestamp);
        Map<String, Object> sourceOffset = sourceOffset(offsetLsn, emittedLsn, null);
        SourceRecord heartbeat = sourceRecord(
                sourceOffset, topic, DebeziumSchemaFactory.heartbeatKey(), key,
                DebeziumSchemaFactory.heartbeatValue(), value, timestamp);
        List<SourceRecord> output = records == null
                ? new ArrayList<>(1)
                : records;
        output.add(heartbeat);
        markConnectorTransactionBoundary(heartbeat);
        nextHeartbeatNanos = heartbeatDeadline(System.nanoTime());
        return output;
    }

    private void executeHeartbeatAction() {
        String query = optimizedSettings.heartbeatActionQuery();
        if (query == null) {
            return;
        }
        try {
            Connection connection = heartbeatActionConnection;
            if (connection == null || connection.isClosed()) {
                Properties properties = optimizedSettings.postgresConnectionSettings()
                        .controlProperties(config.user(), config.password());
                connection = DriverManager.getConnection(jdbcUrl(), properties);
                connection.setAutoCommit(true);
                optimizedSettings.postgresConnectionSettings()
                        .initializeControlConnection(connection);
                heartbeatActionConnection = connection;
            }
            try (Statement statement = connection.createStatement()) {
                statement.execute(query);
            }
        } catch (SQLException error) {
            closeQuietly(heartbeatActionConnection);
            heartbeatActionConnection = null;
            LOGGER.warn("Cannot execute PostgreSQL heartbeat action query; "
                    + "the scheduled heartbeat will still be emitted", error);
        }
    }

    private long heartbeatDeadline(long nowNanos) {
        long intervalMillis = config == null ? 0L : config.heartbeatIntervalMs();
        if (intervalMillis <= 0L) {
            return Long.MAX_VALUE;
        }
        long intervalNanos = TimeUnit.MILLISECONDS.toNanos(intervalMillis);
        try {
            return Math.addExact(nowNanos, intervalNanos);
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private List<SourceRecord> pollSnapshot() {
        SnapshotState state = snapshotState;
        if (state == null) {
            return null;
        }
        List<SourceRecord> records = new ArrayList<>(config.maxBatchSize());
        try {
            while (snapshotState == state && state.next() != null
                    && records.size() < config.maxBatchSize()) {
                SnapshotRow current = state.next();
                state.next(readNextSnapshotRow(state));
                String marker = snapshotMarker(state.first(), state.next() != null);
                state.first(false);
                records.add(materializeSnapshot(current, state, marker));
                if (state.next() == null) {
                    completeSnapshot(state);
                }
            }
        } catch (SQLException error) {
            throw new ConnectException("Cannot read the PostgreSQL snapshot", error);
        }
        return records.isEmpty() ? null : records;
    }

    static String snapshotMarker(boolean first, boolean hasNext) {
        if (!hasNext) {
            return "last";
        }
        return first ? "first" : "true";
    }

    private SourceRecord materializeSnapshot(SnapshotRow snapshotRow, SnapshotState state,
                                             String marker) {
        RelationContext relation = snapshotRow.relation();
        Map<String, Object> completeAfter = row(
                relation, snapshotRow.tuple(), null, true, null);
        Map<String, Object> key = key(relation, completeAfter);
        Map<String, Object> after = filteredValue(relation, completeAfter);
        Relation relationMessage = relation.message();
        Map<String, Object> value = DebeziumEnvelope.createSnapshot(
                after, config.sourceVersion(), config.topicPrefix(), config.database(),
                relationMessage.namespace(), relationMessage.name(), state.transactionId(),
                state.lsn(), state.snapshotTimeMicros(), marker);
        Map<String, Object> sourceOffset = new LinkedHashMap<>();
        sourceOffset.put("lsn", state.lsn());
        sourceOffset.put("snapshot", true);
        sourceOffset.put("snapshot_completed", "last".equals(marker));
        sourceOffset.put("snapshot_kind", state.kind().offsetValue());
        sourceOffset.put("snapshot_data_collections", String.join(",", state.dataCollections()));
        try {
            sourceOffset.put("snapshot_data_collections_json",
                    OFFSET_JSON.writeValueAsString(state.dataCollections()));
        } catch (JsonProcessingException error) {
            throw new ConnectException("Cannot serialize snapshot data collections", error);
        }
        String topic = topicNames.dataChangeTopic(
                relationMessage.namespace(), relationMessage.name());
        long timestamp = ((Number) value.get("ts_ms")).longValue();
        return sourceRecord(sourceOffset, topic, relation.schemas().key(), key,
                relation.schemas().envelope(), value, timestamp);
    }

    private List<SourceRecord> pollSynchronous() throws InterruptedException {
        ensureConnected();
        if (stopping) {
            return null;
        }

        List<SourceRecord> records = new ArrayList<>(config.maxBatchSize());
        long deadlineNanos = System.nanoTime() + Duration.ofMillis(config.pollIntervalMs()).toNanos();

        try {
            while (!stopping && records.size() < config.maxBatchSize()) {
                drainReadyTransactions(records);
                if (records.size() >= config.maxBatchSize()) {
                    break;
                }

                if (mustPauseReader()) {
                    if (!records.isEmpty()) {
                        break;
                    }
                    if (!awaitHead(deadlineNanos)) {
                        break;
                    }
                    continue;
                }

                PGReplicationStream stream = replicationStream;
                if (stream == null) {
                    throw new RetriableException("PostgreSQL replication connection is being restored");
                }
                ByteBuffer messageBytes = stream.readPending();
                long receivedLsn = stream.getLastReceiveLSN().asLong();
                if (messageBytes == null) {
                    latestReceivedLsn.accumulateAndGet(receivedLsn, Math::max);
                    closeReadOnlyWindowIfCaughtUp(receivedLsn);
                    drainReadyTransactions(records);
                    if (!records.isEmpty() || System.nanoTime() >= deadlineNanos) {
                        break;
                    }
                    if (workQueue.pendingBatches() > 0) {
                        awaitHead(Math.min(deadlineNanos,
                                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(10L)));
                    } else {
                        TimeUnit.MILLISECONDS.sleep(Math.min(10L, config.pollIntervalMs()));
                    }
                    continue;
                }

                int encodedBytes = messageBytes.remaining();
                process(decoder.decode(messageBytes,
                        streamedTransactionId != null), receivedLsn);
                latestReceivedLsn.accumulateAndGet(receivedLsn, Math::max);
                closeReadOnlyWindowIfCaughtUp(receivedLsn);
                if (shouldStartAsyncReader(encodedBytes)) {
                    startReader();
                    break;
                }
            }
        } catch (SQLException error) {
            disconnectForRetry();
            throw new RetriableException(
                    "PostgreSQL logical replication connection was lost; reconnect scheduled", error);
        } catch (RetriableException error) {
            throw error;
        } catch (ConnectException error) {
            throw error;
        } catch (RuntimeException error) {
            throw new ConnectException("Cannot decode PostgreSQL pgoutput stream", error);
        }

        return records.isEmpty() ? null : records;
    }

    private List<SourceRecord> pollAsynchronous() throws InterruptedException {

        List<SourceRecord> records = new ArrayList<>(config.maxBatchSize());
        long deadlineNanos = System.nanoTime() + Duration.ofMillis(config.pollIntervalMs()).toNanos();

        while (!stopping && records.size() < config.maxBatchSize()) {
            throwReaderFailure();
            drainReadyTransactions(records);
            if (!records.isEmpty() || records.size() >= config.maxBatchSize()) {
                break;
            }

            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                break;
            }
            if (workQueue.pendingBatches() > 0) {
                workQueue.awaitHead(remainingNanos, TimeUnit.NANOSECONDS);
            } else {
                workQueue.awaitBatch(remainingNanos, TimeUnit.NANOSECONDS);
            }
        }
        throwReaderFailure();

        return records.isEmpty() ? null : records;
    }

    private void drainReadyTransactions(List<SourceRecord> output) {
        while (output.size() < config.maxBatchSize()) {
            var ready = workQueue.pollReady();
            if (ready.isEmpty()) {
                return;
            }
            OrderedWorkQueue.CompletedBatch<CommittedTransaction, SourceRecord> batch = ready.get();
            output.addAll(batch.items());
            // Streaming transactions can be much larger than Kafka's transaction timeout.
            // Each ordered materialization chunk has an exact resume offset, so it is the
            // largest safe connector-defined producer transaction boundary.
            if (!batch.items().isEmpty()) {
                markConnectorTransactionBoundary(batch.items().getLast());
            }
            if (batch.finalChunk() && batch.metadata().transaction() != null) {
                batch.metadata().transaction().close();
            }
        }
    }

    private List<SourceRecord> markConnectorTransactionBoundary(List<SourceRecord> records) {
        if (records != null && !records.isEmpty()) {
            markConnectorTransactionBoundary(records.getLast());
        }
        return records;
    }

    private void markConnectorTransactionBoundary(SourceRecord record) {
        if (connectorTransactionBoundaries) {
            transactionContext.commitTransaction(record);
        }
    }

    private boolean mustPauseReader() {
        if (snapshotState != null || blockingSnapshotRequest != null || incrementalChunkSetup
                || kafkaSignalPending && transaction == null) {
            return true;
        }
        int transactionItems = retainedTransactionItems();
        long transactionBytes = retainedTransactionBytes();
        boolean hasCommittedWork = workQueue.pendingBatches() > 0;
        boolean transactionCapacityBlocked = hasCommittedWork
                && Math.addExact(workQueue.pendingBatches(), retainedTransactionCount())
                >= optimizedSettings.maxInflightTransactions();
        return transactionCapacityBlocked
                || (hasCommittedWork
                && Math.addExact(workQueue.pendingItems(), transactionItems) >= config.maxQueueSize())
                || (hasCommittedWork
                && Math.addExact(workQueue.pendingBytes(), transactionBytes)
                >= optimizedSettings.maxInflightBytes());
    }

    private int retainedTransactionCount() {
        return retainedTransactions().size();
    }

    private int retainedTransactionItems() {
        int total = 0;
        for (Transaction retained : retainedTransactions()) {
            total = Math.addExact(total, retained.outputRecords());
        }
        return total;
    }

    private long retainedTransactionBytes() {
        long total = 0L;
        for (Transaction retained : retainedTransactions()) {
            total = Math.addExact(total, retained.estimatedBytes());
        }
        return total;
    }

    private Set<Transaction> retainedTransactions() {
        Set<Transaction> retained = Collections.newSetFromMap(new IdentityHashMap<>());
        Transaction active = transaction;
        if (active != null) {
            retained.add(active);
        }
        retained.addAll(streamedTransactions.values());
        for (PreparedTransaction prepared : preparedTransactions.values()) {
            retained.add(prepared.transaction());
        }
        return retained;
    }

    private void ensureUnresolvedTransactionCapacity(long transactionId) {
        int retained = Math.addExact(workQueue.pendingBatches(), retainedTransactionCount());
        if (retained >= optimizedSettings.maxInflightTransactions()) {
            throw new ConnectException("Cannot retain streamed/prepared transaction "
                    + transactionId + ": optimized.max.inflight.transactions="
                    + optimizedSettings.maxInflightTransactions()
                    + " is already exhausted by " + retained + " transactions");
        }
    }

    private boolean awaitHead(long deadlineNanos) throws InterruptedException {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0L) {
            return false;
        }
        return workQueue.awaitHead(remainingNanos, TimeUnit.NANOSECONDS);
    }

    static boolean shouldStartAsyncReader(int encodedMessageBytes) {
        return encodedMessageBytes >= ASYNC_READER_THRESHOLD_BYTES;
    }

    private void startReader() {
        if (readerThread != null || stopping) {
            return;
        }
        readerThread = new Thread(this::readerLoop, "cdc-optimized-postgres-reader");
        readerThread.setDaemon(true);
        readerThread.start();
    }

    private void readerLoop() {
        while (!stopping) {
            try {
                if (snapshotState != null || blockingSnapshotRequest != null || incrementalChunkSetup) {
                    TimeUnit.MILLISECONDS.sleep(1L);
                    continue;
                }
                ensureConnected();
                if (stopping) {
                    return;
                }
                if (snapshotState != null || blockingSnapshotRequest != null || incrementalChunkSetup) {
                    TimeUnit.MILLISECONDS.sleep(1L);
                    continue;
                }
                if (mustPauseReader()) {
                    forceReplicationStatusIfDue();
                    int transactionItems = retainedTransactionItems();
                    long transactionBytes = retainedTransactionBytes();
                    workQueue.awaitCapacity(
                            optimizedSettings.maxInflightTransactions(),
                            Math.max(1, config.maxQueueSize() - transactionItems),
                            Math.max(1L, optimizedSettings.maxInflightBytes() - transactionBytes),
                            10L,
                            TimeUnit.MILLISECONDS);
                    continue;
                }

                PGReplicationStream stream = replicationStream;
                if (stream == null) {
                    throw new RetriableException("PostgreSQL replication connection is being restored");
                }
                ByteBuffer messageBytes = stream.readPending();
                long receivedLsn = stream.getLastReceiveLSN().asLong();
                if (messageBytes == null) {
                    latestReceivedLsn.accumulateAndGet(receivedLsn, Math::max);
                    closeReadOnlyWindowIfCaughtUp(receivedLsn);
                    TimeUnit.MILLISECONDS.sleep(1L);
                    continue;
                }
                process(decoder.decode(messageBytes,
                        streamedTransactionId != null), receivedLsn);
                latestReceivedLsn.accumulateAndGet(receivedLsn, Math::max);
                closeReadOnlyWindowIfCaughtUp(receivedLsn);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                if (!stopping) {
                    readerFailure = new ConnectException(
                            "Optimized PostgreSQL reader was interrupted", error);
                }
                return;
            } catch (SQLException error) {
                disconnectForRetry();
                if (!stopping) {
                    LOGGER.warn("PostgreSQL logical replication connection was lost; "
                            + "the asynchronous reader will reconnect", error);
                }
            } catch (RetriableException error) {
                if (!stopping) {
                    LOGGER.warn("PostgreSQL asynchronous reader reconnect attempt failed; "
                            + "retrying after backoff", error);
                }
            } catch (ConnectException error) {
                if (!stopping) {
                    readerFailure = error;
                }
                return;
            } catch (RuntimeException error) {
                if (!stopping) {
                    readerFailure = new ConnectException(
                            "Cannot decode PostgreSQL pgoutput stream", error);
                }
                return;
            }
        }
    }

    private void throwReaderFailure() {
        RuntimeException failure = readerFailure;
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public synchronized void commit() throws InterruptedException {
        PGReplicationStream stream = replicationStream;
        long committedLsn = lastDeliveredLsn;
        boolean advancesCheckpoint = committedLsn > lastAcknowledgedLsn;
        if (advancesCheckpoint) {
            lastAcknowledgedLsn = committedLsn;
        }
        lastAcknowledgedEmittedLsn = Math.max(
                lastAcknowledgedEmittedLsn, lastDeliveredEmittedLsn);
        lastAcknowledgedResumeTransactionId = lastDeliveredResumeTransactionId;
        lastAcknowledgedResumeChangeCount = lastDeliveredResumeChangeCount;
        if (stream == null) {
            refreshReplayCheckpoint();
        }
        if (stream != null && advancesCheckpoint) {
            try {
                LogSequenceNumber position = LogSequenceNumber.valueOf(committedLsn);
                stream.setAppliedLSN(position);
                stream.setFlushedLSN(position);
                stream.forceUpdateStatus();
                scheduleNextReplicationStatus();
            } catch (SQLException error) {
                disconnectForRetry();
                throw new RetriableException(
                        "Cannot acknowledge committed PostgreSQL LSN " + committedLsn
                                + "; reconnect scheduled",
                        error);
            }
        }
        commitKafkaSignalsIfCheckpointed();
    }

    @Override
    public synchronized void commitRecord(SourceRecord record, RecordMetadata metadata) {
        acknowledgeDeliveredRecord(record);
    }

    private void acknowledgeDeliveredRecord(SourceRecord record) {
        Map<String, ?> offset = record.sourceOffset();
        long deliveredLsn = offsetLong(offset.get("lsn"), 0L);
        long deliveredEmittedLsn = offsetLong(offset.get("last_emitted_lsn"), deliveredLsn);
        long resumeTransactionId = offsetLong(
                offset.get("transaction_resume_id"), -1L);
        int resumeChangeCount = Math.toIntExact(offsetLong(
                offset.get("transaction_resume_count"), 0L));
        if (deliveredLsn > lastDeliveredLsn) {
            lastDeliveredLsn = deliveredLsn;
            lastDeliveredEmittedLsn = deliveredEmittedLsn;
            lastDeliveredResumeTransactionId = resumeTransactionId;
            lastDeliveredResumeChangeCount = resumeChangeCount;
            return;
        }
        if (deliveredLsn < lastDeliveredLsn) {
            return;
        }
        lastDeliveredEmittedLsn = Math.max(
                lastDeliveredEmittedLsn, deliveredEmittedLsn);
        if (resumeTransactionId >= 0L
                && (lastDeliveredResumeTransactionId < 0L
                || lastDeliveredResumeTransactionId == resumeTransactionId
                && resumeChangeCount >= lastDeliveredResumeChangeCount)) {
            lastDeliveredResumeTransactionId = resumeTransactionId;
            lastDeliveredResumeChangeCount = resumeChangeCount;
        }
    }

    @Override
    public void stop() {
        stopping = true;
        KafkaSignalChannel signalChannel = kafkaSignalChannel;
        if (signalChannel != null) {
            signalChannel.wakeup();
        }
        Thread thread = readerThread;
        if (thread != null) {
            thread.interrupt();
        }
        synchronized (this) {
            closeConnections();
        }
        if (thread != null && thread != Thread.currentThread()) {
            try {
                thread.join(TimeUnit.SECONDS.toMillis(5L));
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        }
        readerThread = null;
        dropSlotOnStop();
        OrderedWorkQueue<CommittedTransaction, SourceRecord> queue = workQueue;
        workQueue = null;
        if (queue != null) {
            queue.close();
        }
        kafkaSignalChannel = null;
        closeQuietly(signalChannel);
    }

    private void process(PgOutputMessage message, long messageLsn) throws SQLException {
        switch (message) {
            case Begin begin -> beginTransaction(begin);
            case Relation relation -> {
                requireStreamedIdentity(relation.streamedTransactionId());
                relations.put(relation.relationId(), loadRelation(relation));
            }
            case Type type -> requireStreamedIdentity(type.streamedTransactionId());
            case Origin origin -> {
                requireTransaction().origin(origin.name(), origin.originLsn());
            }
            case LogicalMessage logicalMessage -> {
                requireStreamedIdentity(logicalMessage.streamedTransactionId());
                processLogicalMessage(logicalMessage, messageLsn);
            }
            case StreamStart streamStart -> startStreamedTransaction(streamStart);
            case StreamStop ignored -> stopStreamedTransaction();
            case StreamCommit streamCommit -> commitStreamedTransaction(streamCommit);
            case StreamAbort streamAbort -> abortStreamedTransaction(
                    streamAbort, messageLsn);
            case BeginPrepare beginPrepare -> beginPreparedTransaction(beginPrepare);
            case Prepare prepare -> prepareTransaction(prepare);
            case CommitPrepared commitPrepared -> commitPreparedTransaction(commitPrepared);
            case RollbackPrepared rollbackPrepared -> rollbackPreparedTransaction(rollbackPrepared);
            case StreamPrepare streamPrepare -> prepareStreamedTransaction(streamPrepare);
            case Insert insert -> {
                requireStreamedIdentity(insert.streamedTransactionId());
                addChange(PendingChange.insert(
                        relation(insert.relationId()), insert.newTuple(), messageLsn),
                        insert.streamedTransactionId());
            }
            case Update update -> {
                requireStreamedIdentity(update.streamedTransactionId());
                addChange(PendingChange.update(
                        relation(update.relationId()), update.oldTupleKind(), update.oldTuple(),
                        update.newTuple(), messageLsn), update.streamedTransactionId());
            }
            case Delete delete -> {
                requireStreamedIdentity(delete.streamedTransactionId());
                addChange(PendingChange.delete(
                        relation(delete.relationId()), delete.oldTupleKind(), delete.oldTuple(),
                        messageLsn), delete.streamedTransactionId());
            }
            case Truncate truncate -> {
                requireStreamedIdentity(truncate.streamedTransactionId());
                for (long relationId : truncate.relationIds()) {
                    addChange(PendingChange.truncate(relation(relationId), messageLsn),
                            truncate.streamedTransactionId());
                }
            }
            case Commit commit -> commitTransaction(commit);
        }
    }

    private void startStreamedTransaction(StreamStart start) {
        if (!optimizedSettings.streamedTransactionsEnabled()) {
            throw unsupportedProtocolMessage(start);
        }
        requireNoActiveTransaction("STREAM START");
        Transaction current = streamedTransactions.get(start.transactionId());
        if (start.firstSegment()) {
            if (current != null) {
                throw new ConnectException("Duplicate first streamed segment for transaction "
                        + start.transactionId());
            }
            ensureUnresolvedTransactionCapacity(start.transactionId());
            current = newTransaction(start.transactionId(), 0L, null, null,
                    checkpointAnchorLsn());
            streamedTransactions.put(start.transactionId(), current);
        } else if (current == null) {
            throw new ConnectException("Continuation for unknown streamed transaction "
                    + start.transactionId());
        }
        transaction = current;
        streamedTransactionId = start.transactionId();
    }

    private void stopStreamedTransaction() {
        Transaction current = requireTransaction();
        if (streamedTransactionId == null) {
            throw new ConnectException("STREAM STOP received outside a streamed segment");
        }
        if (streamedTransactions.get(streamedTransactionId) != current) {
            throw new ConnectException("STREAM STOP transaction state is inconsistent");
        }
        transaction = null;
        streamedTransactionId = null;
    }

    private void commitStreamedTransaction(StreamCommit commit) {
        requireNoActiveTransaction("STREAM COMMIT");
        Transaction current = streamedTransactions.remove(commit.transactionId());
        if (current == null) {
            throw new ConnectException("STREAM COMMIT references unknown transaction "
                    + commit.transactionId());
        }
        commitTransaction(current, commit.commitLsn(), commit.endLsn(),
                commit.commitTimeMicros());
    }

    private void abortStreamedTransaction(StreamAbort abort, long messageLsn) {
        requireNoActiveTransaction("STREAM ABORT");
        Transaction current = streamedTransactions.get(abort.transactionId());
        if (current == null) {
            throw new ConnectException("STREAM ABORT references unknown transaction "
                    + abort.transactionId());
        }
        if (abort.subtransactionId() != abort.transactionId()) {
            current.rollbackSubtransaction(abort.subtransactionId());
            return;
        }
        streamedTransactions.remove(abort.transactionId());
        current.close();
        long abortLsn = abort.abortLsn() == null ? messageLsn : abort.abortLsn();
        latestProcessedLsn.accumulateAndGet(abortLsn, Math::max);
        previousLsn = Math.max(previousLsn, abortLsn);
        consumeReplayWatermark(abortLsn);
    }

    private void prepareStreamedTransaction(StreamPrepare prepare) {
        if (!optimizedSettings.twoPhaseEnabled()) {
            throw unsupportedProtocolMessage(prepare);
        }
        requireNoActiveTransaction("STREAM PREPARE");
        Transaction current = streamedTransactions.remove(prepare.transactionId());
        if (current == null) {
            throw new ConnectException("STREAM PREPARE references unknown transaction "
                    + prepare.transactionId());
        }
        PreparedTransaction prepared = new PreparedTransaction(
                current, prepare.prepareLsn(), prepare.endLsn(),
                prepare.prepareTimeMicros(), prepare.gid());
        if (preparedTransactions.putIfAbsent(prepare.transactionId(), prepared) != null) {
            current.close();
            throw new ConnectException("Duplicate streamed prepared transaction "
                    + prepare.transactionId() + " (" + prepare.gid() + ")");
        }
    }

    private void requireStreamedIdentity(Long transactionId) {
        if (!optimizedSettings.streamedTransactionsEnabled()) {
            if (transactionId != null) {
                throw new ConnectException("Unexpected streamed transaction identity "
                        + transactionId);
            }
            return;
        }
        if (streamedTransactionId == null) {
            if (transactionId != null) {
                throw new ConnectException("Streamed payload received outside STREAM START: "
                        + transactionId);
            }
            return;
        }
        if (transactionId == null) {
            throw new ConnectException("Streamed payload is missing its transaction identity");
        }
    }

    private void beginTransaction(Begin begin) {
        requireNoActiveTransaction("BEGIN");
        if (replayResumeTransactionId >= 0L
                && begin.transactionId() != replayResumeTransactionId) {
            throw new ConnectException("Stored large-transaction resume state expects transaction "
                    + replayResumeTransactionId + " but PostgreSQL replay started transaction "
                    + begin.transactionId());
        }
        transaction = newTransaction(begin.transactionId(), begin.commitTimeMicros(),
                null, null, -1L);
    }

    private void beginPreparedTransaction(BeginPrepare begin) {
        requireNoActiveTransaction("BEGIN PREPARE");
        if (!optimizedSettings.twoPhaseEnabled()) {
            throw unsupportedProtocolMessage(begin);
        }
        ensureUnresolvedTransactionCapacity(begin.transactionId());
        transaction = newTransaction(begin.transactionId(), begin.prepareTimeMicros(),
                begin.gid(), begin.prepareLsn(), checkpointAnchorLsn());
    }

    private Transaction newTransaction(long transactionId, long beginCommitTimeMicros,
                                       String preparedGid, Long prepareLsn,
                                       long checkpointLsn) {
        Transaction created = new Transaction(
                transactionId, beginCommitTimeMicros, preparedGid, prepareLsn,
                checkpointLsn);
        liveTransactions.add(created);
        return created;
    }

    private synchronized long checkpointAnchorLsn() {
        long checkpointLsn = Math.max(latestProcessedLsn.get(), lastAcknowledgedLsn);
        PGReplicationStream stream = replicationStream;
        if (stream != null) {
            checkpointLsn = Math.max(checkpointLsn, stream.getLastFlushedLSN().asLong());
        }
        return checkpointLsn;
    }

    private void prepareTransaction(Prepare prepare) {
        if (!optimizedSettings.twoPhaseEnabled()) {
            throw unsupportedProtocolMessage(prepare);
        }
        Transaction current = requireTransaction();
        current.verifyPreparedIdentity(prepare.transactionId(), prepare.gid(), prepare.prepareLsn());
        transaction = null;
        PreparedTransaction prepared = new PreparedTransaction(
                current, prepare.prepareLsn(), prepare.endLsn(), prepare.prepareTimeMicros(),
                prepare.gid());
        if (preparedTransactions.putIfAbsent(prepare.transactionId(), prepared) != null) {
            throw new ConnectException("Duplicate prepared transaction " + prepare.transactionId()
                    + " (" + prepare.gid() + ")");
        }
    }

    private void commitPreparedTransaction(CommitPrepared commit) {
        if (!optimizedSettings.twoPhaseEnabled()) {
            throw unsupportedProtocolMessage(commit);
        }
        requireNoActiveTransaction("COMMIT PREPARED");
        PreparedTransaction prepared = preparedTransactions.remove(commit.transactionId());
        if (prepared == null) {
            throw new ConnectException("COMMIT PREPARED references unknown transaction "
                    + commit.transactionId() + " (" + commit.gid() + ")");
        }
        prepared.verify(commit.transactionId(), commit.gid());
        commitTransaction(prepared.transaction(), commit.commitLsn(), commit.endLsn(),
                commit.commitTimeMicros());
    }

    private void rollbackPreparedTransaction(RollbackPrepared rollback) {
        if (!optimizedSettings.twoPhaseEnabled()) {
            throw unsupportedProtocolMessage(rollback);
        }
        requireNoActiveTransaction("ROLLBACK PREPARED");
        PreparedTransaction prepared = preparedTransactions.remove(rollback.transactionId());
        if (prepared == null) {
            throw new ConnectException("ROLLBACK PREPARED references unknown transaction "
                    + rollback.transactionId() + " (" + rollback.gid() + ")");
        }
        prepared.verify(rollback.transactionId(), rollback.gid());
        prepared.transaction().close();
        latestCommitTimeMicros = rollback.rollbackTimeMicros();
        latestProcessedLsn.accumulateAndGet(rollback.rollbackEndLsn(), Math::max);
        previousLsn = rollback.rollbackEndLsn();
        consumeReplayWatermark(rollback.rollbackEndLsn());
    }

    private void requireNoActiveTransaction(String boundary) {
        if (transaction != null) {
            throw new ConnectException(boundary + " received while transaction "
                    + transaction.transactionId() + " is still active");
        }
    }

    private static ConnectException unsupportedProtocolMessage(PgOutputMessage message) {
        return new ConnectException("Streaming/two-phase pgoutput message is outside the verified "
                + "fast-path contract: " + message.getClass().getSimpleName());
    }

    private void addChange(PendingChange change) {
        addChange(change, null);
    }

    private void addChange(PendingChange change, Long payloadTransactionId) {
        Transaction current = requireTransaction();
        if (change.relation() != null) {
            boolean signal = isSignalRelation(change.relation());
            if (!signal && (!includedTables.contains(qualifiedTable(change.relation()))
                    || skippedOperations.contains(change.operation()))) {
                return;
            }
        }
        boolean firstMetadataEvent = current.totalChangeCount() == 0
                && config.provideTransactionMetadata();
        long changeBytes = Math.addExact(estimatedBytes(change),
                firstMetadataEvent ? TRANSACTION_METADATA_BYTES : 0L);
        current.add(change, changeBytes, payloadTransactionId);
    }

    private void processLogicalMessage(LogicalMessage message, long receivedLsn) {
        long messageLsn = message.lsn() > 0L ? message.lsn() : receivedLsn;
        if (!message.transactional() && consumeReplayWatermark(messageLsn)) {
            latestProcessedLsn.accumulateAndGet(messageLsn, Math::max);
            previousLsn = messageLsn;
            return;
        }
        if (!optimizedSettings.includesLogicalMessage(message.prefix())) {
            if (!message.transactional()) {
                latestProcessedLsn.accumulateAndGet(messageLsn, Math::max);
                previousLsn = messageLsn;
            }
            return;
        }

        PendingChange change = PendingChange.logicalMessage(message, messageLsn);
        if (message.transactional()) {
            addChange(change, message.streamedTransactionId());
            return;
        }

        long commitTimeMicros = latestCommitTimeMicros;
        long checkpointLsn = safeCheckpointLsn(messageLsn);
        EventMetadata event = new EventMetadata(messageLsn, null, 1L, 1L);
        List<Callable<List<SourceRecord>>> chunks = List.of(
                () -> materializeRange(List.of(change), List.of(event), 0, 1,
                        null, commitTimeMicros, messageLsn, checkpointLsn, messageLsn, null));
        workQueue.submit(new CommittedTransaction(++transactionSequence, null),
                1, estimatedBytes(change), eventFailureHandlingMode.skipsFailedEvents(), chunks);
        latestProcessedLsn.accumulateAndGet(messageLsn, Math::max);
        previousLsn = messageLsn;
    }

    private void commitTransaction(Commit commit) {
        Transaction current = requireTransaction();
        transaction = null;
        commitTransaction(current, commit.commitLsn(), commit.endLsn(), commit.commitTimeMicros());
    }

    private void commitTransaction(Transaction current, long sourceLsn,
                                   long durableOffsetLsn, long commitTimeMicros) {
        latestCommitTimeMicros = commitTimeMicros;
        if (consumeReplayWatermark(durableOffsetLsn)) {
            latestProcessedLsn.accumulateAndGet(durableOffsetLsn, Math::max);
            previousLsn = sourceLsn;
            current.close();
            return;
        }

        current.seal();
        if (current.resumeActive()) {
            replayResumeTransactionId = -1L;
            replayResumeChangeCount = 0;
        }

        DetectedSnapshotSignal detectedSignal = snapshotRequest(current.changes());
        SnapshotSignal.Request snapshotRequest = detectedSignal == null
                ? null : detectedSignal.request();
        IncrementalBoundary incrementalBoundary = incrementalBoundary(current.changes());
        if (detectedSignal != null) {
            validateSnapshotRequest(snapshotRequest);
        }
        deduplicateIncrementalWindow(current.changes());
        if (incrementalBoundary == null) {
            incrementalBoundary = readOnlyIncrementalBoundary(
                    current.transactionId(), durableOffsetLsn);
        }
        if (snapshotRequest != null && snapshotRequest.stop()) {
            stopIncrementalSnapshot(snapshotRequest);
        }
        boolean startsIncrementalSnapshot = snapshotRequest != null
                && !snapshotRequest.stop()
                && snapshotRequest.type() == SnapshotSignal.Type.INCREMENTAL
                && !snapshotRequest.dataCollections().isEmpty();
        if (startsIncrementalSnapshot) {
            if (incrementalSnapshotState != null) {
                throw new ConnectException(
                        "An incremental snapshot is already active on the native path");
            }
            try {
                startIncrementalSnapshot(snapshotRequest);
            } catch (SQLException error) {
                throw new ConnectException(
                        "Cannot initialize the PostgreSQL incremental snapshot", error);
            }
        }
        String incrementalStateBeforeBoundary = incrementalProgressJson(
                incrementalSnapshotState);
        ClosedIncrementalWindow closedIncrementalWindow = processIncrementalBoundary(
                incrementalBoundary, sourceLsn, durableOffsetLsn);
        String transactionIncrementalState = closedIncrementalWindow != null
                && !closedIncrementalWindow.rows().isEmpty()
                ? incrementalStateBeforeBoundary
                : incrementalProgressJson(incrementalSnapshotState);

        if (!current.changes().isEmpty()) {
            List<PendingChange> changes = current.changes();
            long previousSourceLsn = previousLsn;
            String transactionBoundaryId = transactionId(
                    current.transactionId(), durableOffsetLsn);
            boolean blockingSignal = snapshotRequest != null
                    && snapshotRequest.type() == SnapshotSignal.Type.BLOCKING;
            if (!blockingSignal) {
                if (current.resumeChangeCount() > changes.size()) {
                    throw new ConnectException("Stored large-transaction resume count "
                            + current.resumeChangeCount() + " exceeds replayed transaction size "
                            + changes.size());
                }
                int fromIndex = current.resumeChangeCount();
                submitTransaction(current, fromIndex, changes.size(), commitTimeMicros,
                        previousSourceLsn, durableOffsetLsn, transactionBoundaryId,
                        transactionIncrementalState);
            } else {
                int split = detectedSignal.changeIndex() + 1;
                List<PendingChange> beforeSnapshot = List.copyOf(changes.subList(0, split));
                List<EventMetadata> eventMetadata = eventMetadata(
                        changes, current.transactionId(), sourceLsn);
                List<EventMetadata> beforeMetadata = List.copyOf(eventMetadata.subList(0, split));
                submitTransaction(beforeSnapshot, beforeMetadata, current,
                        commitTimeMicros, previousSourceLsn, lastAcknowledgedLsn,
                        estimatedBytes(beforeSnapshot),
                        transactionBoundaryId,
                        transactionIncrementalState);
                if (split < changes.size()) {
                    List<PendingChange> afterSnapshot = List.copyOf(
                            changes.subList(split, changes.size()));
                    deferredTransaction = new DeferredTransaction(
                            afterSnapshot,
                            List.copyOf(eventMetadata.subList(split, eventMetadata.size())),
                            current,
                            commitTimeMicros,
                            previousSourceLsn,
                            durableOffsetLsn,
                            estimatedBytes(afterSnapshot),
                            transactionBoundaryId);
                }
            }
        } else {
            current.close();
        }
        submitClosedIncrementalWindow(closedIncrementalWindow);
        if (snapshotRequest != null && !snapshotRequest.stop()) {
            if (snapshotRequest.type() == SnapshotSignal.Type.BLOCKING) {
                blockingSnapshotRequest = snapshotRequest;
            }
        }
        latestProcessedLsn.accumulateAndGet(durableOffsetLsn, Math::max);
        previousLsn = sourceLsn;
    }

    private long safeCheckpointLsn(long emittedLsn) {
        long barrier = unresolvedCheckpointLsn();
        return barrier < 0L ? emittedLsn : Math.min(barrier, emittedLsn);
    }

    private long unresolvedCheckpointLsn() {
        long checkpoint = replayCheckpointLsn;
        for (Transaction retained : retainedTransactions()) {
            long candidate = retained.checkpointLsn();
            if (candidate >= 0L && (checkpoint < 0L || candidate < checkpoint)) {
                checkpoint = candidate;
            }
        }
        return checkpoint;
    }

    private boolean consumeReplayWatermark(long processedLsn) {
        long watermark = replaySkipThroughLsn;
        if (watermark <= 0L) {
            return false;
        }
        if (processedLsn <= watermark) {
            if (processedLsn == watermark) {
                replaySkipThroughLsn = 0L;
                replayCheckpointLsn = -1L;
            }
            return true;
        }
        replaySkipThroughLsn = 0L;
        replayCheckpointLsn = -1L;
        return false;
    }

    private void submitTransaction(List<PendingChange> changes,
                                   List<EventMetadata> eventMetadata,
                                   Transaction transaction,
                                   long commitTimeMicros,
                                   long previousSourceLsn,
                                   long durableOffsetLsn,
                                   long estimatedBytes,
                                   String transactionBoundaryId) {
        submitTransaction(changes, eventMetadata, transaction, commitTimeMicros,
                previousSourceLsn, durableOffsetLsn, estimatedBytes,
                transactionBoundaryId, incrementalProgressJson(incrementalSnapshotState));
    }

    private void submitTransaction(List<PendingChange> changes,
                                   List<EventMetadata> eventMetadata,
                                   Transaction transaction,
                                   long commitTimeMicros,
                                   long previousSourceLsn,
                                   long durableOffsetLsn,
                                   long estimatedBytes,
                                   String transactionBoundaryId,
                                   String incrementalState) {
        long checkpointLsn = safeCheckpointLsn(durableOffsetLsn);
        List<Callable<List<SourceRecord>>> chunks = new ArrayList<>();
        if (config.provideTransactionMetadata()) {
            EventMetadata first = eventMetadata.getFirst();
            chunks.add(() -> List.of(transactionBoundaryRecord(
                    "BEGIN", first.transactionId(), transaction, commitTimeMicros,
                    checkpointLsn, durableOffsetLsn, changes)));
        }
        int chunkSize = optimizedSettings.processingChunkSize();
        for (int start = 0; start < changes.size(); start += chunkSize) {
            int fromIndex = start;
            int toIndex = Math.min(start + chunkSize, changes.size());
            chunks.add(() -> materializeRange(changes, eventMetadata, fromIndex, toIndex,
                    transaction, commitTimeMicros, previousSourceLsn, checkpointLsn,
                    durableOffsetLsn, incrementalState));
        }
        if (config.provideTransactionMetadata()) {
            chunks.add(() -> List.of(transactionBoundaryRecord(
                    "END", transactionBoundaryId, transaction, commitTimeMicros,
                    checkpointLsn, durableOffsetLsn, changes)));
        }
        workQueue.submit(new CommittedTransaction(++transactionSequence, transaction),
                outputRecordCount(changes), Math.min(
                        Math.max(1L, estimatedBytes), optimizedSettings.maxInflightBytes()),
                eventFailureHandlingMode.skipsFailedEvents(), chunks);
    }

    private void submitTransaction(Transaction transaction, int fromIndex, int toIndex,
                                   long commitTimeMicros, long previousSourceLsn,
                                   long durableOffsetLsn, String transactionBoundaryId,
                                   String incrementalState) {
        List<PendingChange> changes = transaction.changes();
        List<SpillableList.Range> dataRanges = transaction.changes.ranges(fromIndex, toIndex);
        boolean emitBegin = config.provideTransactionMetadata() && fromIndex == 0
                && !transaction.resumeActive();
        boolean emitEnd = config.provideTransactionMetadata();
        int chunkCount = dataRanges.size() + (emitBegin ? 1 : 0) + (emitEnd ? 1 : 0);
        if (chunkCount == 0) {
            transaction.close();
            return;
        }
        int expectedRecords = outputRecordCount(changes, fromIndex, toIndex)
                + (emitBegin ? 1 : 0) + (emitEnd ? 1 : 0);
        long checkpointLsn = safeCheckpointLsn(durableOffsetLsn);
        long retainedBytes = Math.min(transaction.changes.estimatedBytes(fromIndex, toIndex),
                optimizedSettings.maxInflightBytes());
        long resumeOffsetLsn = lastAcknowledgedLsn;
        long resumeEmittedLsn = lastAcknowledgedEmittedLsn;
        long sequence = ++transactionSequence;
        workQueue.submitStreaming(new CommittedTransaction(sequence, transaction),
                expectedRecords, Math.max(1L, retainedBytes),
                eventFailureHandlingMode.skipsFailedEvents(), chunkCount, chunkIndex -> {
                    if (emitBegin && chunkIndex == 0) {
                        PendingChange firstChange = changes.get(fromIndex);
                        EventMetadata first = eventMetadata(
                                firstChange, transaction.transactionId(), fromIndex,
                                durableOffsetLsn);
                        return () -> List.of(withTransactionResume(
                                transactionBoundaryRecord(
                                        "BEGIN", first.transactionId(), transaction,
                                        commitTimeMicros, resumeOffsetLsn,
                                        resumeEmittedLsn, changes),
                                new TransactionResume(transaction.transactionId(), 0)));
                    }
                    int dataIndex = chunkIndex - (emitBegin ? 1 : 0);
                    if (dataIndex < dataRanges.size()) {
                        SpillableList.Range range = dataRanges.get(dataIndex);
                        boolean finalDataChunk = dataIndex == dataRanges.size() - 1;
                        return () -> materializeRange(
                                transaction.changes.readRange(range), transaction,
                                range.fromIndex(), commitTimeMicros, previousSourceLsn,
                                resumeOffsetLsn, resumeEmittedLsn,
                                checkpointLsn, durableOffsetLsn,
                                finalDataChunk && !emitEnd, incrementalState);
                    }
                    return () -> List.of(transactionBoundaryRecord(
                            "END", transactionBoundaryId, transaction, commitTimeMicros,
                            checkpointLsn, durableOffsetLsn, changes));
                });
    }

    private static long estimatedBytes(List<PendingChange> changes) {
        long total = 0L;
        for (PendingChange change : changes) {
            total = Math.addExact(total, estimatedBytes(change));
        }
        return total;
    }

    private void pollKafkaSignals() {
        KafkaSignalChannel channel = kafkaSignalChannel;
        if (channel == null || kafkaSignalCommitPending) {
            return;
        }
        KafkaSignalChannel.Batch batch = channel.poll();
        if (batch.hadRecords() && batch.requests().isEmpty()) {
            channel.commit();
            return;
        }
        pendingKafkaSignals.addAll(batch.requests());
        kafkaSignalPending = !pendingKafkaSignals.isEmpty();
        kafkaSignalCommitPending = batch.hadRecords();
        kafkaSignalCheckpointReady = false;
    }

    private void applyPendingKafkaSignals() {
        if (snapshotState != null || transaction != null) {
            return;
        }
        while (!pendingKafkaSignals.isEmpty()) {
            SnapshotSignal.Request request = pendingKafkaSignals.removeFirst();
            handleKafkaSignal(request);
            kafkaSignalPending = !pendingKafkaSignals.isEmpty();
        }
    }

    private void commitKafkaSignalsIfCheckpointed() {
        KafkaSignalChannel channel = kafkaSignalChannel;
        if (channel != null && kafkaSignalCommitPending && kafkaSignalCheckpointReady
                && pendingKafkaSignals.isEmpty()) {
            channel.commit();
            kafkaSignalCommitPending = false;
            kafkaSignalCheckpointReady = false;
            kafkaSignalPending = false;
        }
    }

    private void markKafkaSignalCheckpoint(List<SourceRecord> records) {
        if (!kafkaSignalCommitPending || kafkaSignalCheckpointReady
                || !pendingKafkaSignals.isEmpty() || records == null || records.isEmpty()) {
            return;
        }
        String currentState = incrementalProgressJson(incrementalSnapshotState);
        for (int index = records.size() - 1; index >= 0; index--) {
            Object stored = records.get(index).sourceOffset().get(
                    "incremental_snapshot_state");
            String recordState = stored == null ? null : stored.toString();
            if (Objects.equals(recordState, currentState)) {
                kafkaSignalCheckpointReady = true;
                return;
            }
        }
    }

    private void handleKafkaSignal(SnapshotSignal.Request request) {
        validateSnapshotRequest(request);
        if (request.stop()) {
            stopIncrementalSnapshot(request);
            return;
        }
        if (request.type() == SnapshotSignal.Type.INCREMENTAL) {
            if (request.dataCollections().isEmpty()) {
                return;
            }
            if (incrementalSnapshotState != null) {
                throw new ConnectException(
                        "An incremental snapshot is already active on the native path");
            }
            try {
                startIncrementalSnapshot(request);
            } catch (SQLException error) {
                throw new ConnectException(
                        "Cannot initialize the PostgreSQL incremental snapshot", error);
            }
            return;
        }
        if (blockingSnapshotRequest != null || incrementalSnapshotState != null) {
            throw new ConnectException("A snapshot is already active on the native path");
        }
        blockingSnapshotRequest = request;
    }

    private DetectedSnapshotSignal snapshotRequest(List<PendingChange> changes) {
        if (!optimizedSettings.sourceSignalingEnabled()) {
            return null;
        }
        DetectedSnapshotSignal detected = null;
        for (int index = 0; index < changes.size(); index++) {
            PendingChange change = changes.get(index);
            if (!"c".equals(change.operation()) || change.relation() == null
                    || !isSignalRelation(change.relation())) {
                continue;
            }
            Map<String, Object> signalRow = row(
                    change.relation(), change.after(), null, true, null);
            var parsed = SnapshotSignal.parse(signalRow);
            if (parsed.isEmpty()) {
                continue;
            }
            if (detected != null) {
                throw new ConnectException(
                        "A transaction may contain only one snapshot signal on the native path");
            }
            detected = new DetectedSnapshotSignal(index, parsed.get());
        }
        return detected;
    }

    private void validateSnapshotRequest(SnapshotSignal.Request request) {
        for (String collection : request.dataCollections()) {
            if (!includedTables.contains(QualifiedTable.parse(collection))) {
                throw new ConnectException("Snapshot data collection " + collection
                        + " must be an exact table from table.include.list");
            }
        }
        if (request.type() == SnapshotSignal.Type.BLOCKING
                && !request.additionalConditions().isEmpty()) {
            throw new ConnectException(
                    "Blocking snapshots do not support additional-conditions on the native path");
        }
    }

    private boolean isSignalRelation(RelationContext relation) {
        return optimizedSettings.signalingEnabled()
                && optimizedSettings.signalTable().matches(
                relation.message().namespace(), relation.message().name());
    }

    private void startIncrementalSnapshot(SnapshotSignal.Request request) throws SQLException {
        if (request.stop() || request.type() != SnapshotSignal.Type.INCREMENTAL) {
            throw new ConnectException("Expected an incremental snapshot request");
        }
        if (!optimizedSettings.readOnly() && !optimizedSettings.signalingEnabled()) {
            throw new ConnectException("A writable incremental snapshot requires "
                    + OptimizedSettings.SIGNAL_DATA_COLLECTION);
        }
        incrementalChunkSetup = true;
        try {
            List<IncrementalCollectionState> collections = new ArrayList<>(
                    request.dataCollections().size());
            for (String collection : request.dataCollections()) {
                RelationContext relation = loadSnapshotRelation(collection);
                List<ColumnRuntime> keys = relation.columns().stream()
                        .filter(column -> column.metadata().key())
                        .toList();
                if (keys.isEmpty()) {
                    throw new ConnectException("Incremental snapshot requires a primary or replica "
                            + "identity key for " + collection);
                }
                String additionalCondition = request.additionalConditions().get(collection);
                collections.add(new IncrementalCollectionState(
                        relation, loadMaximumKey(relation, additionalCondition), null,
                        additionalCondition));
            }
            incrementalSnapshotState = new IncrementalSnapshotState(List.copyOf(collections));
        } finally {
            incrementalChunkSetup = false;
        }
    }

    private void stopIncrementalSnapshot(SnapshotSignal.Request request) {
        if (!request.stop() || request.type() != SnapshotSignal.Type.INCREMENTAL) {
            throw new ConnectException("Expected an incremental stop request");
        }
        IncrementalSnapshotState state = incrementalSnapshotState;
        if (state != null) {
            state.stop(request.dataCollections(), incrementalWindow != null);
            if (incrementalWindow == null && state.complete()) {
                incrementalSnapshotState = null;
            }
        }
    }

    private void openNextIncrementalWindow() throws SQLException {
        IncrementalSnapshotState state = incrementalSnapshotState;
        if (state == null || incrementalWindow != null) {
            return;
        }
        incrementalChunkSetup = true;
        try {
            while (!state.complete() && (state.shouldSkipCurrentCollection()
                    || state.current().maximumKey() == null)) {
                state.advanceCollection();
            }
            if (state.complete()) {
                incrementalSnapshotState = null;
                return;
            }

            IncrementalCollectionState collection = state.current();
            long windowId = ++incrementalWindowSequence;
            String watermarkId = UUID.randomUUID().toString();
            ReadOnlyWatermark lowWatermark = optimizedSettings.readOnly()
                    ? readOnlyWatermark() : null;
            String openTimestamp = null;
            if (!optimizedSettings.readOnly()) {
                openTimestamp = Instant.now().toString();
                emitIncrementalWatermark(watermarkId + "-open", "snapshot-window-open",
                        "{\"openWindowTimestamp\": \"" + openTimestamp + "\"}");
            }
            IncrementalChunk chunk = readIncrementalChunk(collection);
            ReadOnlyWatermark highWatermark = optimizedSettings.readOnly()
                    ? readOnlyWatermark() : null;
            long snapshotTimeMicros = highWatermark == null
                    ? currentPostgresMicros() : highWatermark.timestampMicros();
            IncrementalWindow window = new IncrementalWindow(
                    windowId, watermarkId, snapshotTimeMicros, collection,
                    chunk.rows(), chunk.lastKey(), chunk.lastChunk(), chunk.estimatedBytes(),
                    lowWatermark, highWatermark);
            if (optimizedSettings.readOnly()) {
                window.open(true);
            }
            incrementalWindow = window;
            if (!optimizedSettings.readOnly()) {
                String closeTimestamp = Instant.now().toString();
                emitIncrementalWatermark(watermarkId + "-close", "snapshot-window-close",
                        "{\"openWindowTimestamp\": \"" + openTimestamp
                                + "\", \"closeWindowTimestamp\": \"" + closeTimestamp + "\"}");
            }
        } finally {
            incrementalChunkSetup = false;
        }
    }

    private void emitIncrementalWatermark(String id, String type, String data)
            throws SQLException {
        String table = optimizedSettings.signalTable().sqlName();
        try (PreparedStatement statement = controlConnection.prepareStatement(
                "INSERT INTO " + table + " (id,type,data) VALUES (?,?,?)")) {
            statement.setString(1, id);
            statement.setString(2, type);
            statement.setString(3, data);
            if (statement.executeUpdate() != 1) {
                throw new ConnectException("PostgreSQL did not persist an incremental watermark");
            }
        }
    }

    private ReadOnlyWatermark readOnlyWatermark() throws SQLException {
        try (Statement statement = controlConnection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT txid_snapshot_xmax(txid_current_snapshot())::bigint, "
                             + "pg_current_wal_lsn()::text, "
                             + "(EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint")) {
            if (!result.next()) {
                throw new ConnectException("Cannot read PostgreSQL read-only snapshot watermark");
            }
            return new ReadOnlyWatermark(
                    result.getLong(1),
                    LogSequenceNumber.valueOf(result.getString(2)).asLong(),
                    result.getLong(3));
        }
    }

    private List<Object> loadMaximumKey(RelationContext relation, String additionalCondition)
            throws SQLException {
        List<ColumnRuntime> keys = relation.columns().stream()
                .filter(column -> column.metadata().key())
                .toList();
        Relation message = relation.message();
        String keyProjection = keys.stream()
                .map(column -> quoteIdentifier(column.message().name()))
                .reduce((left, right) -> left + "," + right)
                .orElseThrow();
        String descending = keys.stream()
                .map(column -> quoteIdentifier(column.message().name()) + " DESC")
                .reduce((left, right) -> left + "," + right)
                .orElseThrow();
        String query = "SELECT " + keyProjection + " FROM "
                + quoteIdentifier(message.namespace()) + "." + quoteIdentifier(message.name())
                + (additionalCondition == null ? "" : " WHERE (" + additionalCondition + ")")
                + " ORDER BY " + descending + " LIMIT 1";
        try (PreparedStatement statement = controlConnection.prepareStatement(query);
             ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                return null;
            }
            return resultKey(result, 1, keys.size());
        }
    }

    private IncrementalChunk readIncrementalChunk(IncrementalCollectionState collection)
            throws SQLException {
        RelationContext relation = collection.relation();
        List<ColumnRuntime> keys = relation.columns().stream()
                .filter(column -> column.metadata().key())
                .toList();
        String sourceAlias = "src";
        String projection = relation.columns().stream()
                .map(column -> qualifiedColumn(sourceAlias, column) + "::text")
                .reduce((left, right) -> left + "," + right)
                .orElseThrow();
        String keyProjection = keys.stream()
                .map(column -> qualifiedColumn(sourceAlias, column))
                .reduce((left, right) -> left + "," + right)
                .orElseThrow();
        String keyExpression = keys.size() == 1
                ? keyProjection
                : "(" + keyProjection + ")";
        String parameters = parameterExpression(keys);
        List<String> predicates = new ArrayList<>();
        if (collection.lastKey() != null) {
            predicates.add(keyExpression + " > " + parameters);
        }
        predicates.add(keyExpression + " <= " + parameters);
        if (collection.additionalCondition() != null) {
            predicates.add("(" + collection.additionalCondition() + ")");
        }
        Relation message = relation.message();
        String query = "SELECT " + projection + "," + keyProjection + " FROM "
                + quoteIdentifier(message.namespace()) + "." + quoteIdentifier(message.name())
                + " AS " + sourceAlias + " WHERE " + String.join(" AND ", predicates)
                + " ORDER BY " + keyProjection + " LIMIT ?";

        LinkedHashMap<String, SnapshotRow> rows = new LinkedHashMap<>();
        List<Object> lastKey = collection.lastKey();
        long estimatedBytes = 0L;
        try (PreparedStatement statement = controlConnection.prepareStatement(query)) {
            int parameter = 1;
            if (collection.lastKey() != null) {
                parameter = bindKey(statement, parameter, collection.lastKey(), keys);
            }
            parameter = bindKey(statement, parameter, collection.maximumKey(), keys);
            statement.setInt(parameter, optimizedSettings.incrementalSnapshotChunkSize());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    List<TupleValue> values = new ArrayList<>(relation.columns().size());
                    for (int index = 1; index <= relation.columns().size(); index++) {
                        String value = result.getString(index);
                        values.add(value == null
                                ? TupleValue.nullValue()
                                : new TupleValue(ValueKind.TEXT,
                                value.getBytes(StandardCharsets.UTF_8)));
                    }
                    SnapshotRow row = new SnapshotRow(
                            relation, new TupleData(List.copyOf(values)));
                    estimatedBytes = Math.addExact(estimatedBytes, tupleBytes(row.tuple()));
                    if (estimatedBytes > optimizedSettings.maxInflightBytes()) {
                        throw new ConnectException("Incremental snapshot chunk exceeds "
                                + OptimizedSettings.MAX_INFLIGHT_BYTES);
                    }
                    rows.put(snapshotKey(row), row);
                    lastKey = resultKey(result, relation.columns().size() + 1, keys.size());
                }
            }
        }
        return new IncrementalChunk(rows, lastKey, rows.isEmpty(), estimatedBytes);
    }

    private static String qualifiedColumn(String alias, ColumnRuntime column) {
        return alias + "." + quoteIdentifier(column.message().name());
    }

    private static String parameterExpression(List<ColumnRuntime> columns) {
        return typedParameterExpression(columns.stream()
                .map(ColumnRuntime::databaseType)
                .toList());
    }

    static String typedParameterExpression(List<String> databaseTypes) {
        String parameters = databaseTypes.stream()
                .map(databaseType -> {
                    if (databaseType == null || databaseType.isBlank()) {
                        throw new ConnectException(
                                "Cannot determine an incremental snapshot key database type");
                    }
                    return "CAST(? AS " + databaseType + ")";
                })
                .reduce((left, right) -> left + "," + right)
                .orElseThrow();
        return databaseTypes.size() == 1 ? parameters : "(" + parameters + ")";
    }

    private static int bindKey(PreparedStatement statement, int first, List<Object> values,
                               List<ColumnRuntime> columns)
            throws SQLException {
        if (values.size() != columns.size()) {
            throw new ConnectException("Incremental snapshot key value count " + values.size()
                    + " does not match key column count " + columns.size());
        }
        int parameter = first;
        for (Object value : values) {
            statement.setString(parameter++, Objects.toString(value));
        }
        return parameter;
    }

    private static List<Object> resultKey(ResultSet result, int first, int count)
            throws SQLException {
        List<Object> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            String value = result.getString(first + index);
            if (value == null) {
                throw new ConnectException("Incremental snapshot key columns must not be null");
            }
            values.add(value);
        }
        return List.copyOf(values);
    }

    private String snapshotKey(SnapshotRow snapshotRow) {
        Map<String, Object> snapshot = row(
                snapshotRow.relation(), snapshotRow.tuple(), null, true, null);
        return snapshotKey(snapshotRow.relation(), snapshot);
    }

    private String snapshotKey(RelationContext relation, Map<String, Object> value) {
        Map<String, Object> key = key(relation, value);
        if (key == null) {
            throw new ConnectException("Incremental snapshot relation has no usable key: "
                    + relation.message().qualifiedName());
        }
        return qualifiedTable(relation).canonicalName() + ":"
                + Base64.getEncoder().encodeToString(jsonSerializer.key("", key));
    }

    private void deduplicateIncrementalWindow(List<PendingChange> changes) {
        IncrementalWindow window = incrementalWindow;
        if (window == null || !window.open()) {
            return;
        }
        for (PendingChange change : changes) {
            RelationContext relation = change.relation();
            if (relation == null || !qualifiedTable(relation).equals(
                    qualifiedTable(window.collection().relation()))) {
                continue;
            }
            if ("t".equals(change.operation())) {
                window.rows().clear();
                continue;
            }
            removeIncrementalKey(window, relation, change.before(), change.oldTupleKind());
            removeIncrementalKey(window, relation, change.after(), null);
        }
    }

    private void removeIncrementalKey(IncrementalWindow window, RelationContext relation,
                                      TupleData tuple, Character tupleKind) {
        if (tuple == null) {
            return;
        }
        Map<String, Object> value = row(relation, tuple, tupleKind, false, null);
        window.rows().remove(snapshotKey(relation, value));
    }

    private IncrementalBoundary incrementalBoundary(List<PendingChange> changes) {
        IncrementalWindow window = incrementalWindow;
        if (window == null) {
            return null;
        }
        IncrementalBoundary detected = null;
        for (PendingChange change : changes) {
            if (!"c".equals(change.operation()) || change.relation() == null
                    || !isSignalRelation(change.relation())) {
                continue;
            }
            Map<String, Object> signalRow = row(
                    change.relation(), change.after(), null, true, null);
            String id = Objects.toString(signalRow.get("id"), "");
            String type = Objects.toString(signalRow.get("type"), "");
            IncrementalBoundary.Type boundaryType = null;
            if ((window.watermarkId() + "-open").equals(id)
                    && "snapshot-window-open".equals(type)) {
                boundaryType = IncrementalBoundary.Type.OPEN;
            } else if ((window.watermarkId() + "-close").equals(id)
                    && "snapshot-window-close".equals(type)) {
                boundaryType = IncrementalBoundary.Type.CLOSE;
            }
            if (boundaryType != null) {
                if (detected != null) {
                    throw new ConnectException(
                            "An incremental watermark transaction must contain one boundary");
                }
                detected = new IncrementalBoundary(window, boundaryType);
            }
        }
        return detected;
    }

    private IncrementalBoundary readOnlyIncrementalBoundary(
            long transactionId, long durableOffsetLsn) {
        IncrementalWindow window = incrementalWindow;
        if (!optimizedSettings.readOnly() || window == null || !window.readOnly()
                || !window.open()) {
            return null;
        }
        long expandedTransactionId = expandTransactionId(
                transactionId, window.highTransactionId());
        if (expandedTransactionId < window.highTransactionId()
                && durableOffsetLsn < window.highLsn()) {
            return null;
        }
        return new IncrementalBoundary(window, IncrementalBoundary.Type.CLOSE);
    }

    private void closeReadOnlyWindowIfCaughtUp(long receivedLsn) {
        IncrementalWindow window = incrementalWindow;
        if (!optimizedSettings.readOnly() || window == null || !window.readOnly()
                || !window.open() || transaction != null
                || receivedLsn < window.highLsn()) {
            return;
        }
        ClosedIncrementalWindow closed = closeIncrementalWindow(
                window, window.highLsn(), window.highLsn());
        latestProcessedLsn.accumulateAndGet(window.highLsn(), Math::max);
        previousLsn = Math.max(previousLsn, window.highLsn());
        submitClosedIncrementalWindow(closed);
    }

    static long expandTransactionId(long transactionId, long referenceTransactionId) {
        final long epochSize = 1L << 32;
        final long halfEpoch = 1L << 31;
        long candidate = (referenceTransactionId & -epochSize)
                | (transactionId & (epochSize - 1L));
        long difference = candidate - referenceTransactionId;
        if (difference < -halfEpoch) {
            return candidate + epochSize;
        }
        if (difference > halfEpoch) {
            return candidate - epochSize;
        }
        return candidate;
    }

    private ClosedIncrementalWindow processIncrementalBoundary(
            IncrementalBoundary boundary, long sourceLsn, long durableOffsetLsn) {
        if (boundary == null || incrementalWindow != boundary.window()) {
            return null;
        }
        IncrementalWindow window = boundary.window();
        if (boundary.type() == IncrementalBoundary.Type.OPEN) {
            if (window.open()) {
                throw new ConnectException("Incremental window opened twice: " + window.id());
            }
            window.open(true);
            return null;
        }
        if (!window.open()) {
            throw new ConnectException("Incremental window closed before it opened: "
                    + window.id());
        }
        return closeIncrementalWindow(window, sourceLsn, durableOffsetLsn);
    }

    private ClosedIncrementalWindow closeIncrementalWindow(
            IncrementalWindow window, long highLsn, long durableOffsetLsn) {
        if (incrementalWindow != window) {
            return null;
        }
        List<SnapshotRow> rows = List.copyOf(window.rows().values());
        IncrementalSnapshotState state = incrementalSnapshotState;
        if (state != null) {
            if (state.stopsCurrentCollection()) {
                state.finishStoppedCurrentCollection();
            } else {
                state.current().lastKey(window.lastKey());
                boolean reachedMaximum = Objects.equals(
                        window.lastKey(), state.current().maximumKey());
                boolean collectionComplete = window.lastChunk()
                        || reachedMaximum
                        && (state.hasNextCollection() || !state.terminalWindowRequired());
                if (collectionComplete) {
                    state.advanceCollection();
                }
            }
            if (state.complete()) {
                incrementalSnapshotState = null;
            }
        }
        incrementalWindow = null;
        return new ClosedIncrementalWindow(
                rows, window.readOnly() ? window.lowLsn() : highLsn,
                highLsn, durableOffsetLsn, window.snapshotTimeMicros(),
                window.estimatedBytes(), incrementalProgressJson(incrementalSnapshotState));
    }

    private void submitClosedIncrementalWindow(ClosedIncrementalWindow window) {
        if (window == null || window.rows().isEmpty()) {
            return;
        }
        List<Callable<List<SourceRecord>>> chunks = new ArrayList<>();
        int chunkSize = optimizedSettings.processingChunkSize();
        for (int start = 0; start < window.rows().size(); start += chunkSize) {
            int fromIndex = start;
            int toIndex = Math.min(start + chunkSize, window.rows().size());
            chunks.add(() -> materializeIncrementalRange(
                    window.rows(), fromIndex, toIndex, window.lowLsn(), window.highLsn(),
                    window.durableOffsetLsn(), window.snapshotTimeMicros(),
                    window.resumeState()));
        }
        workQueue.submit(new CommittedTransaction(++transactionSequence, null),
                window.rows().size(), window.estimatedBytes(), chunks);
    }

    private List<SourceRecord> materializeIncrementalRange(
            List<SnapshotRow> rows, int fromIndex, int toIndex,
            long lowLsn, long highLsn, long offsetLsn, long snapshotTimeMicros,
            String resumeState) {
        List<SourceRecord> records = new ArrayList<>(toIndex - fromIndex);
        for (int index = fromIndex; index < toIndex; index++) {
            records.add(materializeIncremental(
                    rows.get(index), lowLsn, highLsn, offsetLsn, snapshotTimeMicros,
                    resumeState));
        }
        return records;
    }

    private SourceRecord materializeIncremental(SnapshotRow snapshotRow,
                                                long lowLsn, long highLsn,
                                                long offsetLsn,
                                                long snapshotTimeMicros,
                                                String resumeState) {
        RelationContext relation = snapshotRow.relation();
        Map<String, Object> completeAfter = row(
                relation, snapshotRow.tuple(), null, true, null);
        Map<String, Object> recordKey = key(relation, completeAfter);
        Map<String, Object> after = filteredValue(relation, completeAfter);
        Relation relationMessage = relation.message();
        Map<String, Object> value = DebeziumEnvelope.createIncrementalSnapshot(
                after, config.sourceVersion(), config.topicPrefix(), config.database(),
                relationMessage.namespace(), relationMessage.name(), lowLsn, highLsn,
                snapshotTimeMicros);
        Map<String, Object> sourceOffset = sourceOffset(offsetLsn, resumeState);
        String topic = topicNames.dataChangeTopic(
                relationMessage.namespace(), relationMessage.name());
        long timestamp = ((Number) value.get("ts_ms")).longValue();
        return sourceRecord(sourceOffset, topic, relation.schemas().key(), recordKey,
                relation.schemas().envelope(), value, timestamp);
    }

    private List<SourceRecord> materializeRange(List<PendingChange> changes,
                                                List<EventMetadata> eventMetadata,
                                                int fromIndex, int toIndex,
                                                Transaction transaction, long commitTimeMicros,
                                                long previousSourceLsn, long offsetLsn,
                                                long emittedLsn,
                                                String incrementalState) {
        List<SourceRecord> records = new ArrayList<>(toIndex - fromIndex);
        for (int index = fromIndex; index < toIndex; index++) {
            PendingChange change = changes.get(index);
            EventMetadata metadata = eventMetadata.get(index);
            try {
                SourceRecord record = materialize(change, transaction, commitTimeMicros,
                        previousSourceLsn, offsetLsn, emittedLsn, metadata,
                        incrementalState);
                records.add(record);
                if (change.operation().equals("d") && config.tombstonesOnDelete()) {
                    records.add(new SourceRecord(
                            record.sourcePartition(), record.sourceOffset(), record.topic(),
                            record.kafkaPartition(), record.keySchema(), record.key(), null, null,
                            record.timestamp()));
                }
            } catch (RuntimeException error) {
                handleEventProcessingFailure(change, metadata, error);
            }
        }
        return records;
    }

    private List<SourceRecord> materializeRange(List<PendingChange> changes,
                                                Transaction transaction,
                                                int absoluteFromIndex,
                                                long commitTimeMicros,
                                                long previousSourceLsn,
                                                long resumeOffsetLsn,
                                                long resumeEmittedLsn,
                                                long finalOffsetLsn,
                                                long finalEmittedLsn,
                                                boolean completesTransaction,
                                                String incrementalState) {
        List<SourceRecord> records = new ArrayList<>(changes.size());
        for (int index = 0; index < changes.size(); index++) {
            PendingChange change = changes.get(index);
            EventMetadata metadata = eventMetadata(
                    change, transaction.transactionId(), absoluteFromIndex + index,
                    finalEmittedLsn);
            try {
                SourceRecord record = materialize(change, transaction, commitTimeMicros,
                        previousSourceLsn, resumeOffsetLsn, resumeEmittedLsn, metadata,
                        incrementalState, null);
                records.add(withTransactionProgress(
                        record, resumeOffsetLsn, resumeEmittedLsn,
                        transaction.transactionId(), absoluteFromIndex + index + 1, false));
                if (change.operation().equals("d") && config.tombstonesOnDelete()) {
                    SourceRecord tombstone = new SourceRecord(
                            record.sourcePartition(), record.sourceOffset(), record.topic(),
                            record.kafkaPartition(), record.keySchema(), record.key(), null, null,
                            record.timestamp());
                    records.add(withTransactionProgress(
                            tombstone, resumeOffsetLsn, resumeEmittedLsn,
                            transaction.transactionId(), absoluteFromIndex + index + 1, false));
                }
            } catch (RuntimeException error) {
                handleEventProcessingFailure(change, metadata, error);
            }
        }
        if (completesTransaction && !records.isEmpty()) {
            records.set(records.size() - 1,
                    withTransactionProgress(
                            records.getLast(), finalOffsetLsn, finalEmittedLsn,
                            transaction.transactionId(), absoluteFromIndex + changes.size(), true));
        }
        return records;
    }

    private void handleEventProcessingFailure(PendingChange change, EventMetadata metadata,
                                              RuntimeException error) {
        String lsn = LogSequenceNumber.valueOf(metadata.eventLsn()).asString();
        String collection = change.relation() == null
                ? "message" : change.relation().message().qualifiedName();
        String description = "PostgreSQL event at LSN " + lsn + " for " + collection;
        switch (eventFailureHandlingMode) {
            case FAIL -> throw new ConnectException("Cannot process " + description, error);
            case WARN -> LOGGER.warn("Skipping {} after processing failure", description, error);
            case SKIP -> {
                // Intentionally silent: the public skip contract omits the event and continues.
            }
        }
    }

    private SourceRecord materialize(PendingChange change, Transaction tx, long commitTimeMicros,
                                     long previousSourceLsn, long offsetLsn,
                                     long emittedLsn,
                                     EventMetadata eventMetadata) {
        return materialize(change, tx, commitTimeMicros, previousSourceLsn, offsetLsn, emittedLsn,
                eventMetadata, null, null);
    }

    private SourceRecord materialize(PendingChange change, Transaction tx, long commitTimeMicros,
                                     long previousSourceLsn, long offsetLsn,
                                     long emittedLsn,
                                     EventMetadata eventMetadata,
                                     String incrementalState) {
        return materialize(change, tx, commitTimeMicros, previousSourceLsn, offsetLsn,
                emittedLsn, eventMetadata, incrementalState, null);
    }

    private SourceRecord materialize(PendingChange change, Transaction tx, long commitTimeMicros,
                                     long previousSourceLsn, long offsetLsn,
                                     long emittedLsn,
                                     EventMetadata eventMetadata,
                                     String incrementalState,
                                     TransactionResume resume) {
        if (change.logicalMessage() != null) {
            return materializeLogicalMessage(change, tx, commitTimeMicros,
                    previousSourceLsn, offsetLsn, emittedLsn, eventMetadata);
        }
        RelationContext relation = change.relation();
        Map<String, Object> completeBefore = null;
        Map<String, Object> completeAfter = null;
        Map<String, Object> before = null;
        Map<String, Object> after = null;
        Map<String, Object> key = null;

        switch (change.operation()) {
            case "c" -> {
                completeAfter = row(relation, change.after(), null, true, null);
                key = key(relation, completeAfter);
            }
            case "u" -> {
                if (change.before() != null) {
                    completeBefore = row(
                            relation, change.before(), change.oldTupleKind(), true, null);
                }
                Map<String, Object> toastFallback = change.oldTupleKind() != null
                        && change.oldTupleKind() == 'O' ? completeBefore : null;
                completeAfter = row(relation, change.after(), null, true, toastFallback);
                key = key(relation, completeAfter);
            }
            case "d" -> {
                completeBefore = row(
                        relation, change.before(), change.oldTupleKind(), true, null);
                key = key(relation, completeBefore);
            }
            case "t" -> { }
            default -> throw new IllegalStateException("Unknown operation " + change.operation());
        }
        before = filteredValue(relation, completeBefore);
        after = filteredValue(relation, completeAfter);

        Relation relationMessage = relation.message();
        Map<String, Object> transactionMetadata = null;
        if (config.provideTransactionMetadata()) {
            transactionMetadata = new LinkedHashMap<>();
            transactionMetadata.put("id", eventMetadata.transactionId());
            transactionMetadata.put("total_order", eventMetadata.totalOrder());
            transactionMetadata.put("data_collection_order", eventMetadata.dataCollectionOrder());
        }
        Map<String, Object> value = DebeziumEnvelope.create(
                before,
                after,
                change.operation(),
                config.sourceVersion(),
                config.topicPrefix(),
                config.database(),
                relationMessage.namespace(),
                relationMessage.name(),
                tx.transactionId(),
                previousSourceLsn,
                eventMetadata.eventLsn(),
                commitTimeMicros,
                transactionMetadata,
                tx.originName(),
                tx.originLsn());

        Map<String, Object> sourceOffset = sourceOffset(
                offsetLsn, emittedLsn, incrementalState);
        addTransactionResume(sourceOffset, resume);
        sourceOffset.put("txId", tx.transactionId());
        String topic = topicNames.dataChangeTopic(
                relationMessage.namespace(), relationMessage.name());
        long timestamp = ((Number) value.get("ts_ms")).longValue();
        return sourceRecord(sourceOffset, topic, relation.schemas().key(), key,
                relation.schemas().envelope(), value, timestamp);
    }

    private SourceRecord materializeLogicalMessage(PendingChange change, Transaction tx,
                                                   long commitTimeMicros,
                                                   long previousSourceLsn, long offsetLsn,
                                                   long emittedLsn,
                                                   EventMetadata eventMetadata) {
        LogicalMessage message = change.logicalMessage();
        Long transactionId = tx == null ? null : tx.transactionId();
        Map<String, Object> key = Map.of("prefix", message.prefix());
        String content = Base64.getEncoder().encodeToString(message.content());
        Map<String, Object> transactionMetadata = null;
        if (config.provideTransactionMetadata() && transactionId != null) {
            transactionMetadata = new LinkedHashMap<>();
            transactionMetadata.put("id", eventMetadata.transactionId());
            transactionMetadata.put("total_order", eventMetadata.totalOrder());
            transactionMetadata.put(
                    "data_collection_order", eventMetadata.dataCollectionOrder());
        }
        Map<String, Object> value = DebeziumEnvelope.createLogicalMessage(
                message.prefix(), content, config.sourceVersion(), config.topicPrefix(),
                config.database(), transactionId, previousSourceLsn, eventMetadata.eventLsn(),
                commitTimeMicros, transactionMetadata,
                tx == null ? null : tx.originName(),
                tx == null ? null : tx.originLsn());
        Map<String, Object> sourceOffset = new LinkedHashMap<>();
        sourceOffset.put("lsn", offsetLsn);
        sourceOffset.put("last_emitted_lsn", emittedLsn);
        if (transactionId != null) {
            sourceOffset.put("txId", transactionId);
        }
        String topic = config.topicPrefix() + ".message";
        long timestamp = ((Number) value.get("ts_ms")).longValue();
        return sourceRecord(
                sourceOffset, topic, DebeziumSchemaFactory.logicalMessageKey(), key,
                DebeziumSchemaFactory.logicalMessageValue(), value, timestamp);
    }

    private List<EventMetadata> eventMetadata(List<PendingChange> changes,
                                              long transactionId, long fallbackLsn) {
        List<EventMetadata> metadata = new ArrayList<>(changes.size());
        Map<String, Long> collectionOrder = new LinkedHashMap<>();
        for (int index = 0; index < changes.size(); index++) {
            PendingChange change = changes.get(index);
            String collection = change.relation() == null
                    ? "message" : change.relation().message().qualifiedName();
            long order = collectionOrder.merge(collection, 1L, Long::sum);
            long eventLsn = change.eventLsn() > 0L ? change.eventLsn() : fallbackLsn;
            metadata.add(new EventMetadata(
                    eventLsn,
                    transactionId(transactionId, eventLsn),
                    index + 1L,
                    order));
        }
        return List.copyOf(metadata);
    }

    private EventMetadata eventMetadata(PendingChange change, long transactionId,
                                        int zeroBasedIndex, long fallbackLsn) {
        long eventLsn = change.eventLsn() > 0L ? change.eventLsn() : fallbackLsn;
        return new EventMetadata(eventLsn, transactionId(transactionId, eventLsn),
                zeroBasedIndex + 1L, change.dataCollectionOrder());
    }

    private SourceRecord transactionBoundaryRecord(String status, String id, Transaction tx,
                                                   long commitTimeMicros, long offsetLsn,
                                                   long emittedLsn,
                                                   List<PendingChange> changes) {
        String topic = topicNames.transactionTopic();
        Map<String, Object> key = Map.of("id", id);
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("status", status);
        value.put("id", id);
        if ("BEGIN".equals(status)) {
            value.put("event_count", null);
            value.put("data_collections", null);
        } else {
            value.put("event_count", (long) changes.size());
            value.put("data_collections", dataCollections(changes));
        }
        long timestamp = postgresTimestampMillis(commitTimeMicros);
        value.put("ts_ms", timestamp);
        Map<String, Object> sourceOffset = new LinkedHashMap<>();
        sourceOffset.put("lsn", offsetLsn);
        sourceOffset.put("last_emitted_lsn", emittedLsn);
        sourceOffset.put("txId", tx.transactionId());
        return sourceRecord(
                sourceOffset, topic, DebeziumSchemaFactory.transactionMetadataKey(), key,
                DebeziumSchemaFactory.transactionMetadataValue(), value, timestamp);
    }

    private static List<Map<String, Object>> dataCollections(List<PendingChange> changes) {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (PendingChange change : changes) {
            counts.merge(transactionDataCollection(change), 1L, Long::sum);
        }
        // Debezium exposes its logical-message monitor identifier before relational
        // collections even when a row preceded the message in the transaction.
        if (counts.containsKey(LOGICAL_MESSAGE_DATA_COLLECTION)) {
            Map<String, Long> ordered = new LinkedHashMap<>();
            ordered.put(LOGICAL_MESSAGE_DATA_COLLECTION,
                    counts.remove(LOGICAL_MESSAGE_DATA_COLLECTION));
            ordered.putAll(counts);
            counts = ordered;
        }
        List<Map<String, Object>> collections = new ArrayList<>(counts.size());
        for (Map.Entry<String, Long> entry : counts.entrySet()) {
            Map<String, Object> collection = new LinkedHashMap<>();
            collection.put("data_collection", entry.getKey());
            collection.put("event_count", entry.getValue());
            collections.add(collection);
        }
        return List.copyOf(collections);
    }

    private static String transactionDataCollection(PendingChange change) {
        return change.logicalMessage() == null
                ? change.relation().message().qualifiedName()
                : LOGICAL_MESSAGE_DATA_COLLECTION;
    }

    static String logicalMessageDataCollection() {
        return LOGICAL_MESSAGE_DATA_COLLECTION;
    }

    private static long postgresTimestampMillis(long postgresMicros) {
        return Math.addExact(946_684_800_000L, postgresMicros / 1_000L);
    }

    private static long currentPostgresMicros() {
        return Math.subtractExact(
                Math.multiplyExact(System.currentTimeMillis(), 1_000L),
                946_684_800_000_000L);
    }

    private static String transactionId(long transactionId, long lsn) {
        return transactionId + ":" + lsn;
    }

    private SourceRecord sourceRecord(
            Map<String, Object> sourceOffset, String topic,
            Map<String, Object> keySchema, Object key,
            Map<String, Object> valueSchema, Object value,
            long timestamp) {
        if (optimizedSettings.externalTransformsConfigured()) {
            ConnectSchemaAdapter.AdaptedValue transformedKey = key == null
                    ? null : ConnectSchemaAdapter.adapt(keySchema, key);
            ConnectSchemaAdapter.AdaptedValue transformedValue = value == null
                    ? null : ConnectSchemaAdapter.adapt(valueSchema, value);
            return new SourceRecord(sourcePartition, sourceOffset, topic, null,
                    transformedKey == null ? null : transformedKey.schema(),
                    transformedKey == null ? null : transformedKey.value(),
                    transformedValue == null ? null : transformedValue.schema(),
                    transformedValue == null ? null : transformedValue.value(), timestamp);
        }

        byte[] serializedKey;
        byte[] serializedValue;
        if (optimizedSettings.schemasEnabled()) {
            serializedKey = key == null ? null : jsonSerializer.keyWithSchema(
                    topic, keySchema, key);
            serializedValue = value == null ? null : jsonSerializer.valueWithSchema(
                    topic, valueSchema, value);
        } else {
            serializedKey = key == null ? null : jsonSerializer.key(topic, key);
            serializedValue = value == null ? null : jsonSerializer.value(topic, value);
        }
        return new SourceRecord(sourcePartition, sourceOffset, topic, null,
                null, serializedKey, null, serializedValue, timestamp);
    }

    private static long estimatedBytes(PendingChange change) {
        long total = Math.addExact(512L, tupleBytes(change.before()));
        total = Math.addExact(total, tupleBytes(change.after()));
        if (change.logicalMessage() != null) {
            total = Math.addExact(total, change.logicalMessage().content().length);
        }
        return total;
    }

    private int outputRecordCount(List<PendingChange> changes) {
        int records = 0;
        for (PendingChange change : changes) {
            records = Math.addExact(records, outputRecordCount(change));
        }
        if (!changes.isEmpty() && config.provideTransactionMetadata()) {
            records = Math.addExact(records, 2);
        }
        return records;
    }

    private int outputRecordCount(List<PendingChange> changes, int fromIndex, int toIndex) {
        int records = 0;
        for (int index = fromIndex; index < toIndex; index++) {
            records = Math.addExact(records, outputRecordCount(changes.get(index)));
        }
        return records;
    }

    private int outputRecordCount(PendingChange change) {
        return change.operation().equals("d") && config.tombstonesOnDelete() ? 2 : 1;
    }

    static long checkedTransactionBytes(long currentBytes, long changeBytes, long maximumBytes) {
        if (currentBytes < 0L || changeBytes < 1L || maximumBytes < 1L) {
            throw new IllegalArgumentException("Transaction byte counters must be positive");
        }
        final long nextBytes;
        try {
            nextBytes = Math.addExact(currentBytes, changeBytes);
        } catch (ArithmeticException error) {
            throw new ConnectException("Transaction byte accounting overflow", error);
        }
        if (nextBytes > maximumBytes) {
            throw new ConnectException("A single transaction requires at least " + nextBytes
                    + " estimated bytes, exceeding optimized.max.inflight.bytes=" + maximumBytes);
        }
        return nextBytes;
    }

    private static long tupleBytes(TupleData tuple) {
        if (tuple == null) {
            return 0L;
        }
        long total = 0L;
        for (TupleValue value : tuple.values()) {
            if (value.data() != null) {
                total = Math.addExact(total, value.data().length);
            }
        }
        return total;
    }

    private SpillableList.Codec<PendingChange> pendingChangeCodec(Transaction owner) {
        return new SpillableList.Codec<>() {
            @Override
            public void write(DataOutput output, PendingChange change) throws IOException {
                output.writeUTF(change.operation());
                output.writeInt(owner.relationId(change.relation()));
                output.writeInt(change.oldTupleKind() == null ? -1 : change.oldTupleKind());
                writeTuple(output, change.before());
                writeTuple(output, change.after());
                writeLogicalMessage(output, change.logicalMessage());
                output.writeLong(change.eventLsn());
                output.writeLong(change.dataCollectionOrder());
            }

            @Override
            public PendingChange read(DataInput input) throws IOException {
                String operation = input.readUTF();
                RelationContext relation = owner.relation(input.readInt());
                int tupleKind = input.readInt();
                return new PendingChange(operation, relation,
                        tupleKind < 0 ? null : (char) tupleKind,
                        readTuple(input), readTuple(input), readLogicalMessage(input),
                        input.readLong(), input.readLong());
            }
        };
    }

    private static void writeTuple(DataOutput output, TupleData tuple) throws IOException {
        if (tuple == null) {
            output.writeInt(-1);
            return;
        }
        output.writeInt(tuple.values().size());
        for (TupleValue value : tuple.values()) {
            output.writeByte(value.kind().ordinal());
            writeBytes(output, value.data());
        }
    }

    private static TupleData readTuple(DataInput input) throws IOException {
        int size = input.readInt();
        if (size < 0) {
            return null;
        }
        List<TupleValue> values = new ArrayList<>(size);
        for (int index = 0; index < size; index++) {
            int ordinal = input.readUnsignedByte();
            if (ordinal >= ValueKind.values().length) {
                throw new IOException("Invalid tuple value kind " + ordinal);
            }
            values.add(new TupleValue(ValueKind.values()[ordinal], readBytes(input)));
        }
        return new TupleData(List.copyOf(values));
    }

    private static void writeLogicalMessage(DataOutput output, LogicalMessage message)
            throws IOException {
        output.writeBoolean(message != null);
        if (message == null) {
            return;
        }
        output.writeBoolean(message.transactional());
        output.writeLong(message.lsn());
        output.writeUTF(message.prefix());
        writeBytes(output, message.content());
    }

    private static LogicalMessage readLogicalMessage(DataInput input) throws IOException {
        if (!input.readBoolean()) {
            return null;
        }
        return new LogicalMessage(input.readBoolean(), input.readLong(), input.readUTF(),
                readBytes(input));
    }

    private static void writeBytes(DataOutput output, byte[] value) throws IOException {
        if (value == null) {
            output.writeInt(-1);
            return;
        }
        output.writeInt(value.length);
        output.write(value);
    }

    private static byte[] readBytes(DataInput input) throws IOException {
        int length = input.readInt();
        if (length < 0) {
            return null;
        }
        byte[] value = new byte[length];
        input.readFully(value);
        return value;
    }

    private Map<String, Object> row(RelationContext relation, TupleData tuple, Character tupleKind,
                                    boolean defaultsForMissing, Map<String, Object> unchangedToastFallback) {
        List<ColumnRuntime> columns = relation.columns();
        List<TupleValue> values = tuple.values();
        Map<String, TupleValue> transmitted = new HashMap<>();
        if (tupleKind != null && tupleKind == 'K') {
            List<ColumnRuntime> keyColumns = columns.stream()
                    .filter(column -> column.metadata().key())
                    .toList();
            if (values.size() == columns.size()) {
                // pgoutput protocol v1 can retain the relation's full tuple width for
                // a key tuple and encode non-key positions as NULL placeholders. They
                // mean "not transmitted", not SQL NULL, so do not materialize them.
                for (int index = 0; index < columns.size(); index++) {
                    if (columns.get(index).metadata().key()) {
                        transmitted.put(columns.get(index).message().name(), values.get(index));
                    }
                }
            } else if (values.size() == keyColumns.size()) {
                for (int index = 0; index < values.size(); index++) {
                    transmitted.put(keyColumns.get(index).message().name(), values.get(index));
                }
            } else {
                throw new ConnectException("Key tuple column count " + values.size()
                        + " does not match relation key column count " + keyColumns.size()
                        + " or relation column count " + columns.size());
            }
        } else if (values.size() == columns.size()) {
            for (int index = 0; index < values.size(); index++) {
                transmitted.put(columns.get(index).message().name(), values.get(index));
            }
        } else {
            throw new ConnectException("Tuple column count " + values.size()
                    + " does not match relation " + relation.message().qualifiedName()
                    + " column count " + columns.size());
        }

        Map<String, Object> result = new LinkedHashMap<>();
        for (ColumnRuntime column : columns) {
            TupleValue value = transmitted.get(column.message().name());
            Object converted;
            if (value == null) {
                converted = defaultsForMissing ? valueConverter.defaultValue(
                        column.message(), column.metadata()) : null;
            } else {
                converted = column.unknownDatatype()
                        ? valueConverter.convertUnknown(value, column.metadata())
                        : column.customConverter() == null
                        && optimizedSettings.rawUtf8Enabled()
                        && shouldUseRawUtf8(column.message().typeOid(), value)
                        ? new RawUtf8String(value.data())
                        : valueConverter.convert(column.message(), value, column.metadata());
                if (converted == UnchangedToast.INSTANCE) {
                        converted = unchangedToastFallback != null
                            && unchangedToastFallback.containsKey(column.message().name())
                            ? unchangedToastFallback.get(column.message().name())
                            : "__debezium_unavailable_value";
                }
            }
            if (column.customConverter() != null
                    && converted != null
                    && converted != UnchangedToast.INSTANCE
                    && !"__debezium_unavailable_value".equals(converted)) {
                converted = column.customConverter().convert(converted);
            }
            result.put(column.message().name(), converted);
        }
        return result;
    }

    static boolean shouldUseRawUtf8(long typeOid, TupleValue value) {
        return typeOid == POSTGRES_TEXT_OID
                && value.kind() == ValueKind.TEXT
                && value.data() != null
                && value.data().length >= RAW_UTF8_THRESHOLD_BYTES;
    }

    private Map<String, Object> key(RelationContext relation, Map<String, Object> row) {
        if (row == null) {
            return null;
        }
        Map<String, Object> key = new LinkedHashMap<>();
        for (ColumnRuntime column : relation.columns()) {
            if (column.metadata().key()) {
                key.put(column.message().name(), row.get(column.message().name()));
            }
        }
        if (key.isEmpty()) {
            return null;
        }
        return topicNames.augmentKeyValue(
                relation.message().namespace(), relation.message().name(),
                relation.schemas().key(), key);
    }

    private static Map<String, Object> filteredValue(
            RelationContext relation, Map<String, Object> row) {
        if (row == null) {
            return null;
        }
        Map<String, Object> filtered = new LinkedHashMap<>();
        for (ColumnRuntime column : relation.columns()) {
            if (column.includedInValue()) {
                Object value = row.get(column.message().name());
                filtered.put(column.message().name(), column.valueTransformer() == null
                        ? value : column.valueTransformer().convert(value));
            }
        }
        return filtered;
    }

    private RelationContext loadRelation(Relation relation) throws SQLException {
        Map<String, ColumnMetadata> metadata = new HashMap<>();
        Map<String, String> databaseTypes = new HashMap<>();
        Map<String, Long> baseTypeOids = new HashMap<>();
        Map<String, Integer> effectiveTypeModifiers = new HashMap<>();
        Map<String, Boolean> unknownDatatypes = new HashMap<>();
        Map<String, SourceTypePropagator.JdbcMetadata> jdbcMetadata =
                sourceTypePropagator.enabled()
                        ? loadJdbcColumnMetadata(relation) : Map.of();
        String query = """
                SELECT a.attname,
                       NOT a.attnotnull AS nullable,
                       pg_get_expr(d.adbin, d.adrelid) AS default_expression,
                       CASE
                         WHEN EXISTS (
                           SELECT 1 FROM pg_catalog.pg_index pk
                            WHERE pk.indrelid = c.oid AND pk.indisprimary
                         ) THEN EXISTS (
                           SELECT 1 FROM pg_catalog.pg_index pk
                            WHERE pk.indrelid = c.oid
                              AND pk.indisprimary
                              AND a.attnum = ANY(pk.indkey)
                         )
                         ELSE EXISTS (
                           SELECT 1 FROM pg_catalog.pg_index ri
                            WHERE ri.indrelid = c.oid
                              AND ri.indisreplident
                              AND a.attnum = ANY(ri.indkey)
                         )
                       END AS key_column,
                       t.typname AS type_name,
                       t.typtype::text AS type_kind,
                       t.typcategory = 'A' AS array_type,
                       t.typelem::bigint AS element_type_oid,
                       et.typname AS element_type_name,
                       et.typtype::text AS element_type_kind,
                       pg_catalog.format_type(a.atttypid, a.atttypmod) AS database_type,
                       COALESCE(NULLIF(t.typbasetype, 0), a.atttypid)::bigint AS base_type_oid,
                       CASE WHEN t.typtype = 'd' AND t.typtypmod >= 0
                            THEN t.typtypmod ELSE a.atttypmod
                        END AS effective_type_modifier,
                       bt.typname AS base_type_name,
                       bt.typtype::text AS base_type_kind
                  FROM pg_catalog.pg_attribute a
                  JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                  JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
             LEFT JOIN pg_catalog.pg_type et ON et.oid = t.typelem
             LEFT JOIN pg_catalog.pg_type bt ON bt.oid = NULLIF(t.typbasetype, 0)
             LEFT JOIN pg_catalog.pg_attrdef d
                    ON d.adrelid = a.attrelid AND d.adnum = a.attnum
                 WHERE n.nspname = ?
                   AND c.relname = ?
                   AND a.attnum > 0
                   AND NOT a.attisdropped
              ORDER BY a.attnum
                """;
        try (PreparedStatement statement = controlConnection.prepareStatement(query)) {
            statement.setString(1, relation.namespace());
            statement.setString(2, relation.name());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    metadata.put(result.getString("attname"), new ColumnMetadata(
                            result.getBoolean("nullable"), result.getString("default_expression"),
                            result.getBoolean("key_column"), result.getString("type_name"),
                            result.getBoolean("array_type"),
                            result.getLong("element_type_oid"), result.getString("element_type_name")));
                    databaseTypes.put(result.getString("attname"),
                            result.getString("database_type"));
                    baseTypeOids.put(result.getString("attname"),
                            result.getLong("base_type_oid"));
                    effectiveTypeModifiers.put(result.getString("attname"),
                            result.getInt("effective_type_modifier"));
                    unknownDatatypes.put(result.getString("attname"),
                            !PostgresTypeSupport.isSupported(
                                    result.getString("type_name"),
                                    result.getString("type_kind"),
                                    result.getBoolean("array_type"),
                                    result.getString("element_type_name"),
                                    result.getString("element_type_kind"),
                                    result.getString("base_type_name"),
                                    result.getString("base_type_kind")));
                }
            }
        }

        List<ColumnRuntime> columns = new ArrayList<>(relation.columns().size());
        for (Column column : relation.columns()) {
            ColumnMetadata columnMetadata = metadata.getOrDefault(
                    column.name(), new ColumnMetadata(true, null, column.key(), null, false, 0L, null));
            long baseTypeOid = baseTypeOids.getOrDefault(
                    column.name(), column.typeOid());
            CustomConverters.Binding customConverter = customConverters.binding(
                    relation.namespace(), relation.name(), column, columnMetadata,
                    databaseTypes.get(column.name()),
                    baseTypeOid,
                    valueConverter);
            boolean unknownDatatype = unknownDatatypes.getOrDefault(column.name(), false)
                    && customConverter == null;
            Column valueColumn = valueColumn(
                    column, baseTypeOid,
                    effectiveTypeModifiers.getOrDefault(
                            column.name(), column.typeModifier()));
            Map<String, String> sourceTypeParameters = sourceTypePropagator.parameters(
                    relation.namespace(), relation.name(), column.name(), columnMetadata,
                    jdbcMetadata.get(column.name()));
            columns.add(new ColumnRuntime(
                    valueColumn, columnMetadata, databaseTypes.get(column.name()),
                    baseTypeOid,
                    columnFilter.includes(
                            relation.namespace(), relation.name(), column.name())
                            && (!unknownDatatype || optimizedSettings.includeUnknownDatatypes()),
                    unknownDatatype,
                    customConverter,
                    columnValueTransformers.binding(
                            relation.namespace(), relation.name(), column, baseTypeOid),
                    sourceTypeParameters));
        }
        List<DebeziumSchemaFactory.ColumnDefinition> definitions = columns.stream()
                .map(column -> new DebeziumSchemaFactory.ColumnDefinition(
                        column.message(), column.metadata(), column.includedInValue(),
                        column.customConverter() == null ? null
                                : column.customConverter().schemaDescription(
                                        column.message().name()),
                        mergedSchemaParameters(
                                column.sourceTypeParameters(),
                                column.valueTransformer() == null ? Map.of()
                                        : column.valueTransformer().schemaParameters()),
                        column.unknownDatatype()))
                .toList();
        DebeziumSchemaFactory.RelationSchemas schemas = schemaFactory.relationWithSchemaPrefix(
                topicNames.recordSchemaPrefix(relation.namespace(), relation.name()),
                definitions);
        if (columns.stream().anyMatch(column -> column.metadata().key())) {
            schemas = new DebeziumSchemaFactory.RelationSchemas(
                    topicNames.augmentKeySchema(
                            relation.namespace(), relation.name(), schemas.key()),
                    schemas.envelope());
        }
        return new RelationContext(relation, List.copyOf(columns), schemas);
    }

    static Column valueColumn(Column column, long baseTypeOid) {
        return valueColumn(column, baseTypeOid, column.typeModifier());
    }

    static Column valueColumn(Column column, long baseTypeOid,
                              int effectiveTypeModifier) {
        return column.typeOid() == baseTypeOid
                && column.typeModifier() == effectiveTypeModifier
                ? column
                : new Column(column.key(), column.name(), baseTypeOid,
                        effectiveTypeModifier);
    }

    private Map<String, SourceTypePropagator.JdbcMetadata> loadJdbcColumnMetadata(
            Relation relation) throws SQLException {
        DatabaseMetaData metadata = controlConnection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        String schemaPattern = metadataPattern(relation.namespace(), escape);
        String tablePattern = metadataPattern(relation.name(), escape);
        Map<String, SourceTypePropagator.JdbcMetadata> columns = new HashMap<>();
        try (ResultSet result = metadata.getColumns(
                controlConnection.getCatalog(), schemaPattern, tablePattern, null)) {
            while (result.next()) {
                if (!relation.namespace().equals(result.getString("TABLE_SCHEM"))
                        || !relation.name().equals(result.getString("TABLE_NAME"))) {
                    continue;
                }
                int length = result.getInt("COLUMN_SIZE");
                Integer nullableLength = result.wasNull() ? null : length;
                int scale = result.getInt("DECIMAL_DIGITS");
                Integer nullableScale = result.wasNull() ? null : scale;
                columns.put(result.getString("COLUMN_NAME"),
                        new SourceTypePropagator.JdbcMetadata(
                                nullableLength, nullableScale));
            }
        }
        return Map.copyOf(columns);
    }

    private static String metadataPattern(String value, String escape) {
        if (escape == null || escape.isEmpty()) {
            return value;
        }
        return value.replace(escape, escape + escape)
                .replace("%", escape + "%")
                .replace("_", escape + "_");
    }

    private static Map<String, String> mergedSchemaParameters(
            Map<String, String> first, Map<String, String> second) {
        if (first.isEmpty()) {
            return second;
        }
        if (second.isEmpty()) {
            return first;
        }
        Map<String, String> merged = new LinkedHashMap<>(first);
        merged.putAll(second);
        return Map.copyOf(merged);
    }

    private RelationContext loadSnapshotRelation(String qualifiedName) throws SQLException {
        final QualifiedTable table;
        try {
            table = QualifiedTable.parse(qualifiedName);
        } catch (IllegalArgumentException error) {
            throw new ConnectException("Snapshot requires an exact schema.table name: "
                    + qualifiedName, error);
        }
        return loadSnapshotRelation(table);
    }

    private RelationContext loadSnapshotRelation(QualifiedTable qualifiedTable)
            throws SQLException {
        String namespace = qualifiedTable.schema();
        String table = qualifiedTable.table();
        String query = """
                SELECT c.oid::bigint,
                       c.relreplident::text,
                       a.attname,
                       a.atttypid::bigint,
                       a.atttypmod,
                       CASE
                         WHEN EXISTS (
                           SELECT 1 FROM pg_catalog.pg_index pk
                            WHERE pk.indrelid = c.oid AND pk.indisprimary
                         ) THEN EXISTS (
                           SELECT 1 FROM pg_catalog.pg_index pk
                            WHERE pk.indrelid = c.oid
                              AND pk.indisprimary
                              AND a.attnum = ANY(pk.indkey)
                         )
                         ELSE EXISTS (
                           SELECT 1 FROM pg_catalog.pg_index ri
                            WHERE ri.indrelid = c.oid
                              AND ri.indisreplident
                              AND a.attnum = ANY(ri.indkey)
                         )
                       END AS key_column
                  FROM pg_catalog.pg_attribute a
                  JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = ?
                   AND c.relname = ?
                   AND a.attnum > 0
                   AND NOT a.attisdropped
              ORDER BY a.attnum
                """;
        long relationId = 0L;
        int replicaIdentity = 'd';
        List<Column> columns = new ArrayList<>();
        try (PreparedStatement statement = controlConnection.prepareStatement(query)) {
            statement.setString(1, namespace);
            statement.setString(2, table);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    relationId = result.getLong(1);
                    String identity = result.getString(2);
                    replicaIdentity = identity == null || identity.isEmpty() ? 'd' : identity.charAt(0);
                    columns.add(new Column(
                            result.getBoolean(6), result.getString(3), result.getLong(4),
                            result.getInt(5)));
                }
            }
        }
        if (columns.isEmpty()) {
            throw new ConnectException("Snapshot table does not exist or has no columns: "
                    + qualifiedTable.canonicalName());
        }
        Relation relation = new Relation(
                relationId, namespace, table, replicaIdentity, List.copyOf(columns));
        RelationContext context = loadRelation(relation);
        relations.put(relationId, context);
        return context;
    }

    private SnapshotRow readNextSnapshotRow(SnapshotState state) throws SQLException {
        while (true) {
            if (state.resultSet() == null) {
                if (state.relationIndex() >= state.relations().size()) {
                    return null;
                }
                openSnapshotCursor(state);
            }
            ResultSet result = state.resultSet();
            if (result.next()) {
                RelationContext relation = state.currentRelation();
                List<TupleValue> values = new ArrayList<>(relation.columns().size());
                for (int index = 1; index <= relation.columns().size(); index++) {
                    String value = result.getString(index);
                    values.add(value == null
                            ? TupleValue.nullValue()
                            : new TupleValue(ValueKind.TEXT,
                            value.getBytes(StandardCharsets.UTF_8)));
                }
                return new SnapshotRow(relation, new TupleData(List.copyOf(values)));
            }
            state.closeCursor();
            state.relationIndex(state.relationIndex() + 1);
        }
    }

    private void openSnapshotCursor(SnapshotState state) throws SQLException {
        RelationContext relation = state.relations().get(state.relationIndex());
        String projection = relation.columns().stream()
                .map(column -> quoteIdentifier(column.message().name()) + "::text")
                .reduce((left, right) -> left + "," + right)
                .orElseThrow();
        List<String> keys = relation.columns().stream()
                .filter(column -> column.metadata().key())
                .map(column -> quoteIdentifier(column.message().name()))
                .toList();
        String orderBy = keys.isEmpty() ? "" : " ORDER BY " + String.join(",", keys);
        Relation message = relation.message();
        String query = "SELECT " + projection + " FROM "
                + quoteIdentifier(message.namespace()) + "." + quoteIdentifier(message.name())
                + orderBy;
        PreparedStatement statement = controlConnection.prepareStatement(query);
        statement.setFetchSize(Math.min(1_000, config.maxBatchSize()));
        state.openCursor(relation, statement, statement.executeQuery());
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static QualifiedTable qualifiedTable(RelationContext relation) {
        return QualifiedTable.of(relation.message().namespace(), relation.message().name());
    }

    private RelationContext relation(long relationId) {
        RelationContext relation = relations.get(relationId);
        if (relation == null) {
            throw new ConnectException("pgoutput change references unknown relation id " + relationId);
        }
        return relation;
    }

    private Transaction requireTransaction() {
        if (transaction == null) {
            throw new ConnectException("pgoutput change or commit received outside a transaction");
        }
        return transaction;
    }

    private void ensurePublication() throws SQLException {
        OptimizedSettings.PublicationAutoCreateMode mode =
                optimizedSettings.publicationAutoCreateMode();
        boolean exists = publicationExists();
        if (mode == OptimizedSettings.PublicationAutoCreateMode.DISABLED) {
            if (!exists) {
                throw new ConnectException("Publication " + config.publication() + " does not exist");
            }
            return;
        }

        String publication = quoteIdentifier(config.publication());
        if (mode == OptimizedSettings.PublicationAutoCreateMode.FILTERED) {
            String tables = publicationTables();
            try (Statement statement = controlConnection.createStatement()) {
                if (tables.isEmpty()) {
                    if (exists) {
                        statement.execute("DROP PUBLICATION " + publication);
                    }
                    statement.execute("CREATE PUBLICATION " + publication);
                } else {
                    statement.execute((exists ? "ALTER PUBLICATION " + publication + " SET TABLE "
                            : "CREATE PUBLICATION " + publication + " FOR TABLE ") + tables);
                }
            }
            return;
        }
        if (exists) {
            return;
        }
        String suffix = mode == OptimizedSettings.PublicationAutoCreateMode.ALL_TABLES
                ? " FOR ALL TABLES" : "";
        try (Statement statement = controlConnection.createStatement()) {
            statement.execute("CREATE PUBLICATION " + publication + suffix);
        }
    }

    private boolean publicationExists() throws SQLException {
        try (PreparedStatement statement = controlConnection.prepareStatement(
                "SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = ?")) {
            statement.setString(1, config.publication());
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    private String publicationTables() {
        Set<QualifiedTable> tables = new LinkedHashSet<>(includedTables);
        if (optimizedSettings.sourceSignalingEnabled()) {
            tables.add(optimizedSettings.signalTable());
        }
        return tables.stream().sorted().map(QualifiedTable::sqlName)
                .reduce((left, right) -> left + "," + right)
                .orElse("");
    }

    private void ensureSlot() throws SQLException {
        String slotColumns = optimizedSettings.twoPhaseEnabled()
                ? "plugin, database, active, two_phase"
                : "plugin, database, active";
        try (PreparedStatement statement = controlConnection.prepareStatement(
                "SELECT " + slotColumns
                        + " FROM pg_catalog.pg_replication_slots WHERE slot_name = ?")) {
            statement.setString(1, config.slot());
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    if (!"pgoutput".equals(result.getString("plugin"))) {
                        throw new ConnectException("Replication slot " + config.slot() + " does not use pgoutput");
                    }
                    if (!config.database().equals(result.getString("database"))) {
                        throw new ConnectException("Replication slot " + config.slot() + " belongs to another database");
                    }
                    if (optimizedSettings.twoPhaseEnabled() && !result.getBoolean("two_phase")) {
                        throw new ConnectException("Replication slot " + config.slot()
                                + " was not created with two_phase=true");
                    }
                    return;
                }
            }
        }

        String createSlot;
        if (shouldCreateFailoverSlot()) {
            createSlot = optimizedSettings.twoPhaseEnabled()
                    ? "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot(?, 'pgoutput', false, true, true)"
                    : "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot(?, 'pgoutput', false, false, true)";
        } else {
            createSlot = optimizedSettings.twoPhaseEnabled()
                    ? "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot(?, 'pgoutput', false, true)"
                    : "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot(?, 'pgoutput')";
        }
        try (PreparedStatement statement = controlConnection.prepareStatement(createSlot)) {
            statement.setString(1, config.slot());
            statement.executeQuery().close();
        }
    }

    private void openInitialSnapshot(boolean recreateIncompleteSnapshotSlot) throws SQLException {
        if (optimizedSettings.twoPhaseEnabled()) {
            throw new ConnectException("An initial snapshot cannot create a two-phase logical slot; "
                    + "start with snapshot.mode=no_data or use a pre-created slot");
        }
        SlotStatus existing = slotStatus();
        if (existing != null) {
            if (!recreateIncompleteSnapshotSlot) {
                throw new ConnectException("Initial snapshot requires a new replication slot; slot "
                        + config.slot() + " already exists without an incomplete native snapshot offset");
            }
            if (existing.active()) {
                throw new ConnectException("Cannot recreate active replication slot " + config.slot()
                        + " while restarting an incomplete initial snapshot");
            }
            try (PreparedStatement statement = controlConnection.prepareStatement(
                    "SELECT pg_catalog.pg_drop_replication_slot(?)")) {
                statement.setString(1, config.slot());
                statement.executeQuery().close();
            }
        }

        openReplicationConnection();
        String snapshotName;
        long snapshotLsn;
        if (shouldCreateFailoverSlot()) {
            try (Statement statement = replicationConnection.createStatement();
                 ResultSet result = statement.executeQuery(
                         "CREATE_REPLICATION_SLOT " + quoteIdentifier(config.slot())
                                 + " LOGICAL pgoutput (FAILOVER)")) {
                if (!result.next()) {
                    throw new ConnectException("PostgreSQL did not return failover slot metadata for "
                            + config.slot());
                }
                snapshotName = result.getString("snapshot_name");
                snapshotLsn = LogSequenceNumber.valueOf(
                        result.getString("consistent_point")).asLong();
            }
        } else {
            PGConnection pgConnection = replicationConnection.unwrap(PGConnection.class);
            ReplicationSlotInfo slot = pgConnection.getReplicationAPI()
                    .createReplicationSlot()
                    .logical()
                    .withSlotName(config.slot())
                    .withOutputPlugin("pgoutput")
                    .make();
            snapshotName = slot.getSnapshotName();
            snapshotLsn = slot.getConsistentPoint().asLong();
        }
        if (snapshotName == null || !snapshotName.matches("[A-Za-z0-9-]+")) {
            throw new ConnectException("PostgreSQL did not export a usable snapshot for slot "
                    + config.slot());
        }

        controlConnection.setReadOnly(true);
        controlConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        controlConnection.setAutoCommit(false);
        try (Statement statement = controlConnection.createStatement()) {
            statement.execute("SET TRANSACTION SNAPSHOT '" + snapshotName + "'");
        }
        long transactionId;
        long snapshotTimeMicros;
        try (Statement statement = controlConnection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT txid_current()::bigint, "
                             + "(extract(epoch FROM clock_timestamp()) * 1000000)::bigint")) {
            if (!result.next()) {
                throw new ConnectException("Cannot read initial snapshot transaction metadata");
            }
            transactionId = result.getLong(1);
            snapshotTimeMicros = Math.subtractExact(
                    result.getLong(2), 946_684_800_000_000L);
        }

        List<RelationContext> snapshotRelations = new ArrayList<>(includedTables.size());
        for (QualifiedTable table : includedTables.stream().sorted().toList()) {
            snapshotRelations.add(loadSnapshotRelation(table));
        }
        List<String> snapshotCollections = includedTables.stream().sorted()
                .map(QualifiedTable::canonicalName).toList();
        SnapshotState state = new SnapshotState(
                SnapshotKind.INITIAL, snapshotLsn, transactionId, snapshotTimeMicros,
                List.copyOf(snapshotCollections), List.copyOf(snapshotRelations));
        snapshotState = state;
        latestProcessedLsn.set(snapshotLsn);
        latestCommitTimeMicros = snapshotTimeMicros;
        state.next(readNextSnapshotRow(state));
        if (state.next() == null) {
            completeSnapshot(state);
        }
    }

    private boolean shouldCreateFailoverSlot() throws SQLException {
        if (!optimizedSettings.slotFailover()
                || controlConnection.getMetaData().getDatabaseMajorVersion() < 17) {
            return false;
        }
        try (Statement statement = controlConnection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT NOT pg_catalog.pg_is_in_recovery()")) {
            return result.next() && result.getBoolean(1);
        }
    }

    private void openBlockingSnapshot(SnapshotSignal.Request request) throws SQLException {
        validateSnapshotRequest(request);
        if (transaction != null || workQueue.pendingBatches() != 0) {
            throw new ConnectException(
                    "Blocking snapshot may start only after the ordered WAL barrier is drained");
        }

        closeQuietly(replicationStream);
        replicationStream = null;
        closeQuietly(replicationConnection);
        replicationConnection = null;

        controlConnection.setReadOnly(true);
        controlConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        controlConnection.setAutoCommit(false);
        long snapshotLsn;
        long transactionId;
        long snapshotTimeMicros;
        try (Statement statement = controlConnection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT pg_current_wal_lsn()::text, txid_current()::bigint, "
                             + "(extract(epoch FROM clock_timestamp()) * 1000000)::bigint")) {
            if (!result.next()) {
                throw new ConnectException("Cannot read blocking snapshot transaction metadata");
            }
            snapshotLsn = LogSequenceNumber.valueOf(result.getString(1)).asLong();
            transactionId = result.getLong(2);
            snapshotTimeMicros = Math.subtractExact(
                    result.getLong(3), 946_684_800_000_000L);
        }

        List<String> snapshotCollections = request.dataCollections().stream().sorted().toList();
        List<RelationContext> snapshotRelations = new ArrayList<>(snapshotCollections.size());
        for (String collection : snapshotCollections) {
            snapshotRelations.add(loadSnapshotRelation(collection));
        }
        SnapshotState state = new SnapshotState(
                SnapshotKind.BLOCKING, snapshotLsn, transactionId, snapshotTimeMicros,
                List.copyOf(snapshotCollections), List.copyOf(snapshotRelations));
        snapshotState = state;
        blockingSnapshotRequest = null;
        latestProcessedLsn.set(snapshotLsn);
        latestCommitTimeMicros = snapshotTimeMicros;
        state.next(readNextSnapshotRow(state));
        if (state.next() == null) {
            completeSnapshot(state);
        }
    }

    private SlotStatus slotStatus() throws SQLException {
        try (PreparedStatement statement = controlConnection.prepareStatement(
                "SELECT plugin, database, active FROM pg_catalog.pg_replication_slots "
                        + "WHERE slot_name = ?")) {
            statement.setString(1, config.slot());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return null;
                }
                String plugin = result.getString("plugin");
                String database = result.getString("database");
                if (!"pgoutput".equals(plugin) || !config.database().equals(database)) {
                    throw new ConnectException("Replication slot " + config.slot()
                            + " is not a pgoutput slot for database " + config.database());
                }
                return new SlotStatus(result.getBoolean("active"));
            }
        }
    }

    private void completeSnapshot(SnapshotState state) throws SQLException {
        if (snapshotState != state) {
            return;
        }
        state.close();
        controlConnection.commit();
        controlConnection.setAutoCommit(true);
        controlConnection.setReadOnly(false);
        previousLsn = state.lsn();
        latestProcessedLsn.set(state.lsn());
        openReplicationStream(state.lsn());
        DeferredTransaction deferred = deferredTransaction;
        deferredTransaction = null;
        if (deferred != null) {
            submitTransaction(
                    deferred.changes(), deferred.eventMetadata(), deferred.transaction(),
                    deferred.commitTimeMicros(), deferred.previousSourceLsn(),
                    deferred.durableOffsetLsn(), deferred.estimatedBytes(),
                    deferred.transactionBoundaryId());
        }
        snapshotState = null;
    }

    private void ensureConnected() throws InterruptedException {
        if (replicationStream != null || stopping) {
            return;
        }

        long waitNanos = reconnectNotBeforeNanos - System.nanoTime();
        if (waitNanos > 0L) {
            TimeUnit.NANOSECONDS.sleep(waitNanos);
        }
        if (stopping) {
            return;
        }

        try {
            openConnections(lastAcknowledgedLsn);
        } catch (SQLException error) {
            disconnectForRetry();
            throw new RetriableException(
                    "PostgreSQL is unavailable; optimized connector will retry", error);
        } catch (RuntimeException error) {
            closeConnections();
            throw error;
        }
    }

    private synchronized void openConnections(long startLsn) throws SQLException {
        openConnections(startLsn, false, false);
    }

    private synchronized void openConnections(long startLsn, boolean initialSnapshot,
                                              boolean recreateIncompleteSnapshotSlot)
            throws SQLException {
        if (stopping) {
            return;
        }
        Properties controlProperties = optimizedSettings.postgresConnectionSettings()
                .controlProperties(config.user(), config.password());
        controlConnection = DriverManager.getConnection(jdbcUrl(), controlProperties);
        controlConnection.setAutoCommit(true);
        optimizedSettings.postgresConnectionSettings()
                .initializeControlConnection(controlConnection);
        includedTables = dataCollectionFilter.resolve(controlConnection);
        ensurePublication();
        if (initialSnapshot) {
            openInitialSnapshot(recreateIncompleteSnapshotSlot);
        } else {
            ensureSlot();
            openReplicationStream(startLsn);
        }
        transaction = null;
        preparedTransactions.clear();
        streamedTransactions.clear();
        relations.clear();
        if (!initialSnapshot) {
            previousLsn = Math.max(startLsn, replaySkipThroughLsn);
            latestProcessedLsn.set(startLsn);
        }
        reconnectNotBeforeNanos = 0L;
    }

    private synchronized void disconnectForRetry() {
        closeConnections();
        transaction = null;
        preparedTransactions.clear();
        relations.clear();
        refreshReplayCheckpoint();
        reconnectNotBeforeNanos = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(RECONNECT_BACKOFF_MILLIS);
    }

    private void refreshReplayCheckpoint() {
        replaySkipThroughLsn = Math.max(
                replaySkipThroughLsn, lastAcknowledgedEmittedLsn);
        replayResumeTransactionId = lastAcknowledgedResumeTransactionId;
        replayResumeChangeCount = lastAcknowledgedResumeChangeCount;
        replayCheckpointLsn = lastAcknowledgedEmittedLsn > lastAcknowledgedLsn
                ? lastAcknowledgedLsn : -1L;
        previousLsn = lastAcknowledgedEmittedLsn;
        lastDeliveredLsn = lastAcknowledgedLsn;
        lastDeliveredEmittedLsn = lastAcknowledgedEmittedLsn;
        lastDeliveredResumeTransactionId = lastAcknowledgedResumeTransactionId;
        lastDeliveredResumeChangeCount = lastAcknowledgedResumeChangeCount;
        latestProcessedLsn.set(lastAcknowledgedLsn);
    }

    private void closeConnections() {
        SnapshotState snapshot = snapshotState;
        snapshotState = null;
        blockingSnapshotRequest = null;
        deferredTransaction = null;
        incrementalSnapshotState = null;
        incrementalWindow = null;
        incrementalChunkSetup = false;
        if (snapshot != null) {
            snapshot.close();
        }
        closeQuietly(replicationStream);
        replicationStream = null;
        closeQuietly(replicationConnection);
        replicationConnection = null;
        closeQuietly(controlConnection);
        controlConnection = null;
        closeQuietly(heartbeatActionConnection);
        heartbeatActionConnection = null;
        transaction = null;
        streamedTransactionId = null;
        if (workQueue != null) {
            workQueue.reset();
        }
        closeLiveTransactions();
        preparedTransactions.clear();
        streamedTransactions.clear();
        relations.clear();
    }

    private void closeLiveTransactions() {
        List<Transaction> snapshot;
        synchronized (liveTransactions) {
            snapshot = List.copyOf(liveTransactions);
        }
        for (Transaction active : snapshot) {
            active.close();
        }
    }

    private void openReplicationStream(long startLsn) throws SQLException {
        openReplicationConnection();
        PGConnection pgConnection = replicationConnection.unwrap(PGConnection.class);

        var builder = pgConnection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(config.slot())
                .withSlotOption("proto_version", optimizedSettings.pgOutputProtocolVersion())
                .withSlotOption("publication_names", config.publication())
                .withStatusInterval(
                        optimizedSettings.statusUpdateIntervalMs(), TimeUnit.MILLISECONDS);
        optimizedSettings.slotStreamParameters().forEach((key, value) -> {
            if (!Set.of("proto_version", "publication_names", "messages").contains(key)) {
                builder.withSlotOption(key, value);
            }
        });
        if (optimizedSettings.logicalMessagesEnabled()) {
            builder.withSlotOption("messages", true);
        } else if (optimizedSettings.slotStreamParameters().containsKey("messages")) {
            builder.withSlotOption(
                    "messages", optimizedSettings.slotStreamParameters().get("messages"));
        }
        if (startLsn > 0) {
            builder.withStartPosition(LogSequenceNumber.valueOf(startLsn));
        }
        replicationStream = builder.start();
        scheduleNextReplicationStatus();
    }

    private void forceReplicationStatusIfDue() throws SQLException {
        long now = System.nanoTime();
        if (now < nextReplicationStatusNanos) {
            return;
        }
        PGReplicationStream stream = replicationStream;
        if (stream != null) {
            stream.forceUpdateStatus();
        }
        scheduleNextReplicationStatus(now);
    }

    private void scheduleNextReplicationStatus() {
        scheduleNextReplicationStatus(System.nanoTime());
    }

    private void scheduleNextReplicationStatus(long now) {
        long interval = TimeUnit.MILLISECONDS.toNanos(
                optimizedSettings.statusUpdateIntervalMs());
        nextReplicationStatusNanos = now > Long.MAX_VALUE - interval
                ? Long.MAX_VALUE : now + interval;
    }

    private void openReplicationConnection() throws SQLException {
        if (replicationConnection != null) {
            return;
        }
        Properties properties = optimizedSettings.postgresConnectionSettings()
                .replicationProperties(config.user(), config.password());
        replicationConnection = DriverManager.getConnection(jdbcUrl(), properties);
    }

    private void dropSlotOnStop() {
        if (optimizedSettings == null || config == null || !optimizedSettings.slotDropOnStop()) {
            return;
        }
        Properties properties = optimizedSettings.postgresConnectionSettings()
                .controlProperties(config.user(), config.password());
        try (Connection connection = DriverManager.getConnection(jdbcUrl(), properties);
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT pg_catalog.pg_drop_replication_slot(slot_name) "
                             + "FROM pg_catalog.pg_replication_slots WHERE slot_name = ?")) {
            connection.setAutoCommit(true);
            optimizedSettings.postgresConnectionSettings()
                    .initializeControlConnection(connection);
            statement.setString(1, config.slot());
            statement.executeQuery().close();
        } catch (SQLException error) {
            LOGGER.warn("Cannot drop PostgreSQL replication slot {} while stopping", config.slot(), error);
        }
    }

    private Map<String, Object> sourceOffset(long lsn, String incrementalState) {
        return sourceOffset(lsn, lsn, incrementalState);
    }

    private Map<String, Object> sourceOffset(long lsn, long lastEmittedLsn,
                                             String incrementalState) {
        Map<String, Object> offset = new LinkedHashMap<>();
        offset.put("lsn", lsn);
        offset.put("last_emitted_lsn", lastEmittedLsn);
        if (incrementalState != null) {
            offset.put("incremental_snapshot", true);
            offset.put("incremental_snapshot_state", incrementalState);
        }
        return offset;
    }

    private static void addTransactionResume(Map<String, Object> offset,
                                             TransactionResume resume) {
        if (resume != null) {
            offset.put("transaction_resume_id", resume.transactionId());
            offset.put("transaction_resume_count", resume.changeCount());
        }
    }

    private static SourceRecord withTransactionResume(SourceRecord record,
                                                      TransactionResume resume) {
        return rewriteTransactionProgress(record,
                offsetLong(record.sourceOffset().get("lsn"), 0L),
                offsetLong(record.sourceOffset().get("last_emitted_lsn"), 0L),
                resume);
    }

    static SourceRecord withTransactionProgress(SourceRecord record,
                                                long offsetLsn, long emittedLsn,
                                                long transactionId, int changeCount,
                                                boolean completesTransaction) {
        return rewriteTransactionProgress(record, offsetLsn, emittedLsn,
                completesTransaction ? null
                        : new TransactionResume(transactionId, changeCount));
    }

    private static SourceRecord rewriteTransactionProgress(SourceRecord record,
                                                           long offsetLsn,
                                                           long emittedLsn,
                                                           TransactionResume resume) {
        Map<String, Object> offset = new LinkedHashMap<>(record.sourceOffset());
        offset.put("lsn", offsetLsn);
        offset.put("last_emitted_lsn", emittedLsn);
        offset.remove("transaction_resume_id");
        offset.remove("transaction_resume_count");
        addTransactionResume(offset, resume);
        return new SourceRecord(record.sourcePartition(), offset, record.topic(),
                record.kafkaPartition(), record.keySchema(), record.key(),
                record.valueSchema(), record.value(), record.timestamp(), record.headers());
    }

    private String incrementalProgressJson(IncrementalSnapshotState state) {
        if (state == null) {
            return null;
        }
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("collection_index", state.collectionIndex());
        root.put("stop_all", state.stopAll());
        root.put("finish_current_on_stop", state.finishCurrentOnStop());
        root.put("stopped_collections", state.stoppedCollections());
        List<Map<String, Object>> collections = new ArrayList<>(state.collections().size());
        for (IncrementalCollectionState collection : state.collections()) {
            Map<String, Object> stored = new LinkedHashMap<>();
            stored.put("data_collection",
                    qualifiedTable(collection.relation()).canonicalName());
            stored.put("maximum_key", collection.maximumKey());
            stored.put("last_key", collection.lastKey());
            stored.put("additional_condition", collection.additionalCondition());
            collections.add(stored);
        }
        root.put("collections", collections);
        try {
            return OFFSET_JSON.writeValueAsString(root);
        } catch (JsonProcessingException error) {
            throw new ConnectException("Cannot serialize incremental snapshot progress", error);
        }
    }

    private IncrementalSnapshotState restoreIncrementalState(String serialized)
            throws SQLException {
        final Map<String, Object> root;
        try {
            root = OFFSET_JSON.readValue(serialized, new TypeReference<>() { });
        } catch (JsonProcessingException error) {
            throw new ConnectException("Cannot parse stored incremental snapshot progress", error);
        }
        Object rawCollections = root.get("collections");
        if (!(rawCollections instanceof List<?> storedCollections)
                || storedCollections.isEmpty()) {
            throw new ConnectException("Stored incremental snapshot progress has no collections");
        }
        List<IncrementalCollectionState> collections = new ArrayList<>(
                storedCollections.size());
        for (Object rawCollection : storedCollections) {
            if (!(rawCollection instanceof Map<?, ?> stored)) {
                throw new ConnectException("Stored incremental snapshot collection is invalid");
            }
            String name = Objects.toString(stored.get("data_collection"), "");
            if (!includedTables.contains(QualifiedTable.parse(name))) {
                throw new ConnectException("Stored incremental snapshot collection " + name
                        + " is outside table.include.list");
            }
            RelationContext relation = loadSnapshotRelation(name);
            List<Object> maximumKey = storedKey(stored.get("maximum_key"), relation);
            List<Object> lastKey = storedKey(stored.get("last_key"), relation);
            String additionalCondition = stored.get("additional_condition") == null
                    ? null : stored.get("additional_condition").toString();
            collections.add(new IncrementalCollectionState(
                    relation, maximumKey, lastKey, additionalCondition));
        }
        Object rawIndex = root.get("collection_index");
        if (!(rawIndex instanceof Number number)) {
            throw new ConnectException("Stored incremental snapshot collection index is invalid");
        }
        int collectionIndex = number.intValue();
        if (collectionIndex < 0 || collectionIndex >= collections.size()) {
            throw new ConnectException("Stored incremental snapshot collection index is out of range");
        }
        boolean stopAll = Boolean.TRUE.equals(root.get("stop_all"));
        boolean finishCurrentOnStop = Boolean.TRUE.equals(
                root.get("finish_current_on_stop"));
        Set<String> stoppedCollections = storedStoppedCollections(
                root.get("stopped_collections"), collections);
        return new IncrementalSnapshotState(
                List.copyOf(collections), collectionIndex, stopAll,
                finishCurrentOnStop, stoppedCollections);
    }

    private static Set<String> storedStoppedCollections(
            Object value, List<IncrementalCollectionState> collections) {
        if (value == null) {
            return Set.of();
        }
        if (!(value instanceof List<?> values)) {
            throw new ConnectException("Stored stopped incremental collections are invalid");
        }
        Set<String> known = collections.stream()
                .map(collection -> qualifiedTable(collection.relation()).canonicalName())
                .collect(java.util.stream.Collectors.toSet());
        Set<String> stopped = new LinkedHashSet<>();
        for (Object item : values) {
            String name = Objects.toString(item, "");
            if (!known.contains(name)) {
                throw new ConnectException("Stored stopped incremental collection " + name
                        + " is not part of the snapshot");
            }
            stopped.add(name);
        }
        return Set.copyOf(stopped);
    }

    private static List<Object> storedKey(Object value, RelationContext relation) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof List<?> values)) {
            throw new ConnectException("Stored incremental snapshot key is invalid");
        }
        List<ColumnRuntime> keys = relation.columns().stream()
                .filter(column -> column.metadata().key())
                .toList();
        if (values.size() != keys.size()) {
            throw new ConnectException("Stored incremental snapshot key value count "
                    + values.size() + " does not match key column count " + keys.size());
        }
        List<Object> normalized = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            Object stored = values.get(index);
            if (stored == null) {
                throw new ConnectException("Stored incremental snapshot key contains null");
            }
            normalized.add(normalizeStoredKeyValue(stored, keys.get(index).baseTypeOid()));
        }
        return List.copyOf(normalized);
    }

    static String normalizeStoredKeyValue(Object value, long postgresTypeOid) {
        if (value instanceof String text) {
            return text;
        }
        if (value instanceof Number number) {
            long epochMillis = number.longValue();
            if (postgresTypeOid == POSTGRES_DATE_OID) {
                return new java.sql.Date(epochMillis).toString();
            }
            if (postgresTypeOid == POSTGRES_TIME_OID
                    || postgresTypeOid == POSTGRES_TIMETZ_OID) {
                return new java.sql.Time(epochMillis).toString();
            }
            if (postgresTypeOid == POSTGRES_TIMESTAMP_OID
                    || postgresTypeOid == POSTGRES_TIMESTAMPTZ_OID) {
                return new java.sql.Timestamp(epochMillis).toString();
            }
        }
        if (value instanceof Map<?, ?> object && object.get("value") != null) {
            return object.get("value").toString();
        }
        return value.toString();
    }

    private StoredOffset storedOffset() {
        Map<String, Object> offset = context.offsetStorageReader().offset(sourcePartition);
        if (offset == null) {
            return new StoredOffset(
                    0L, 0L, false, false, SnapshotKind.INITIAL, List.of(), null,
                    -1L, 0);
        }
        long lsn = offsetLong(offset.get("lsn"), 0L);
        long lastEmittedLsn = Math.max(
                lsn, offsetLong(offset.get("last_emitted_lsn"), lsn));
        boolean snapshot = booleanOffset(offset.get("snapshot"));
        boolean completed = booleanOffset(offset.get("snapshot_completed"));
        SnapshotKind kind = SnapshotKind.fromOffset(offset.get("snapshot_kind"));
        List<String> collections = parseOffsetCollections(
                offset.get("snapshot_data_collections_json"),
                offset.get("snapshot_data_collections"));
        String incrementalState = offset.get("incremental_snapshot_state") == null
                ? null : offset.get("incremental_snapshot_state").toString();
        long resumeTransactionId = offsetLong(
                offset.get("transaction_resume_id"), -1L);
        int resumeChangeCount = Math.toIntExact(offsetLong(
                offset.get("transaction_resume_count"), 0L));
        if (resumeTransactionId < 0L || resumeChangeCount < 0) {
            resumeTransactionId = -1L;
            resumeChangeCount = 0;
        }
        return new StoredOffset(
                lsn, lastEmittedLsn, true, snapshot && !completed, kind, collections,
                incrementalState, resumeTransactionId, resumeChangeCount);
    }

    private static long offsetLong(Object value, long defaultValue) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String text) {
            return Long.parseLong(text);
        }
        return defaultValue;
    }

    static List<String> parseOffsetCollections(Object jsonValue, Object legacyValue) {
        if (jsonValue != null && !jsonValue.toString().isBlank()) {
            try {
                List<String> values = OFFSET_JSON.readValue(
                        jsonValue.toString(), new TypeReference<>() { });
                if (values == null) {
                    throw new IllegalArgumentException("Snapshot collection array is null");
                }
                return values.stream().map(QualifiedTable::parse)
                        .map(QualifiedTable::canonicalName).toList();
            } catch (JsonProcessingException | IllegalArgumentException error) {
                throw new ConnectException("Stored snapshot data collections are invalid", error);
            }
        }
        if (legacyValue == null || legacyValue.toString().isBlank()) {
            return List.of();
        }
        try {
            return java.util.Arrays.stream(legacyValue.toString().split(","))
                    .map(QualifiedTable::parse).map(QualifiedTable::canonicalName).toList();
        } catch (IllegalArgumentException error) {
            throw new ConnectException("Stored snapshot data collections are invalid", error);
        }
    }

    private static boolean booleanOffset(Object value) {
        return value instanceof Boolean flag ? flag
                : value != null && Boolean.parseBoolean(value.toString());
    }

    private String jdbcUrl() {
        return "jdbc:postgresql://" + config.host() + ":" + config.port() + "/" + config.database();
    }

    private static Set<String> parseSkippedOperations(String value) {
        Set<String> operations = new HashSet<>();
        if (value == null || value.isBlank() || "none".equalsIgnoreCase(value.trim())) {
            return Set.of();
        }
        for (String operation : value.split(",")) {
            String normalized = operation.trim().toLowerCase();
            if (!Set.of("c", "u", "d", "t").contains(normalized)) {
                throw new ConnectException("Unsupported skipped.operations value " + operation);
            }
            operations.add(normalized);
        }
        return Set.copyOf(operations);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Task shutdown must continue closing the remaining resources.
        }
    }

    private record ColumnRuntime(Column message, ColumnMetadata metadata,
                                 String databaseType, long baseTypeOid,
                                 boolean includedInValue,
                                 boolean unknownDatatype,
                                 CustomConverters.Binding customConverter,
                                 ColumnValueTransformers.Binding valueTransformer,
                                 Map<String, String> sourceTypeParameters) {}

    private record RelationContext(Relation message, List<ColumnRuntime> columns,
                                   DebeziumSchemaFactory.RelationSchemas schemas) {}

    private record StoredOffset(long lsn, long lastEmittedLsn,
                                boolean present, boolean incompleteSnapshot,
                                SnapshotKind snapshotKind, List<String> snapshotCollections,
                                String incrementalSnapshotState,
                                long resumeTransactionId, int resumeChangeCount) {
        boolean incompleteInitialSnapshot() {
            return incompleteSnapshot && snapshotKind == SnapshotKind.INITIAL;
        }

        boolean incompleteBlockingSnapshot() {
            return incompleteSnapshot && snapshotKind == SnapshotKind.BLOCKING
                    && !snapshotCollections.isEmpty();
        }

        boolean incompleteIncrementalSnapshot() {
            return incrementalSnapshotState != null && !incrementalSnapshotState.isBlank();
        }
    }

    private record SlotStatus(boolean active) {}

    private record SnapshotRow(RelationContext relation, TupleData tuple) {}

    private record IncrementalChunk(LinkedHashMap<String, SnapshotRow> rows,
                                    List<Object> lastKey,
                                    boolean lastChunk,
                                    long estimatedBytes) {}

    private record ReadOnlyWatermark(long transactionId, long lsn,
                                     long timestampMicros) {}

    private record ClosedIncrementalWindow(List<SnapshotRow> rows,
                                           long lowLsn,
                                           long highLsn,
                                           long durableOffsetLsn,
                                           long snapshotTimeMicros,
                                           long estimatedBytes,
                                           String resumeState) {}

    private static final class IncrementalCollectionState {
        private final RelationContext relation;
        private final List<Object> maximumKey;
        private final String additionalCondition;
        private List<Object> lastKey;

        IncrementalCollectionState(RelationContext relation, List<Object> maximumKey,
                                   List<Object> lastKey, String additionalCondition) {
            this.relation = relation;
            this.maximumKey = maximumKey;
            this.lastKey = lastKey;
            this.additionalCondition = additionalCondition;
        }

        RelationContext relation() { return relation; }
        List<Object> maximumKey() { return maximumKey; }
        List<Object> lastKey() { return lastKey; }
        void lastKey(List<Object> value) { lastKey = value; }
        String additionalCondition() { return additionalCondition; }
    }

    private static final class IncrementalSnapshotState {
        private final List<IncrementalCollectionState> collections;
        private final boolean terminalWindowRequired;
        private final Set<String> stoppedCollections = new LinkedHashSet<>();
        private int collectionIndex;
        private boolean stopAll;
        private boolean finishCurrentOnStop;

        IncrementalSnapshotState(List<IncrementalCollectionState> collections) {
            this.collections = collections;
            terminalWindowRequired = true;
        }

        IncrementalSnapshotState(List<IncrementalCollectionState> collections,
                                 int collectionIndex, boolean stopAll,
                                 boolean finishCurrentOnStop,
                                 Set<String> stoppedCollections) {
            this.collections = collections;
            this.collectionIndex = collectionIndex;
            this.stopAll = stopAll;
            this.finishCurrentOnStop = finishCurrentOnStop;
            this.stoppedCollections.addAll(stoppedCollections);
            terminalWindowRequired = collectionIndex == 0
                    && collections.stream().allMatch(collection -> collection.lastKey() == null);
        }

        boolean complete() {
            return collectionIndex >= collections.size()
                    || stopAll && !finishCurrentOnStop;
        }
        boolean hasNextCollection() { return collectionIndex + 1 < collections.size(); }
        IncrementalCollectionState current() { return collections.get(collectionIndex); }
        void advanceCollection() { collectionIndex++; }
        List<IncrementalCollectionState> collections() { return collections; }
        int collectionIndex() { return collectionIndex; }
        boolean terminalWindowRequired() { return terminalWindowRequired; }
        boolean stopAll() { return stopAll; }
        boolean finishCurrentOnStop() { return finishCurrentOnStop; }
        List<String> stoppedCollections() { return List.copyOf(stoppedCollections); }

        void stop(List<String> dataCollections, boolean windowActive) {
            if (dataCollections.isEmpty()) {
                stopAll = true;
                finishCurrentOnStop = windowActive;
            } else {
                Set<String> snapshotCollections = collections.stream()
                        .map(collection -> qualifiedTable(collection.relation()).canonicalName())
                        .collect(java.util.stream.Collectors.toSet());
                dataCollections.stream()
                        .filter(snapshotCollections::contains)
                        .forEach(stoppedCollections::add);
                if (windowActive && stoppedCollections.contains(
                        qualifiedTable(current().relation()).canonicalName())) {
                    finishCurrentOnStop = true;
                }
            }
        }

        boolean stopsCurrentCollection() {
            return stopAll || stoppedCollections.contains(
                    qualifiedTable(current().relation()).canonicalName());
        }

        boolean shouldSkipCurrentCollection() {
            return stopsCurrentCollection() && !finishCurrentOnStop;
        }

        void finishStoppedCurrentCollection() {
            finishCurrentOnStop = false;
            advanceCollection();
        }
    }

    private static final class IncrementalWindow {
        private final long id;
        private final String watermarkId;
        private final long snapshotTimeMicros;
        private final IncrementalCollectionState collection;
        private final LinkedHashMap<String, SnapshotRow> rows;
        private final List<Object> lastKey;
        private final boolean lastChunk;
        private final long estimatedBytes;
        private final ReadOnlyWatermark lowWatermark;
        private final ReadOnlyWatermark highWatermark;
        private boolean open;

        IncrementalWindow(long id, String watermarkId, long snapshotTimeMicros,
                          IncrementalCollectionState collection,
                          LinkedHashMap<String, SnapshotRow> rows,
                          List<Object> lastKey, boolean lastChunk, long estimatedBytes,
                          ReadOnlyWatermark lowWatermark,
                          ReadOnlyWatermark highWatermark) {
            this.id = id;
            this.watermarkId = watermarkId;
            this.snapshotTimeMicros = snapshotTimeMicros;
            this.collection = collection;
            this.rows = rows;
            this.lastKey = lastKey;
            this.lastChunk = lastChunk;
            this.estimatedBytes = estimatedBytes;
            this.lowWatermark = lowWatermark;
            this.highWatermark = highWatermark;
        }

        long id() { return id; }
        String watermarkId() { return watermarkId; }
        long snapshotTimeMicros() { return snapshotTimeMicros; }
        IncrementalCollectionState collection() { return collection; }
        LinkedHashMap<String, SnapshotRow> rows() { return rows; }
        List<Object> lastKey() { return lastKey; }
        boolean lastChunk() { return lastChunk; }
        long estimatedBytes() { return estimatedBytes; }
        boolean readOnly() { return lowWatermark != null && highWatermark != null; }
        long highTransactionId() { return highWatermark.transactionId(); }
        long lowLsn() { return lowWatermark.lsn(); }
        long highLsn() { return highWatermark.lsn(); }
        boolean open() { return open; }
        void open(boolean value) { open = value; }
    }

    private record IncrementalBoundary(IncrementalWindow window, Type type) {
        enum Type {
            OPEN,
            CLOSE
        }
    }

    private record DetectedSnapshotSignal(int changeIndex, SnapshotSignal.Request request) {}

    private record DeferredTransaction(List<PendingChange> changes,
                                       List<EventMetadata> eventMetadata,
                                       Transaction transaction,
                                       long commitTimeMicros,
                                       long previousSourceLsn,
                                       long durableOffsetLsn,
                                       long estimatedBytes,
                                       String transactionBoundaryId) {}

    private record PreparedTransaction(Transaction transaction,
                                       long prepareLsn,
                                       long endLsn,
                                       long prepareTimeMicros,
                                       String gid) {
        void verify(long transactionId, String actualGid) {
            if (transaction.transactionId() != transactionId || !gid.equals(actualGid)) {
                throw new ConnectException("Prepared transaction identity mismatch: expected "
                        + transaction.transactionId() + " (" + gid + "), received "
                        + transactionId + " (" + actualGid + ")");
            }
        }
    }

    private enum SnapshotKind {
        INITIAL("initial"),
        BLOCKING("blocking");

        private final String offsetValue;

        SnapshotKind(String offsetValue) {
            this.offsetValue = offsetValue;
        }

        String offsetValue() {
            return offsetValue;
        }

        static SnapshotKind fromOffset(Object value) {
            if (value != null && "blocking".equals(value.toString())) {
                return BLOCKING;
            }
            return INITIAL;
        }
    }

    private static final class SnapshotState implements AutoCloseable {
        private final SnapshotKind kind;
        private final long lsn;
        private final long transactionId;
        private final long snapshotTimeMicros;
        private final List<String> dataCollections;
        private final List<RelationContext> relations;
        private int relationIndex;
        private RelationContext currentRelation;
        private PreparedStatement statement;
        private ResultSet resultSet;
        private SnapshotRow next;
        private boolean first = true;

        SnapshotState(SnapshotKind kind, long lsn, long transactionId, long snapshotTimeMicros,
                      List<String> dataCollections, List<RelationContext> relations) {
            this.kind = kind;
            this.lsn = lsn;
            this.transactionId = transactionId;
            this.snapshotTimeMicros = snapshotTimeMicros;
            this.dataCollections = dataCollections;
            this.relations = relations;
        }

        SnapshotKind kind() { return kind; }
        long lsn() { return lsn; }
        long transactionId() { return transactionId; }
        long snapshotTimeMicros() { return snapshotTimeMicros; }
        List<String> dataCollections() { return dataCollections; }
        List<RelationContext> relations() { return relations; }
        int relationIndex() { return relationIndex; }
        void relationIndex(int value) { relationIndex = value; }
        RelationContext currentRelation() { return currentRelation; }
        ResultSet resultSet() { return resultSet; }
        SnapshotRow next() { return next; }
        void next(SnapshotRow value) { next = value; }
        boolean first() { return first; }
        void first(boolean value) { first = value; }

        void openCursor(RelationContext relation, PreparedStatement nextStatement,
                        ResultSet nextResultSet) {
            currentRelation = relation;
            statement = nextStatement;
            resultSet = nextResultSet;
        }

        void closeCursor() {
            closeQuietly(resultSet);
            closeQuietly(statement);
            resultSet = null;
            statement = null;
            currentRelation = null;
        }

        @Override
        public void close() {
            closeCursor();
        }
    }

    private record CommittedTransaction(long sequence, Transaction transaction) {}

    private record TransactionResume(long transactionId, int changeCount) {}

    private record EventMetadata(long eventLsn, String transactionId,
                                 long totalOrder, long dataCollectionOrder) {}

    private final class Transaction implements AutoCloseable {
        private final long transactionId;
        private final long beginCommitTimeMicros;
        private final String preparedGid;
        private final Long prepareLsn;
        private final List<RelationContext> relationContexts = new ArrayList<>();
        private final Map<RelationContext, Integer> relationIds = new IdentityHashMap<>();
        private final Map<String, Long> collectionOrders = new HashMap<>();
        private final Map<Long, Integer> subtransactionStarts = new LinkedHashMap<>();
        private final SpillableList<PendingChange> changes;
        private final long checkpointLsn;
        private int outputRecords;
        private int resumeChangeCount;
        private boolean resumeActive;
        private String originName;
        private Long originLsn;

        Transaction(long transactionId, long beginCommitTimeMicros) {
            this(transactionId, beginCommitTimeMicros, null, null, -1L);
        }

        Transaction(long transactionId, long beginCommitTimeMicros,
                    String preparedGid, Long prepareLsn, long checkpointLsn) {
            this.transactionId = transactionId;
            this.beginCommitTimeMicros = beginCommitTimeMicros;
            this.preparedGid = preparedGid;
            this.prepareLsn = prepareLsn;
            this.checkpointLsn = checkpointLsn;
            long budget = optimizedSettings.maxInflightBytes();
            long residentBudget = Math.max(1L,
                    budget / optimizedSettings.maxInflightTransactions());
            long blockBudget = Math.max(1L,
                    budget / Math.max(2L, optimizedSettings.processingWorkers() + 2L));
            this.changes = new SpillableList<>(pendingChangeCodec(this),
                    optimizedSettings.transactionSpillDirectory(), residentBudget,
                    blockBudget, Math.min(optimizedSettings.processingChunkSize(),
                    config.maxBatchSize()));
            if (transactionId == replayResumeTransactionId) {
                resumeChangeCount = replayResumeChangeCount;
                resumeActive = true;
            }
        }

        long transactionId() {
            return transactionId;
        }

        long beginCommitTimeMicros() {
            return beginCommitTimeMicros;
        }

        long checkpointLsn() {
            return checkpointLsn;
        }

        List<PendingChange> changes() {
            return changes;
        }

        long estimatedBytes() {
            return changes.estimatedBytes();
        }

        int totalChangeCount() {
            return changes.size();
        }

        int outputRecords() {
            return outputRecords + (changes.isEmpty() || !config.provideTransactionMetadata()
                    ? 0 : 2);
        }

        int resumeChangeCount() {
            return resumeChangeCount;
        }

        boolean resumeActive() {
            return resumeActive;
        }

        void seal() {
            changes.seal();
        }

        String originName() {
            return originName;
        }

        Long originLsn() {
            return originLsn;
        }

        void origin(String name, long lsn) {
            if (originName != null
                    && (!originName.equals(name) || !Objects.equals(originLsn, lsn))) {
                throw new ConnectException("Transaction " + transactionId
                        + " contains conflicting replication-origin messages");
            }
            originName = Objects.requireNonNull(name, "name");
            originLsn = lsn;
        }

        void verifyPreparedIdentity(long actualTransactionId, String actualGid,
                                    long actualPrepareLsn) {
            if (preparedGid == null || prepareLsn == null) {
                throw new ConnectException("PREPARE received for a regular transaction "
                        + transactionId);
            }
            if (transactionId != actualTransactionId
                    || !preparedGid.equals(actualGid)
                    || prepareLsn != actualPrepareLsn) {
                throw new ConnectException("Prepared transaction boundary mismatch: expected "
                        + transactionId + " (" + preparedGid + ") at " + prepareLsn
                        + ", received " + actualTransactionId + " (" + actualGid
                        + ") at " + actualPrepareLsn);
            }
        }

        void add(PendingChange change, long estimatedChangeBytes,
                 Long payloadTransactionId) {
            if (payloadTransactionId != null && payloadTransactionId != transactionId) {
                subtransactionStarts.putIfAbsent(payloadTransactionId, changes.size());
            }
            String collection = change.relation() == null
                    ? "message" : change.relation().message().qualifiedName();
            long collectionOrder = collectionOrders.merge(collection, 1L, Long::sum);
            PendingChange ordered = change.withDataCollectionOrder(collectionOrder);
            changes.append(ordered, estimatedChangeBytes);
            outputRecords = Math.addExact(outputRecords, outputRecordCount(ordered));
        }

        void rollbackSubtransaction(long subtransactionId) {
            Integer fromIndex = subtransactionStarts.get(subtransactionId);
            if (fromIndex == null) {
                return;
            }
            changes.truncate(fromIndex);
            subtransactionStarts.entrySet().removeIf(
                    entry -> entry.getValue() >= fromIndex);
            rebuildDerivedState();
        }

        private void rebuildDerivedState() {
            collectionOrders.clear();
            outputRecords = 0;
            for (int index = 0; index < changes.size(); index++) {
                PendingChange change = changes.get(index);
                String collection = change.relation() == null
                        ? "message" : change.relation().message().qualifiedName();
                long order = collectionOrders.merge(collection, 1L, Long::sum);
                if (change.dataCollectionOrder() != order) {
                    throw new ConnectException("Streamed subtransaction rollback left invalid "
                            + "data-collection ordering at change " + index);
                }
                outputRecords = Math.addExact(outputRecords, outputRecordCount(change));
            }
        }

        int relationId(RelationContext relation) {
            if (relation == null) {
                return -1;
            }
            Integer existing = relationIds.get(relation);
            if (existing != null) {
                return existing;
            }
            int created = relationContexts.size();
            relationContexts.add(relation);
            relationIds.put(relation, created);
            return created;
        }

        RelationContext relation(int id) {
            if (id < 0) {
                return null;
            }
            return relationContexts.get(id);
        }

        @Override
        public void close() {
            changes.close();
            liveTransactions.remove(this);
        }
    }

    private record PendingChange(String operation, RelationContext relation,
                                 Character oldTupleKind, TupleData before, TupleData after,
                                 LogicalMessage logicalMessage, long eventLsn,
                                 long dataCollectionOrder) {
        static PendingChange insert(RelationContext relation, TupleData after, long eventLsn) {
            return new PendingChange(
                    "c", relation, null, null, Objects.requireNonNull(after), null, eventLsn, 0L);
        }

        static PendingChange update(RelationContext relation, Character oldTupleKind,
                                    TupleData before, TupleData after, long eventLsn) {
            return new PendingChange(
                    "u", relation, oldTupleKind, before, Objects.requireNonNull(after), null,
                    eventLsn, 0L);
        }

        static PendingChange delete(RelationContext relation, char oldTupleKind,
                                    TupleData before, long eventLsn) {
            return new PendingChange(
                    "d", relation, oldTupleKind, Objects.requireNonNull(before), null, null,
                    eventLsn, 0L);
        }

        static PendingChange truncate(RelationContext relation, long eventLsn) {
            return new PendingChange("t", relation, null, null, null, null, eventLsn, 0L);
        }

        static PendingChange logicalMessage(LogicalMessage message, long eventLsn) {
            return new PendingChange("m", null, null, null, null,
                    Objects.requireNonNull(message), eventLsn, 0L);
        }

        PendingChange withDataCollectionOrder(long order) {
            return new PendingChange(operation, relation, oldTupleKind, before, after,
                    logicalMessage, eventLsn, order);
        }
    }
}
