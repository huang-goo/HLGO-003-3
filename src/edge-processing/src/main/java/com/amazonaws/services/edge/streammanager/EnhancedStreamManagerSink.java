/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.edge.streammanager;

import com.amazonaws.greengrass.streammanager.client.StreamManagerClient;
import com.amazonaws.greengrass.streammanager.client.StreamManagerClientFactory;
import com.amazonaws.greengrass.streammanager.client.config.StreamManagerClientConfig;
import com.amazonaws.greengrass.streammanager.client.config.StreamManagerServerInfo;
import com.amazonaws.greengrass.streammanager.client.exception.StreamManagerException;
import com.amazonaws.greengrass.streammanager.model.MessageStreamDefinition;
import com.amazonaws.greengrass.streammanager.model.Persistence;
import com.amazonaws.greengrass.streammanager.model.StrategyOnFull;
import com.amazonaws.greengrass.streammanager.model.export.ExportDefinition;
import com.amazonaws.greengrass.streammanager.model.export.KinesisConfig;
import com.amazonaws.services.common.model.AggregatedWindow;
import com.amazonaws.services.common.model.AnomalyMessage;
import com.amazonaws.services.common.utils.OfflineMessageStore;
import com.amazonaws.services.common.utils.OfflineMessageStore.StoredMessage;
import com.google.gson.Gson;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class EnhancedStreamManagerSink extends RichSinkFunction<AggregatedWindow> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(EnhancedStreamManagerSink.class);

    private static final long RECORDS_FLUSH_INTERVAL_MILLISECONDS = 30L * 1000L;
    private static final int DEFAULT_BATCH_SIZE = 50;
    private static final int DEFAULT_FORWARD_BATCH_SIZE = 10;
    private static final long OFFLINE_STORAGE_CHECK_INTERVAL_MS = 5000;

    private final String region;
    private final String targetGGStream;
    private final String streamMgrHost;
    private final String streamMgrPort;
    private final String kinesisExportStreamName;
    private final int batchSize;
    private final String offlineStoragePath;

    private final BlockingQueue<AggregatedWindow> bufferedWindows;
    private transient ListState<PendingTransaction> transactionState;
    private transient StreamManagerClient writeClient;
    private transient OfflineMessageStore offlineMessageStore;
    private transient ScheduledExecutorService retryExecutor;
    private transient volatile boolean isRunning;
    private long emptyListTimestamp;
    private final Map<String, PendingTransaction> pendingTransactions;
    private final Gson gson;

    public static class PendingTransaction implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        String transactionId;
        long checkpointId;
        List<String> messageIds;
        List<String> idempotencyKeys;
        long timestamp;
        boolean isCommitted;

        public PendingTransaction() {
            this.messageIds = new ArrayList<>();
            this.idempotencyKeys = new ArrayList<>();
            this.timestamp = System.currentTimeMillis();
            this.isCommitted = false;
        }
    }

    public EnhancedStreamManagerSink(String region, String targetGGStream, String streamMgrHost,
                                     String streamMgrPort, String kinesisExportStreamName,
                                     int batchSize, String offlineStoragePath) {
        this.region = region;
        this.targetGGStream = targetGGStream;
        this.streamMgrHost = streamMgrHost;
        this.streamMgrPort = streamMgrPort;
        this.kinesisExportStreamName = kinesisExportStreamName;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.offlineStoragePath = offlineStoragePath;
        this.bufferedWindows = new LinkedBlockingQueue<>();
        this.pendingTransactions = new ConcurrentHashMap<>();
        this.emptyListTimestamp = System.currentTimeMillis();
        this.gson = new Gson();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRunning = true;

        final StreamManagerClientConfig clientConfig = StreamManagerClientConfig.builder()
                .logger(logger)
                .serverInfo(StreamManagerServerInfo.builder()
                        .host(streamMgrHost)
                        .port(Integer.parseInt(streamMgrPort)).build())
                .build();
        this.writeClient = StreamManagerClientFactory.standard().withClientConfig(clientConfig).build();

        createTargetStream();

        if (offlineStoragePath != null && !offlineStoragePath.isEmpty()) {
            this.offlineMessageStore = new OfflineMessageStore(offlineStoragePath);
            startOfflineRetryThread();
        }

        logger.info("EnhancedStreamManagerSink initialized: stream={}, host={}, port={}",
                targetGGStream, streamMgrHost, streamMgrPort);
    }

    private void createTargetStream() {
        try {
            this.writeClient.createMessageStream(new MessageStreamDefinition().withName(targetGGStream)
                    .withMaxSize(536870912L)
                    .withStreamSegmentSize(33554432L)
                    .withTimeToLiveMillis(null)
                    .withStrategyOnFull(StrategyOnFull.OverwriteOldestData)
                    .withPersistence(Persistence.File)
                    .withFlushOnWrite(false)
                    .withExportDefinition(
                            new ExportDefinition()
                                    .withKinesis(new ArrayList<KinesisConfig>() {
                                        {
                                            add(new KinesisConfig()
                                                    .withIdentifier("KinesisExport-" + kinesisExportStreamName)
                                                    .withBatchSize(1L)
                                                    .withKinesisStreamName(kinesisExportStreamName));
                                        }
                                    })
                    )
            );
            logger.info("Created StreamManager stream: {}", targetGGStream);
        } catch (StreamManagerException e) {
            logger.warn("Stream may already exist or failed to create: {}", e.getMessage());
        }
    }

    private void startOfflineRetryThread() {
        retryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "offline-retry-thread");
            t.setDaemon(true);
            return t;
        });

        retryExecutor.scheduleAtFixedRate(() -> {
            if (!isRunning) return;
            try {
                retryOfflineMessages();
            } catch (Exception e) {
                logger.error("Error in offline retry thread", e);
            }
        }, OFFLINE_STORAGE_CHECK_INTERVAL_MS, OFFLINE_STORAGE_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);

        logger.info("Offline retry thread started");
    }

    @Override
    public void invoke(AggregatedWindow window, Context context) throws Exception {
        bufferedWindows.add(window);

        if (shouldPublish()) {
            flushBufferedWindows();
        }
    }

    private void flushBufferedWindows() {
        List<AggregatedWindow> windowsToSend = new ArrayList<>();
        bufferedWindows.drainTo(windowsToSend, batchSize);

        if (!windowsToSend.isEmpty()) {
            writeBatch(windowsToSend);
            emptyListTimestamp = System.currentTimeMillis();
        }
    }

    private void writeBatch(List<AggregatedWindow> windowsToSend) {
        for (AggregatedWindow window : windowsToSend) {
            String transactionId = UUID.randomUUID().toString();
            long checkpointId = -1;

            PendingTransaction transaction = new PendingTransaction();
            transaction.transactionId = transactionId;
            transaction.checkpointId = checkpointId;
            transaction.messageIds = window.getMessageIds();
            transaction.idempotencyKeys = window.getIdempotencyKeys();

            Map<String, Object> envelope = new HashMap<>();
            envelope.put("transaction_id", transactionId);
            envelope.put("checkpoint_id", checkpointId);
            envelope.put("window_id", window.getWindowId());
            envelope.put("device_id", window.getDeviceId());
            envelope.put("window_start_ms", window.getWindowStartMs());
            envelope.put("window_end_ms", window.getWindowEndMs());
            envelope.put("message_count", window.getMessageCount());
            envelope.put("aggregated_measurements", window.getAggregatedMeasurements());
            envelope.put("aggregated_anomaly_scores", window.getAggregatedAnomalyScores());
            envelope.put("message_ids", window.getMessageIds());
            envelope.put("idempotency_keys", window.getIdempotencyKeys());
            envelope.put("edge_processed_timestamp_ms", window.getEdgeProcessedTimestampMs());
            envelope.put("is_suspect_window", window.isSuspectWindow());
            envelope.put("metadata", window.getMetadata());

            String payload = gson.toJson(envelope);

            try {
                writeClient.appendMessage(targetGGStream, payload.getBytes());
                transaction.isCommitted = true;
                pendingTransactions.put(transactionId, transaction);

                logger.debug("Successfully appended window: {} with {} messages",
                        window.getWindowId(), window.getMessageCount());

            } catch (StreamManagerException e) {
                logger.error("Failed to write to StreamManager, storing offline: {}", e.getMessage());
                storeOffline(window, payload);
            }
        }
    }

    private void storeOffline(AggregatedWindow window, String payload) {
        if (offlineMessageStore != null) {
            try {
                int priority = window.hasHighAnomalyScore(1.5) ? 1 : 0;
                offlineMessageStore.storeMessage(payload, priority);
            } catch (Exception e) {
                logger.error("Failed to store message offline", e);
            }
        }
    }

    private void retryOfflineMessages() {
        if (offlineMessageStore == null || !offlineMessageStore.hasPendingMessages()) {
            return;
        }

        List<StoredMessage> messagesToForward = offlineMessageStore.getMessagesForForwarding(DEFAULT_FORWARD_BATCH_SIZE);

        for (StoredMessage message : messagesToForward) {
            try {
                writeClient.appendMessage(targetGGStream, message.getPayload().getBytes());
                offlineMessageStore.confirmDelivery(message.getMessageId());
                logger.info("Successfully retried offline message: {}", message.getMessageId());
            } catch (StreamManagerException e) {
                logger.warn("Failed to retry message {}, will retry later: {}",
                        message.getMessageId(), e.getMessage());
                offlineMessageStore.failDelivery(message.getMessageId(), true);
            }
        }
    }

    private boolean shouldPublish() {
        if (bufferedWindows.size() >= batchSize) {
            return true;
        }
        return System.currentTimeMillis() - emptyListTimestamp >= RECORDS_FLUSH_INTERVAL_MILLISECONDS;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        logger.debug("Taking snapshot for checkpoint: {}", context.getCheckpointId());

        transactionState.clear();

        flushBufferedWindows();

        for (PendingTransaction transaction : pendingTransactions.values()) {
            if (!transaction.isCommitted) {
                transactionState.add(transaction);
            }
        }

        for (AggregatedWindow window : bufferedWindows) {
            PendingTransaction transaction = new PendingTransaction();
            transaction.messageIds = window.getMessageIds();
            transaction.idempotencyKeys = window.getIdempotencyKeys();
            transactionState.add(transaction);
        }

        pendingTransactions.clear();

        logger.debug("Snapshot completed for checkpoint: {}", context.getCheckpointId());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<PendingTransaction> descriptor = new ListStateDescriptor<>(
                "pendingTransactions",
                TypeInformation.of(new TypeHint<PendingTransaction>() {}));

        transactionState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            logger.info("Restoring state from checkpoint");
            for (PendingTransaction transaction : transactionState.get()) {
                pendingTransactions.put(transaction.transactionId, transaction);
            }
            logger.info("Restored {} pending transactions", pendingTransactions.size());
        }
    }

    @Override
    public void close() throws Exception {
        isRunning = false;

        flushBufferedWindows();

        if (retryExecutor != null) {
            retryExecutor.shutdown();
            try {
                if (!retryExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    retryExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                retryExecutor.shutdownNow();
            }
        }

        if (offlineMessageStore != null) {
            offlineMessageStore.close();
        }

        if (writeClient != null) {
            writeClient.close();
        }

        super.close();
        logger.info("EnhancedStreamManagerSink closed");
    }
}
