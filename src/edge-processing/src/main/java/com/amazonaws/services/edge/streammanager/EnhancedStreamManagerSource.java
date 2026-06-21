/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.edge.streammanager;

import com.amazonaws.greengrass.streammanager.client.StreamManagerClient;
import com.amazonaws.greengrass.streammanager.client.StreamManagerClientFactory;
import com.amazonaws.greengrass.streammanager.client.config.StreamManagerClientConfig;
import com.amazonaws.greengrass.streammanager.client.config.StreamManagerServerInfo;
import com.amazonaws.greengrass.streammanager.client.exception.NotEnoughMessagesException;
import com.amazonaws.greengrass.streammanager.model.Message;
import com.amazonaws.greengrass.streammanager.model.MessageStreamInfo;
import com.amazonaws.greengrass.streammanager.model.ReadMessagesOptions;
import com.amazonaws.services.common.model.AnomalyMessage;
import com.amazonaws.services.common.utils.BloomFilter;
import com.amazonaws.services.common.utils.ClockDriftCorrector;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class EnhancedStreamManagerSource extends RichSourceFunction<AnomalyMessage> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(EnhancedStreamManagerSource.class);

    private static final int BLOOM_FILTER_EXPECTED_INSERTIONS = 100000;
    private static final double BLOOM_FILTER_FPP = 0.001;
    private static final long DEFAULT_READ_TIMEOUT_MS = 1000;
    private static final long DEFAULT_MIN_MESSAGE_COUNT = 1;
    private static final long DEFAULT_MAX_MESSAGE_COUNT = 100;

    private final String ggSourceStreamName;
    private final String streamMgrHost;
    private final String streamMgrPort;
    private final String deviceId;

    private final BlockingQueue<AnomalyMessage> bufferedRecords;
    private transient ListState<AnomalyMessage> checkPointedState;
    private transient ValueState<Long> sequenceNumberState;
    private transient ValueState<Long> lastProcessedSequenceState;
    private transient ValueState<BloomFilter> bloomFilterState;
    private transient ValueState<ClockDriftCorrector> clockCorrectorState;

    private transient StreamManagerClient readClient;
    private transient volatile boolean isRunning;
    private final AtomicLong totalMessagesRead;
    private final AtomicLong duplicateMessages;
    private final Gson gson;

    public EnhancedStreamManagerSource(String ggSourceStreamName, String streamMgrHost,
                                       String streamMgrPort, String deviceId) {
        this.ggSourceStreamName = ggSourceStreamName;
        this.streamMgrHost = streamMgrHost;
        this.streamMgrPort = streamMgrPort;
        this.deviceId = deviceId;
        this.bufferedRecords = new LinkedBlockingQueue<>();
        this.totalMessagesRead = new AtomicLong(0);
        this.duplicateMessages = new AtomicLong(0);
        this.gson = new Gson();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        final StreamManagerClientConfig clientConfig = StreamManagerClientConfig.builder()
                .logger(logger)
                .serverInfo(StreamManagerServerInfo.builder()
                        .host(streamMgrHost)
                        .port(Integer.parseInt(streamMgrPort)).build())
                .build();
        this.readClient = StreamManagerClientFactory.standard().withClientConfig(clientConfig).build();

        logger.info("EnhancedStreamManagerSource initialized: stream={}, host={}, port={}",
                ggSourceStreamName, streamMgrHost, streamMgrPort);
    }

    @Override
    public void run(SourceContext<AnomalyMessage> ctx) throws Exception {
        isRunning = true;
        Long startSequence = getStartingSequence();

        logger.info("Starting to read from stream {} at sequence {}", ggSourceStreamName, startSequence);

        while (isRunning) {
            try {
                List<Message> messages = readClient.readMessages(ggSourceStreamName,
                        new ReadMessagesOptions()
                                .withDesiredStartSequenceNumber(startSequence)
                                .withMinMessageCount(DEFAULT_MIN_MESSAGE_COUNT)
                                .withMaxMessageCount(DEFAULT_MAX_MESSAGE_COUNT)
                                .withReadTimeoutMillis(DEFAULT_READ_TIMEOUT_MS));

                processMessages(ctx, messages);

                if (!messages.isEmpty()) {
                    startSequence = messages.get(messages.size() - 1).getSequenceNumber() + 1;
                }

            } catch (NotEnoughMessagesException e) {
                Thread.sleep(100);
                continue;
            } catch (Exception e) {
                logger.error("Error reading from StreamManager", e);
                Thread.sleep(1000);
            }
        }
    }

    private Long getStartingSequence() throws Exception {
        Long lastProcessed = lastProcessedSequenceState.value();
        if (lastProcessed != null) {
            logger.info("Resuming from last processed sequence: {}", lastProcessed);
            return lastProcessed + 1;
        }

        try {
            MessageStreamInfo description = readClient.describeMessageStream(ggSourceStreamName);
            return description.getStorageStatus().getNewestSequenceNumber();
        } catch (Exception e) {
            logger.warn("Failed to get stream description, starting from 0", e);
            return 0L;
        }
    }

    private void processMessages(SourceContext<AnomalyMessage> ctx, List<Message> messages) throws Exception {
        BloomFilter bloomFilter = bloomFilterState.value();
        if (bloomFilter == null) {
            bloomFilter = new BloomFilter(BLOOM_FILTER_EXPECTED_INSERTIONS, BLOOM_FILTER_FPP);
            bloomFilterState.update(bloomFilter);
        }

        if (bloomFilter.isNearCapacity()) {
            logger.info("BloomFilter near capacity, rotating");
            bloomFilter = new BloomFilter(BLOOM_FILTER_EXPECTED_INSERTIONS, BLOOM_FILTER_FPP);
            bloomFilterState.update(bloomFilter);
        }

        ClockDriftCorrector clockCorrector = clockCorrectorState.value();
        if (clockCorrector == null) {
            clockCorrector = new ClockDriftCorrector();
            clockCorrectorState.update(clockCorrector);
        }

        long currentCloudTime = System.currentTimeMillis();
        long networkLatencyMs = 10;

        for (Message message : messages) {
            totalMessagesRead.incrementAndGet();

            try {
                AnomalyMessage anomalyMessage = parseMessage(message);
                anomalyMessage.setDeviceId(deviceId);
                anomalyMessage.setSequenceNumber(message.getSequenceNumber());

                long edgeTimestamp = anomalyMessage.getEdgeTimestampMs();

                if (clockCorrector.needsSynchronization(currentCloudTime)) {
                    clockCorrector.recordOffsetSample(edgeTimestamp, currentCloudTime, networkLatencyMs);
                }

                long correctedTimestamp = clockCorrector.correctTimestamp(edgeTimestamp);
                anomalyMessage.setCorrectedTimestampMs(correctedTimestamp);
                anomalyMessage.setClockOffsetMs(clockCorrector.getCurrentOffsetMs());
                anomalyMessage.setCloudTimestampMs(currentCloudTime);

                String dedupKey = anomalyMessage.getIdempotencyKey();
                if (bloomFilter.mightContain(dedupKey)) {
                    anomalyMessage.setDuplicate(true);
                    duplicateMessages.incrementAndGet();
                    logger.debug("Duplicate message detected: {}", dedupKey);
                } else {
                    bloomFilter.put(dedupKey);
                }

                Long expectedSequence = sequenceNumberState.value();
                if (expectedSequence != null && message.getSequenceNumber() != expectedSequence) {
                    long gap = message.getSequenceNumber() - expectedSequence;
                    anomalyMessage.addMetadata("sequence_gap", String.valueOf(gap));
                    logger.warn("Sequence gap detected: expected={}, actual={}, gap={}",
                            expectedSequence, message.getSequenceNumber(), gap);
                }
                sequenceNumberState.update(message.getSequenceNumber() + 1);
                lastProcessedSequenceState.update(message.getSequenceNumber());

                if (clockCorrector.isDriftExcessive(edgeTimestamp)) {
                    anomalyMessage.addMetadata("clock_drift_excessive", "true");
                    logger.warn("Excessive clock drift detected for message: {}", message.getSequenceNumber());
                }

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(anomalyMessage);
                }

            } catch (Exception e) {
                logger.error("Failed to process message: {}", message.getSequenceNumber(), e);
            }
        }

        if (bloomFilter.isNearCapacity()) {
            bloomFilterState.update(bloomFilter);
        }

        clockCorrectorState.update(clockCorrector);
    }

    private AnomalyMessage parseMessage(Message message) {
        String payload = new String(message.getPayload());

        try {
            JsonObject jsonObject = JsonParser.parseString(payload).getAsJsonObject();
            AnomalyMessage anomalyMessage = gson.fromJson(jsonObject, AnomalyMessage.class);
            if (anomalyMessage != null) {
                return anomalyMessage;
            }
        } catch (Exception e) {
            logger.debug("Failed to parse as AnomalyMessage, trying raw format");
        }

        return parseLegacyFormat(payload);
    }

    private AnomalyMessage parseLegacyFormat(String payload) {
        AnomalyMessage message = new AnomalyMessage();
        Map<String, Double> measurements = new HashMap<>();

        try {
            JsonObject jsonObject = JsonParser.parseString(payload).getAsJsonObject();
            for (Map.Entry<String, com.google.gson.JsonElement> entry : jsonObject.entrySet()) {
                String key = entry.getKey();
                com.google.gson.JsonElement value = entry.getValue();
                try {
                    if (value.isJsonPrimitive() && value.getAsJsonPrimitive().isNumber()) {
                        measurements.put(key, value.getAsDouble());
                    }
                } catch (Exception e) {
                    logger.debug("Failed to parse measurement: {}", key);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to parse legacy message format", e);
        }

        message.setMeasurements(measurements);
        return message;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        logger.debug("Taking source snapshot for checkpoint: {}", context.getCheckpointId());

        checkPointedState.clear();

        for (AnomalyMessage record : bufferedRecords) {
            checkPointedState.add(record);
        }

        logger.debug("Source snapshot completed, buffered records: {}", bufferedRecords.size());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<AnomalyMessage> recordDescriptor = new ListStateDescriptor<>(
                "bufferedRecords", AnomalyMessage.class);
        checkPointedState = context.getOperatorStateStore().getListState(recordDescriptor);

        ValueStateDescriptor<Long> sequenceDescriptor = new ValueStateDescriptor<>(
                "sequenceNumber", Long.class);
        sequenceNumberState = context.getKeyedStateStore().getState(sequenceDescriptor);

        ValueStateDescriptor<Long> lastProcessedDescriptor = new ValueStateDescriptor<>(
                "lastProcessedSequence", Long.class);
        lastProcessedSequenceState = context.getKeyedStateStore().getState(lastProcessedDescriptor);

        ValueStateDescriptor<BloomFilter> bloomFilterDescriptor = new ValueStateDescriptor<>(
                "bloomFilter", BloomFilter.class);
        bloomFilterState = context.getKeyedStateStore().getState(bloomFilterDescriptor);

        ValueStateDescriptor<ClockDriftCorrector> clockCorrectorDescriptor = new ValueStateDescriptor<>(
                "clockCorrector", ClockDriftCorrector.class);
        clockCorrectorState = context.getKeyedStateStore().getState(clockCorrectorDescriptor);

        if (context.isRestored()) {
            logger.info("Restoring source state from checkpoint");
            for (AnomalyMessage element : checkPointedState.get()) {
                bufferedRecords.add(element);
            }
            logger.info("Restored {} buffered records", bufferedRecords.size());
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        logger.info("EnhancedStreamManagerSource cancelled. Stats: read={}, duplicates={}",
                totalMessagesRead.get(), duplicateMessages.get());
    }

    @Override
    public void close() throws Exception {
        isRunning = false;
        if (readClient != null) {
            readClient.close();
        }
        super.close();
        logger.info("EnhancedStreamManagerSource closed");
    }

    public long getTotalMessagesRead() {
        return totalMessagesRead.get();
    }

    public long getDuplicateMessages() {
        return duplicateMessages.get();
    }
}
