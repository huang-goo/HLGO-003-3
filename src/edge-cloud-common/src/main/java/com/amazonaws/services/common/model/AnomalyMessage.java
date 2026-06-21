/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.common.model;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class AnomalyMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    @SerializedName("message_id")
    private String messageId;

    @SerializedName("idempotency_key")
    private String idempotencyKey;

    @SerializedName("sequence_number")
    private long sequenceNumber;

    @SerializedName("device_id")
    private String deviceId;

    @SerializedName("edge_timestamp_ms")
    private long edgeTimestampMs;

    @SerializedName("cloud_timestamp_ms")
    private Long cloudTimestampMs;

    @SerializedName("corrected_timestamp_ms")
    private Long correctedTimestampMs;

    @SerializedName("clock_offset_ms")
    private Long clockOffsetMs;

    @SerializedName("measurements")
    private Map<String, Double> measurements;

    @SerializedName("anomaly_scores")
    private Map<String, Double> anomalyScores;

    @SerializedName("is_duplicate")
    private boolean isDuplicate;

    @SerializedName("retry_count")
    private int retryCount;

    @SerializedName("processing_stage")
    private ProcessingStage processingStage;

    @SerializedName("checkpoint_id")
    private String checkpointId;

    @SerializedName("metadata")
    private Map<String, String> metadata;

    public enum ProcessingStage {
        @SerializedName("RAW")
        RAW,
        @SerializedName("EDGE_PREPROCESSED")
        EDGE_PREPROCESSED,
        @SerializedName("EDGE_AGGREGATED")
        EDGE_AGGREGATED,
        @SerializedName("EDGE_DEDUPED")
        EDGE_DEDUPED,
        @SerializedName("CLOUD_RECEIVED")
        CLOUD_RECEIVED,
        @SerializedName("CLOUD_SCORED")
        CLOUD_SCORED,
        @SerializedName("CLOUD_ALERTED")
        CLOUD_ALERTED
    }

    public AnomalyMessage() {
        this.messageId = UUID.randomUUID().toString();
        this.idempotencyKey = UUID.randomUUID().toString();
        this.processingStage = ProcessingStage.RAW;
        this.measurements = new HashMap<>();
        this.anomalyScores = new HashMap<>();
        this.metadata = new HashMap<>();
        this.edgeTimestampMs = System.currentTimeMillis();
        this.retryCount = 0;
        this.isDuplicate = false;
    }

    public AnomalyMessage(String deviceId, Map<String, Double> measurements) {
        this();
        this.deviceId = deviceId;
        this.measurements = measurements != null ? measurements : new HashMap<>();
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public long getEdgeTimestampMs() {
        return edgeTimestampMs;
    }

    public void setEdgeTimestampMs(long edgeTimestampMs) {
        this.edgeTimestampMs = edgeTimestampMs;
    }

    public Long getCloudTimestampMs() {
        return cloudTimestampMs;
    }

    public void setCloudTimestampMs(Long cloudTimestampMs) {
        this.cloudTimestampMs = cloudTimestampMs;
    }

    public Long getCorrectedTimestampMs() {
        return correctedTimestampMs;
    }

    public void setCorrectedTimestampMs(Long correctedTimestampMs) {
        this.correctedTimestampMs = correctedTimestampMs;
    }

    public Long getClockOffsetMs() {
        return clockOffsetMs;
    }

    public void setClockOffsetMs(Long clockOffsetMs) {
        this.clockOffsetMs = clockOffsetMs;
    }

    public Map<String, Double> getMeasurements() {
        return measurements;
    }

    public void setMeasurements(Map<String, Double> measurements) {
        this.measurements = measurements;
    }

    public Map<String, Double> getAnomalyScores() {
        return anomalyScores;
    }

    public void setAnomalyScores(Map<String, Double> anomalyScores) {
        this.anomalyScores = anomalyScores;
    }

    public void addAnomalyScore(String groupName, double score) {
        if (this.anomalyScores == null) {
            this.anomalyScores = new HashMap<>();
        }
        this.anomalyScores.put(groupName, score);
    }

    public boolean isDuplicate() {
        return isDuplicate;
    }

    public void setDuplicate(boolean duplicate) {
        isDuplicate = duplicate;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public void incrementRetryCount() {
        this.retryCount++;
    }

    public ProcessingStage getProcessingStage() {
        return processingStage;
    }

    public void setProcessingStage(ProcessingStage processingStage) {
        this.processingStage = processingStage;
    }

    public String getCheckpointId() {
        return checkpointId;
    }

    public void setCheckpointId(String checkpointId) {
        this.checkpointId = checkpointId;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public void addMetadata(String key, String value) {
        if (this.metadata == null) {
            this.metadata = new HashMap<>();
        }
        this.metadata.put(key, value);
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    public static AnomalyMessage fromJson(String json) {
        return new Gson().fromJson(json, AnomalyMessage.class);
    }

    public String generateDeduplicationKey() {
        StringBuilder sb = new StringBuilder();
        sb.append(deviceId).append("|");
        sb.append(edgeTimestampMs).append("|");
        if (measurements != null && !measurements.isEmpty()) {
            measurements.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> sb.append(e.getKey()).append(":").append(e.getValue()).append("|"));
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "AnomalyMessage{" +
                "messageId='" + messageId + '\'' +
                ", idempotencyKey='" + idempotencyKey + '\'' +
                ", sequenceNumber=" + sequenceNumber +
                ", deviceId='" + deviceId + '\'' +
                ", edgeTimestampMs=" + edgeTimestampMs +
                ", processingStage=" + processingStage +
                ", isDuplicate=" + isDuplicate +
                ", retryCount=" + retryCount +
                '}';
    }
}
