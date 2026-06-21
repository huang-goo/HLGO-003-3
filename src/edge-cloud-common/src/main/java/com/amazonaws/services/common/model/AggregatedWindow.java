/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.common.model;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregatedWindow implements Serializable {

    private static final long serialVersionUID = 1L;

    @SerializedName("window_id")
    private String windowId;

    @SerializedName("device_id")
    private String deviceId;

    @SerializedName("window_start_ms")
    private long windowStartMs;

    @SerializedName("window_end_ms")
    private long windowEndMs;

    @SerializedName("message_count")
    private int messageCount;

    @SerializedName("aggregated_measurements")
    private Map<String, MeasurementStats> aggregatedMeasurements;

    @SerializedName("aggregated_anomaly_scores")
    private Map<String, AnomalyScoreStats> aggregatedAnomalyScores;

    @SerializedName("message_ids")
    private List<String> messageIds;

    @SerializedName("idempotency_keys")
    private List<String> idempotencyKeys;

    @SerializedName("edge_processed_timestamp_ms")
    private long edgeProcessedTimestampMs;

    @SerializedName("is_suspect_window")
    private boolean isSuspectWindow;

    @SerializedName("metadata")
    private Map<String, String> metadata;

    public static class MeasurementStats implements Serializable {
        private static final long serialVersionUID = 1L;

        @SerializedName("count")
        private int count;

        @SerializedName("sum")
        private double sum;

        @SerializedName("min")
        private double min;

        @SerializedName("max")
        private double max;

        @SerializedName("avg")
        private double avg;

        @SerializedName("variance")
        private double variance;

        @SerializedName("std_dev")
        private double stdDev;

        @SerializedName("first")
        private double first;

        @SerializedName("last")
        private double last;

        public MeasurementStats() {
            this.count = 0;
            this.sum = 0;
            this.min = Double.MAX_VALUE;
            this.max = Double.MIN_VALUE;
        }

        public void addValue(double value) {
            if (count == 0) {
                first = value;
                min = value;
                max = value;
            }
            count++;
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
            last = value;
            avg = sum / count;
            variance = (variance * (count - 1) + (value - avg) * (value - avg)) / count;
            stdDev = Math.sqrt(variance);
        }

        public int getCount() {
            return count;
        }

        public double getSum() {
            return sum;
        }

        public double getMin() {
            return min;
        }

        public double getMax() {
            return max;
        }

        public double getAvg() {
            return avg;
        }

        public double getVariance() {
            return variance;
        }

        public double getStdDev() {
            return stdDev;
        }

        public double getFirst() {
            return first;
        }

        public double getLast() {
            return last;
        }
    }

    public static class AnomalyScoreStats implements Serializable {
        private static final long serialVersionUID = 1L;

        @SerializedName("count")
        private int count;

        @SerializedName("sum")
        private double sum;

        @SerializedName("min")
        private double min;

        @SerializedName("max")
        private double max;

        @SerializedName("avg")
        private double avg;

        @SerializedName("max_timestamp_ms")
        private long maxTimestampMs;

        @SerializedName("anomaly_count")
        private int anomalyCount;

        @SerializedName("threshold")
        private double threshold;

        public AnomalyScoreStats() {
            this.count = 0;
            this.sum = 0;
            this.min = Double.MAX_VALUE;
            this.max = Double.MIN_VALUE;
            this.anomalyCount = 0;
            this.threshold = 1.0;
        }

        public void addScore(double score, long timestampMs) {
            count++;
            sum += score;
            if (score > max) {
                max = score;
                maxTimestampMs = timestampMs;
            }
            min = Math.min(min, score);
            avg = sum / count;
            if (score > threshold) {
                anomalyCount++;
            }
        }

        public int getCount() {
            return count;
        }

        public double getSum() {
            return sum;
        }

        public double getMin() {
            return min;
        }

        public double getMax() {
            return max;
        }

        public double getAvg() {
            return avg;
        }

        public long getMaxTimestampMs() {
            return maxTimestampMs;
        }

        public int getAnomalyCount() {
            return anomalyCount;
        }

        public double getThreshold() {
            return threshold;
        }

        public void setThreshold(double threshold) {
            this.threshold = threshold;
        }

        public double getAnomalyRatio() {
            return count > 0 ? (double) anomalyCount / count : 0.0;
        }
    }

    public AggregatedWindow() {
        this.aggregatedMeasurements = new HashMap<>();
        this.aggregatedAnomalyScores = new HashMap<>();
        this.messageIds = new ArrayList<>();
        this.idempotencyKeys = new ArrayList<>();
        this.metadata = new HashMap<>();
        this.edgeProcessedTimestampMs = System.currentTimeMillis();
    }

    public AggregatedWindow(String deviceId, long windowStartMs, long windowEndMs) {
        this();
        this.deviceId = deviceId;
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
        this.windowId = deviceId + "_" + windowStartMs + "_" + windowEndMs;
    }

    public void addMessage(AnomalyMessage message) {
        messageCount++;
        messageIds.add(message.getMessageId());
        idempotencyKeys.add(message.getIdempotencyKey());

        if (message.getMeasurements() != null) {
            for (Map.Entry<String, Double> entry : message.getMeasurements().entrySet()) {
                String key = entry.getKey();
                Double value = entry.getValue();
                if (value != null) {
                    aggregatedMeasurements.computeIfAbsent(key, k -> new MeasurementStats())
                            .addValue(value);
                }
            }
        }

        if (message.getAnomalyScores() != null) {
            for (Map.Entry<String, Double> entry : message.getAnomalyScores().entrySet()) {
                String key = entry.getKey();
                Double value = entry.getValue();
                if (value != null) {
                    aggregatedAnomalyScores.computeIfAbsent(key, k -> new AnomalyScoreStats())
                            .addScore(value, message.getEdgeTimestampMs());
                }
            }
        }
    }

    public boolean hasHighAnomalyScore(double threshold) {
        return aggregatedAnomalyScores.values().stream()
                .anyMatch(stats -> stats.getMax() > threshold);
    }

    public String getWindowId() {
        return windowId;
    }

    public void setWindowId(String windowId) {
        this.windowId = windowId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public long getWindowStartMs() {
        return windowStartMs;
    }

    public void setWindowStartMs(long windowStartMs) {
        this.windowStartMs = windowStartMs;
    }

    public long getWindowEndMs() {
        return windowEndMs;
    }

    public void setWindowEndMs(long windowEndMs) {
        this.windowEndMs = windowEndMs;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(int messageCount) {
        this.messageCount = messageCount;
    }

    public Map<String, MeasurementStats> getAggregatedMeasurements() {
        return aggregatedMeasurements;
    }

    public void setAggregatedMeasurements(Map<String, MeasurementStats> aggregatedMeasurements) {
        this.aggregatedMeasurements = aggregatedMeasurements;
    }

    public Map<String, AnomalyScoreStats> getAggregatedAnomalyScores() {
        return aggregatedAnomalyScores;
    }

    public void setAggregatedAnomalyScores(Map<String, AnomalyScoreStats> aggregatedAnomalyScores) {
        this.aggregatedAnomalyScores = aggregatedAnomalyScores;
    }

    public List<String> getMessageIds() {
        return messageIds;
    }

    public void setMessageIds(List<String> messageIds) {
        this.messageIds = messageIds;
    }

    public List<String> getIdempotencyKeys() {
        return idempotencyKeys;
    }

    public void setIdempotencyKeys(List<String> idempotencyKeys) {
        this.idempotencyKeys = idempotencyKeys;
    }

    public long getEdgeProcessedTimestampMs() {
        return edgeProcessedTimestampMs;
    }

    public void setEdgeProcessedTimestampMs(long edgeProcessedTimestampMs) {
        this.edgeProcessedTimestampMs = edgeProcessedTimestampMs;
    }

    public boolean isSuspectWindow() {
        return isSuspectWindow;
    }

    public void setSuspectWindow(boolean suspectWindow) {
        isSuspectWindow = suspectWindow;
    }

    public Map<String, String> getMetadata() {
        if (metadata == null) {
            metadata = new HashMap<>();
        }
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    public static AggregatedWindow fromJson(String json) {
        return new Gson().fromJson(json, AggregatedWindow.class);
    }
}
