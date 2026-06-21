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

public class AnomalyAlert implements Serializable {

    private static final long serialVersionUID = 1L;

    @SerializedName("alert_id")
    private String alertId;

    @SerializedName("device_id")
    private String deviceId;

    @SerializedName("alert_type")
    private AlertType alertType;

    @SerializedName("severity")
    private Severity severity;

    @SerializedName("timestamp_ms")
    private long timestampMs;

    @SerializedName("alert_title")
    private String alertTitle;

    @SerializedName("alert_description")
    private String alertDescription;

    @SerializedName("anomaly_group")
    private String anomalyGroup;

    @SerializedName("anomaly_score")
    private double anomalyScore;

    @SerializedName("anomaly_threshold")
    private double anomalyThreshold;

    @SerializedName("related_message_ids")
    private List<String> relatedMessageIds;

    @SerializedName("related_window_ids")
    private List<String> relatedWindowIds;

    @SerializedName("measurements_involved")
    private Map<String, Double> measurementsInvolved;

    @SerializedName("is_acknowledged")
    private boolean isAcknowledged;

    @SerializedName("acknowledged_by")
    private String acknowledgedBy;

    @SerializedName("acknowledged_timestamp_ms")
    private Long acknowledgedTimestampMs;

    @SerializedName("alert_status")
    private AlertStatus alertStatus;

    @SerializedName("notification_channels")
    private List<String> notificationChannels;

    @SerializedName("metadata")
    private Map<String, String> metadata;

    public enum AlertType {
        @SerializedName("SINGLE_POINT")
        SINGLE_POINT,
        @SerializedName("WINDOW_AGGREGATE")
        WINDOW_AGGREGATE,
        @SerializedName("TREND_DETECTION")
        TREND_DETECTION,
        @SerializedName("PATTERN_MATCH")
        PATTERN_MATCH,
        @SerializedName("SYSTEM_ERROR")
        SYSTEM_ERROR,
        @SerializedName("NETWORK_DISCONNECT")
        NETWORK_DISCONNECT,
        @SerializedName("CLOCK_DRIFT")
        CLOCK_DRIFT
    }

    public enum Severity {
        @SerializedName("LOW")
        LOW(1),
        @SerializedName("MEDIUM")
        MEDIUM(2),
        @SerializedName("HIGH")
        HIGH(3),
        @SerializedName("CRITICAL")
        CRITICAL(4);

        private final int level;

        Severity(int level) {
            this.level = level;
        }

        public int getLevel() {
            return level;
        }

        public static Severity fromScore(double score, double threshold) {
            double ratio = score / threshold;
            if (ratio >= 3.0) return CRITICAL;
            if (ratio >= 2.0) return HIGH;
            if (ratio >= 1.5) return MEDIUM;
            return LOW;
        }
    }

    public enum AlertStatus {
        @SerializedName("NEW")
        NEW,
        @SerializedName("ESCALATED")
        ESCALATED,
        @SerializedName("ACKNOWLEDGED")
        ACKNOWLEDGED,
        @SerializedName("RESOLVED")
        RESOLVED,
        @SerializedName("DISMISSED")
        DISMISSED
    }

    public AnomalyAlert() {
        this.relatedMessageIds = new ArrayList<>();
        this.relatedWindowIds = new ArrayList<>();
        this.measurementsInvolved = new HashMap<>();
        this.notificationChannels = new ArrayList<>();
        this.metadata = new HashMap<>();
        this.alertStatus = AlertStatus.NEW;
        this.isAcknowledged = false;
        this.timestampMs = System.currentTimeMillis();
    }

    public AnomalyAlert(String deviceId, AlertType alertType, String anomalyGroup,
                        double anomalyScore, double anomalyThreshold) {
        this();
        this.deviceId = deviceId;
        this.alertType = alertType;
        this.anomalyGroup = anomalyGroup;
        this.anomalyScore = anomalyScore;
        this.anomalyThreshold = anomalyThreshold;
        this.severity = Severity.fromScore(anomalyScore, anomalyThreshold);
        this.alertTitle = generateAlertTitle();
        this.alertDescription = generateAlertDescription();
    }

    private String generateAlertTitle() {
        return String.format("%s Anomaly Detected on %s - %s",
                severity, deviceId, anomalyGroup);
    }

    private String generateAlertDescription() {
        return String.format("Anomaly score %.2f exceeds threshold %.2f (%.2fx). Group: %s, Device: %s",
                anomalyScore, anomalyThreshold, anomalyScore / anomalyThreshold,
                anomalyGroup, deviceId);
    }

    public static AnomalyAlert fromMessage(AnomalyMessage message, String groupName,
                                           double score, double threshold) {
        AnomalyAlert alert = new AnomalyAlert(
                message.getDeviceId(),
                AlertType.SINGLE_POINT,
                groupName,
                score,
                threshold
        );
        alert.getRelatedMessageIds().add(message.getMessageId());
        alert.setTimestampMs(message.getEdgeTimestampMs());
        if (message.getMeasurements() != null) {
            alert.setMeasurementsInvolved(new HashMap<>(message.getMeasurements()));
        }
        return alert;
    }

    public static AnomalyAlert fromWindow(AggregatedWindow window, String groupName,
                                          AggregatedWindow.AnomalyScoreStats stats,
                                          double threshold) {
        AnomalyAlert alert = new AnomalyAlert(
                window.getDeviceId(),
                AlertType.WINDOW_AGGREGATE,
                groupName,
                stats.getMax(),
                threshold
        );
        alert.setTimestampMs(stats.getMaxTimestampMs());
        alert.getRelatedMessageIds().addAll(window.getMessageIds());
        alert.getRelatedWindowIds().add(window.getWindowId());
        alert.setAnomalyScore(stats.getMax());
        return alert;
    }

    public String getAlertId() {
        return alertId;
    }

    public void setAlertId(String alertId) {
        this.alertId = alertId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public AlertType getAlertType() {
        return alertType;
    }

    public void setAlertType(AlertType alertType) {
        this.alertType = alertType;
    }

    public Severity getSeverity() {
        return severity;
    }

    public void setSeverity(Severity severity) {
        this.severity = severity;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public void setTimestampMs(long timestampMs) {
        this.timestampMs = timestampMs;
    }

    public String getAlertTitle() {
        return alertTitle;
    }

    public void setAlertTitle(String alertTitle) {
        this.alertTitle = alertTitle;
    }

    public String getAlertDescription() {
        return alertDescription;
    }

    public void setAlertDescription(String alertDescription) {
        this.alertDescription = alertDescription;
    }

    public String getAnomalyGroup() {
        return anomalyGroup;
    }

    public void setAnomalyGroup(String anomalyGroup) {
        this.anomalyGroup = anomalyGroup;
    }

    public double getAnomalyScore() {
        return anomalyScore;
    }

    public void setAnomalyScore(double anomalyScore) {
        this.anomalyScore = anomalyScore;
    }

    public double getAnomalyThreshold() {
        return anomalyThreshold;
    }

    public void setAnomalyThreshold(double anomalyThreshold) {
        this.anomalyThreshold = anomalyThreshold;
    }

    public List<String> getRelatedMessageIds() {
        return relatedMessageIds;
    }

    public void setRelatedMessageIds(List<String> relatedMessageIds) {
        this.relatedMessageIds = relatedMessageIds;
    }

    public List<String> getRelatedWindowIds() {
        return relatedWindowIds;
    }

    public void setRelatedWindowIds(List<String> relatedWindowIds) {
        this.relatedWindowIds = relatedWindowIds;
    }

    public Map<String, Double> getMeasurementsInvolved() {
        return measurementsInvolved;
    }

    public void setMeasurementsInvolved(Map<String, Double> measurementsInvolved) {
        this.measurementsInvolved = measurementsInvolved;
    }

    public boolean isAcknowledged() {
        return isAcknowledged;
    }

    public void setAcknowledged(boolean acknowledged) {
        isAcknowledged = acknowledged;
    }

    public String getAcknowledgedBy() {
        return acknowledgedBy;
    }

    public void setAcknowledgedBy(String acknowledgedBy) {
        this.acknowledgedBy = acknowledgedBy;
    }

    public Long getAcknowledgedTimestampMs() {
        return acknowledgedTimestampMs;
    }

    public void setAcknowledgedTimestampMs(Long acknowledgedTimestampMs) {
        this.acknowledgedTimestampMs = acknowledgedTimestampMs;
    }

    public AlertStatus getAlertStatus() {
        return alertStatus;
    }

    public void setAlertStatus(AlertStatus alertStatus) {
        this.alertStatus = alertStatus;
    }

    public List<String> getNotificationChannels() {
        return notificationChannels;
    }

    public void setNotificationChannels(List<String> notificationChannels) {
        this.notificationChannels = notificationChannels;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    public static AnomalyAlert fromJson(String json) {
        return new Gson().fromJson(json, AnomalyAlert.class);
    }

    @Override
    public String toString() {
        return "AnomalyAlert{" +
                "alertId='" + alertId + '\'' +
                ", deviceId='" + deviceId + '\'' +
                ", alertType=" + alertType +
                ", severity=" + severity +
                ", timestampMs=" + timestampMs +
                ", anomalyScore=" + anomalyScore +
                ", alertStatus=" + alertStatus +
                '}';
    }
}
