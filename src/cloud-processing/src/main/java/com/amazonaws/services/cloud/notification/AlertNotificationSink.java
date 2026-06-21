/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.cloud.notification;

import com.amazonaws.services.common.model.AnomalyAlert;
import com.amazonaws.services.common.model.AnomalyAlert.AlertStatus;
import com.amazonaws.services.common.model.AnomalyAlert.Severity;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class AlertNotificationSink extends RichSinkFunction<AnomalyAlert> {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(AlertNotificationSink.class);

    private static final long DEFAULT_ALERT_COOLDOWN_MS = 60000;
    private static final int DEFAULT_BATCH_SIZE = 10;
    private static final long DEFAULT_FLUSH_INTERVAL_MS = 5000;

    private final List<NotificationChannel> channels;
    private final long alertCooldownMs;
    private final int batchSize;
    private final long flushIntervalMs;

    private transient MapState<String, Long> lastNotificationState;
    private transient ListState<AnomalyAlert> pendingAlertsState;
    private transient BlockingQueue<AnomalyAlert> alertQueue;
    private transient ScheduledExecutorService flushExecutor;
    private transient volatile boolean isRunning;
    private final AtomicLong totalAlertsProcessed;
    private final AtomicLong totalNotificationsSent;
    private final AtomicLong totalNotificationsFailed;

    public interface NotificationChannel extends Serializable {
        String getName();
        boolean send(AnomalyAlert alert) throws Exception;
        boolean supportsSeverity(Severity severity);
        void close() throws Exception;
    }

    public static class ConsoleNotificationChannel implements NotificationChannel {
        private static final long serialVersionUID = 1L;

        @Override
        public String getName() {
            return "CONSOLE";
        }

        @Override
        public boolean send(AnomalyAlert alert) throws Exception {
            System.out.println("[" + new Date() + "] ALERT: " + alert);
            return true;
        }

        @Override
        public boolean supportsSeverity(Severity severity) {
            return true;
        }

        @Override
        public void close() throws Exception {
        }
    }

    public static class LogNotificationChannel implements NotificationChannel {
        private static final long serialVersionUID = 1L;
        private static final Logger log = LoggerFactory.getLogger(LogNotificationChannel.class);

        @Override
        public String getName() {
            return "LOG";
        }

        @Override
        public boolean send(AnomalyAlert alert) throws Exception {
            log.warn("Anomaly Alert: id={}, device={}, type={}, severity={}, score={}",
                    alert.getAlertId(), alert.getDeviceId(), alert.getAlertType(),
                    alert.getSeverity(), alert.getAnomalyScore());
            return true;
        }

        @Override
        public boolean supportsSeverity(Severity severity) {
            return true;
        }

        @Override
        public void close() throws Exception {
        }
    }

    public static class SnsNotificationChannel implements NotificationChannel {
        private static final long serialVersionUID = 1L;
        private final String topicArn;
        private final String region;
        private transient com.amazonaws.services.sns.AmazonSNS snsClient;

        public SnsNotificationChannel(String topicArn, String region) {
            this.topicArn = topicArn;
            this.region = region;
        }

        @Override
        public String getName() {
            return "SNS:" + topicArn;
        }

        @Override
        public boolean send(AnomalyAlert alert) throws Exception {
            if (snsClient == null) {
                snsClient = com.amazonaws.services.sns.AmazonSNSClientBuilder.standard()
                        .withRegion(region)
                        .build();
            }

            String subject = String.format("[%s] %s", alert.getSeverity(), alert.getAlertTitle());
            String message = buildSnsMessage(alert);

            com.amazonaws.services.sns.model.PublishRequest request =
                    new com.amazonaws.services.sns.model.PublishRequest(topicArn, message, subject);

            snsClient.publish(request);
            return true;
        }

        private String buildSnsMessage(AnomalyAlert alert) {
            StringBuilder sb = new StringBuilder();
            sb.append("Anomaly Detection Alert\n");
            sb.append("======================\n\n");
            sb.append("Alert ID: ").append(alert.getAlertId()).append("\n");
            sb.append("Device ID: ").append(alert.getDeviceId()).append("\n");
            sb.append("Alert Type: ").append(alert.getAlertType()).append("\n");
            sb.append("Severity: ").append(alert.getSeverity()).append("\n");
            sb.append("Timestamp: ").append(new Date(alert.getTimestampMs())).append("\n");
            sb.append("Anomaly Group: ").append(alert.getAnomalyGroup()).append("\n");
            sb.append("Anomaly Score: ").append(String.format("%.2f", alert.getAnomalyScore())).append("\n");
            sb.append("Threshold: ").append(String.format("%.2f", alert.getAnomalyThreshold())).append("\n");
            sb.append("Description: ").append(alert.getAlertDescription()).append("\n");

            if (alert.getMeasurementsInvolved() != null && !alert.getMeasurementsInvolved().isEmpty()) {
                sb.append("\nInvolved Measurements:\n");
                alert.getMeasurementsInvolved().entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEach(e -> sb.append("  ").append(e.getKey()).append(": ")
                                .append(String.format("%.4f", e.getValue())).append("\n"));
            }

            if (alert.getMetadata() != null && !alert.getMetadata().isEmpty()) {
                sb.append("\nMetadata:\n");
                alert.getMetadata().forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v).append("\n"));
            }

            return sb.toString();
        }

        @Override
        public boolean supportsSeverity(Severity severity) {
            return severity.getLevel() >= Severity.MEDIUM.getLevel();
        }

        @Override
        public void close() throws Exception {
            if (snsClient != null) {
                snsClient.shutdown();
            }
        }
    }

    public static class CloudWatchMetricsChannel implements NotificationChannel {
        private static final long serialVersionUID = 1L;
        private final String namespace;
        private final String region;
        private transient com.amazonaws.services.cloudwatch.AmazonCloudWatch cwClient;

        public CloudWatchMetricsChannel(String namespace, String region) {
            this.namespace = namespace;
            this.region = region;
        }

        @Override
        public String getName() {
            return "CLOUDWATCH:" + namespace;
        }

        @Override
        public boolean send(AnomalyAlert alert) throws Exception {
            if (cwClient == null) {
                cwClient = com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder.standard()
                        .withRegion(region)
                        .build();
            }

            List<com.amazonaws.services.cloudwatch.model.Dimension> dimensions = new ArrayList<>();
            dimensions.add(new com.amazonaws.services.cloudwatch.model.Dimension()
                    .withName("DeviceId").withValue(alert.getDeviceId()));
            dimensions.add(new com.amazonaws.services.cloudwatch.model.Dimension()
                    .withName("AnomalyGroup").withValue(alert.getAnomalyGroup()));
            dimensions.add(new com.amazonaws.services.cloudwatch.model.Dimension()
                    .withName("Severity").withValue(alert.getSeverity().toString()));

            List<com.amazonaws.services.cloudwatch.model.MetricDatum> data = new ArrayList<>();
            data.add(new com.amazonaws.services.cloudwatch.model.MetricDatum()
                    .withMetricName("AnomalyScore")
                    .withValue(alert.getAnomalyScore())
                    .withUnit(com.amazonaws.services.cloudwatch.model.StandardUnit.None)
                    .withTimestamp(new Date(alert.getTimestampMs()))
                    .withDimensions(dimensions));

            data.add(new com.amazonaws.services.cloudwatch.model.MetricDatum()
                    .withMetricName("AnomalyAlertCount")
                    .withValue(1.0)
                    .withUnit(com.amazonaws.services.cloudwatch.model.StandardUnit.Count)
                    .withTimestamp(new Date(alert.getTimestampMs()))
                    .withDimensions(dimensions));

            cwClient.putMetricData(new com.amazonaws.services.cloudwatch.model.PutMetricDataRequest()
                    .withNamespace(namespace)
                    .withMetricData(data));

            return true;
        }

        @Override
        public boolean supportsSeverity(Severity severity) {
            return true;
        }

        @Override
        public void close() throws Exception {
            if (cwClient != null) {
                cwClient.shutdown();
            }
        }
    }

    public AlertNotificationSink() {
        this(new ArrayList<>(), DEFAULT_ALERT_COOLDOWN_MS, DEFAULT_BATCH_SIZE, DEFAULT_FLUSH_INTERVAL_MS);
    }

    public AlertNotificationSink(List<NotificationChannel> channels, long alertCooldownMs,
                                 int batchSize, long flushIntervalMs) {
        this.channels = channels != null ? channels : new ArrayList<>();
        this.alertCooldownMs = alertCooldownMs;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.alertQueue = new LinkedBlockingQueue<>();
        this.totalAlertsProcessed = new AtomicLong(0);
        this.totalNotificationsSent = new AtomicLong(0);
        this.totalNotificationsFailed = new AtomicLong(0);
    }

    public AlertNotificationSink addChannel(NotificationChannel channel) {
        this.channels.add(channel);
        return this;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRunning = true;

        MapStateDescriptor<String, Long> lastNotificationDescriptor = new MapStateDescriptor<>(
                "lastNotificationState", String.class, Long.class);
        lastNotificationState = getRuntimeContext().getMapState(lastNotificationDescriptor);

        ListStateDescriptor<AnomalyAlert> pendingAlertsDescriptor = new ListStateDescriptor<>(
                "pendingAlerts", AnomalyAlert.class);
        pendingAlertsState = getRuntimeContext().getListState(pendingAlertsDescriptor);

        startFlushThread();

        logger.info("AlertNotificationSink initialized with {} channels", channels.size());
    }

    private void startFlushThread() {
        flushExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "alert-flush-thread");
            t.setDaemon(true);
            return t;
        });

        flushExecutor.scheduleAtFixedRate(() -> {
            if (!isRunning) return;
            try {
                flushAlerts();
            } catch (Exception e) {
                logger.error("Error in alert flush thread", e);
            }
        }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void invoke(AnomalyAlert alert, Context context) throws Exception {
        if (alert == null) {
            return;
        }

        if (alert.getAlertStatus() == null) {
            alert.setAlertStatus(AlertStatus.NEW);
        }

        totalAlertsProcessed.incrementAndGet();

        String suppressionKey = buildSuppressionKey(alert);
        Long lastNotificationTime = lastNotificationState.get(suppressionKey);

        if (lastNotificationTime != null) {
            long timeSinceLastNotification = System.currentTimeMillis() - lastNotificationTime;
            if (timeSinceLastNotification < alertCooldownMs &&
                    alert.getSeverity().getLevel() < Severity.CRITICAL.getLevel()) {
                logger.debug("Alert suppressed due to cooldown: key={}, timeSinceLast={}ms",
                        suppressionKey, timeSinceLastNotification);
                return;
            }
        }

        alertQueue.offer(alert);

        if (alertQueue.size() >= batchSize) {
            flushAlerts();
        }
    }

    private String buildSuppressionKey(AnomalyAlert alert) {
        return alert.getDeviceId() + "|" + alert.getAnomalyGroup() + "|" + alert.getSeverity();
    }

    private void flushAlerts() throws Exception {
        List<AnomalyAlert> alertsToSend = new ArrayList<>();
        alertQueue.drainTo(alertsToSend, batchSize);

        if (alertsToSend.isEmpty()) {
            return;
        }

        for (AnomalyAlert alert : alertsToSend) {
            boolean allSent = true;
            for (NotificationChannel channel : channels) {
                if (!channel.supportsSeverity(alert.getSeverity())) {
                    continue;
                }

                try {
                    boolean sent = channel.send(alert);
                    if (sent) {
                        totalNotificationsSent.incrementAndGet();
                        alert.getNotificationChannels().add(channel.getName());
                    } else {
                        allSent = false;
                        totalNotificationsFailed.incrementAndGet();
                    }
                } catch (Exception e) {
                    logger.error("Failed to send alert via channel {}: {}",
                            channel.getName(), e.getMessage(), e);
                    allSent = false;
                    totalNotificationsFailed.incrementAndGet();
                }
            }

            if (allSent || alert.getSeverity() == Severity.LOW) {
                String suppressionKey = buildSuppressionKey(alert);
                lastNotificationState.put(suppressionKey, System.currentTimeMillis());
            }
        }

        logger.debug("Flushed {} alerts, sent: {}, failed: {}",
                alertsToSend.size(), totalNotificationsSent.get(), totalNotificationsFailed.get());
    }

    @Override
    public void close() throws Exception {
        isRunning = false;

        flushAlerts();

        if (flushExecutor != null) {
            flushExecutor.shutdown();
            try {
                if (!flushExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    flushExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                flushExecutor.shutdownNow();
            }
        }

        for (NotificationChannel channel : channels) {
            try {
                channel.close();
            } catch (Exception e) {
                logger.warn("Failed to close channel: {}", channel.getName(), e);
            }
        }

        super.close();

        logger.info("AlertNotificationSink closed. Stats: processed={}, sent={}, failed={}",
                totalAlertsProcessed.get(), totalNotificationsSent.get(), totalNotificationsFailed.get());
    }
}
