/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.cloud.operators;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.runner.LineTransformer;
import com.amazon.randomcutforest.util.ShingleBuilder;
import com.amazonaws.services.common.model.AggregatedWindow;
import com.amazonaws.services.common.model.AggregatedWindow.AnomalyScoreStats;
import com.amazonaws.services.common.model.AnomalyAlert;
import com.amazonaws.services.common.model.AnomalyAlert.AlertType;
import com.amazonaws.services.common.model.AnomalyAlert.Severity;
import com.amazonaws.services.common.model.AnomalyMessage;
import com.amazonaws.services.common.model.AnomalyMessage.ProcessingStage;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class CloudSecondaryScoringFunction extends KeyedProcessFunction<String, AggregatedWindow, AggregatedWindow> {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(CloudSecondaryScoringFunction.class);

    public static final OutputTag<AnomalyAlert> ALERT_OUTPUT_TAG = new OutputTag<AnomalyAlert>("anomaly-alerts") {};

    private final ParameterTool parameter;
    private final List<String> measureGroups;
    private final double anomalyThreshold;

    private transient ValueState<ScoringState> scoringState;
    private transient MapState<String, Double> historicalThresholds;
    private transient ValueState<Long> lastAlertTimestamp;

    public static class ScoringState implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        Map<String, RandomCutForest> forests;
        Map<String, ShingleBuilder> shingleBuilders;
        Map<String, double[]> pointBuffers;
        Map<String, double[]> shingleBuffers;
        Map<String, LineTransformer> algorithms;
        Map<String, LinkedList<Double>> scoreHistory;
        Map<String, Double> adaptiveThresholds;
        long totalWindowsProcessed;
        long totalAlertsGenerated;
        double globalAnomalyRate;

        public ScoringState() {
            this.forests = new HashMap<>();
            this.shingleBuilders = new HashMap<>();
            this.pointBuffers = new HashMap<>();
            this.shingleBuffers = new HashMap<>();
            this.algorithms = new HashMap<>();
            this.scoreHistory = new HashMap<>();
            this.adaptiveThresholds = new HashMap<>();
            this.totalWindowsProcessed = 0;
            this.totalAlertsGenerated = 0;
            this.globalAnomalyRate = 0.0;
        }
    }

    public static class AnomalyScoreTransformer implements LineTransformer {
        private final RandomCutForest forest;

        public AnomalyScoreTransformer(RandomCutForest forest) {
            this.forest = forest;
        }

        @Override
        public List<String> getResultValues(double... point) {
            double score = forest.getAnomalyScore(point);
            forest.update(point);
            return Collections.singletonList(Double.toString(score));
        }

        @Override
        public List<String> getEmptyResultValue() {
            return Collections.singletonList("NA");
        }

        @Override
        public List<String> getResultColumnNames() {
            return Collections.singletonList("anomaly_score");
        }

        @Override
        public RandomCutForest getForest() {
            return forest;
        }
    }

    public CloudSecondaryScoringFunction(ParameterTool parameter, List<String> measureGroups) {
        this.parameter = parameter;
        this.measureGroups = measureGroups;
        this.anomalyThreshold = Double.parseDouble(parameter.get("AnomalyThreshold", "1.5"));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<ScoringState> stateDescriptor = new ValueStateDescriptor<>(
                "scoringState", ScoringState.class);
        scoringState = getRuntimeContext().getState(stateDescriptor);

        MapStateDescriptor<String, Double> thresholdDescriptor = new MapStateDescriptor<>(
                "historicalThresholds", String.class, Double.class);
        historicalThresholds = getRuntimeContext().getMapState(thresholdDescriptor);

        ValueStateDescriptor<Long> lastAlertDescriptor = new ValueStateDescriptor<>(
                "lastAlertTimestamp", Long.class);
        lastAlertTimestamp = getRuntimeContext().getState(lastAlertDescriptor);

        logger.info("CloudSecondaryScoringFunction initialized with {} groups, threshold: {}",
                measureGroups.size(), anomalyThreshold);
    }

    @Override
    public void processElement(AggregatedWindow window, Context ctx, Collector<AggregatedWindow> out) throws Exception {
        ScoringState state = scoringState.value();
        if (state == null) {
            state = new ScoringState();
            initializeDetectors(state, window);
            scoringState.update(state);
        }

        window.getMetadata().put("cloud_scoring_start_ms", String.valueOf(System.currentTimeMillis()));

        Map<String, AnomalyScoreStats> secondaryScores = new HashMap<>();

        for (String groupName : measureGroups) {
            AnomalyScoreStats edgeStats = window.getAggregatedAnomalyScores().get(groupName);
            if (edgeStats == null) {
                continue;
            }

            double[] featureVector = buildFeatureVector(window, groupName);
            double secondaryScore = calculateSecondaryScore(state, groupName, featureVector);

            AnomalyScoreStats secondaryStats = new AnomalyScoreStats();
            secondaryStats.setThreshold(anomalyThreshold);
            secondaryStats.addScore(secondaryScore, window.getWindowEndMs());
            secondaryScores.put(groupName + "_secondary", secondaryStats);

            updateScoreHistory(state, groupName, secondaryScore);
            updateAdaptiveThreshold(state, groupName);

            double adaptiveThreshold = state.adaptiveThresholds.getOrDefault(groupName, anomalyThreshold);
            if (secondaryScore > adaptiveThreshold) {
                boolean shouldAlert = checkAlertSuppression(groupName, secondaryScore, adaptiveThreshold, ctx);
                if (shouldAlert) {
                    AnomalyAlert alert = AnomalyAlert.fromWindow(window, groupName, secondaryStats, adaptiveThreshold);
                    alert.setAlertType(AlertType.WINDOW_AGGREGATE);
                    alert.getMetadata().put("secondary_score", String.valueOf(secondaryScore));
                    alert.getMetadata().put("edge_score", String.valueOf(edgeStats.getMax()));
                    alert.getMetadata().put("adaptive_threshold", String.valueOf(adaptiveThreshold));
                    alert.getMetadata().put("score_improvement",
                            String.valueOf(secondaryScore - edgeStats.getMax()));

                    ctx.output(ALERT_OUTPUT_TAG, alert);
                    state.totalAlertsGenerated++;

                    logger.info("Alert generated: device={}, group={}, score={}, threshold={}",
                            window.getDeviceId(), groupName, secondaryScore, adaptiveThreshold);
                }
            }

            window.getMetadata().put(groupName + "_secondary_score", String.valueOf(secondaryScore));
            window.getMetadata().put(groupName + "_adaptive_threshold",
                    String.valueOf(state.adaptiveThresholds.getOrDefault(groupName, anomalyThreshold)));
        }

        window.getAggregatedAnomalyScores().putAll(secondaryScores);

        state.totalWindowsProcessed++;
        updateGlobalAnomalyRate(state, secondaryScores);

        window.getMetadata().put("cloud_scoring_end_ms", String.valueOf(System.currentTimeMillis()));
        window.getMetadata().put("total_windows_processed", String.valueOf(state.totalWindowsProcessed));
        window.getMetadata().put("total_alerts_generated", String.valueOf(state.totalAlertsGenerated));
        window.getMetadata().put("global_anomaly_rate", String.valueOf(state.globalAnomalyRate));

        scoringState.update(state);

        out.collect(window);
    }

    private void initializeDetectors(ScoringState state, AggregatedWindow window) {
        for (String groupName : measureGroups) {
            List<String> features = getFeaturesForGroup(groupName);
            int dimensions = features.size();

            int shingleSize = Integer.parseInt(parameter.get("CloudRcfShingleSize", "4"));
            boolean shingleCyclic = Boolean.parseBoolean(parameter.get("CloudRcfShingleCyclic", "true"));
            ShingleBuilder shingleBuilder = new ShingleBuilder(dimensions, shingleSize, shingleCyclic);

            int numberOfTrees = Integer.parseInt(parameter.get("CloudRcfNumberOfTrees", "100"));
            int sampleSize = Integer.parseInt(parameter.get("CloudRcfSampleSize", "256"));
            double lambda = Double.parseDouble(parameter.get("CloudRcfLambda", "0.0001"));
            int randomSeed = Integer.parseInt(parameter.get("CloudRcfRandomSeed", "42"));

            RandomCutForest forest = RandomCutForest.builder()
                    .numberOfTrees(numberOfTrees)
                    .sampleSize(sampleSize)
                    .dimensions(shingleBuilder.getShingledPointSize())
                    .lambda(lambda)
                    .randomSeed(randomSeed)
                    .build();

            Function<RandomCutForest, LineTransformer> algorithmInitializer = AnomalyScoreTransformer::new;
            LineTransformer algorithm = algorithmInitializer.apply(forest);

            state.forests.put(groupName, forest);
            state.shingleBuilders.put(groupName, shingleBuilder);
            state.pointBuffers.put(groupName, new double[dimensions]);
            state.shingleBuffers.put(groupName, new double[shingleBuilder.getShingledPointSize()]);
            state.algorithms.put(groupName, algorithm);
            state.scoreHistory.put(groupName, new LinkedList<>());
            state.adaptiveThresholds.put(groupName, anomalyThreshold);

            logger.debug("Initialized cloud detector for group: {}, dimensions: {}", groupName, dimensions);
        }
    }

    private List<String> getFeaturesForGroup(String groupName) {
        List<String> baseMeasures = new ArrayList<>();
        switch (groupName) {
            case "reactor_feed":
                for (int i = 23; i <= 28; i++) {
                    baseMeasures.add("xmeas_" + i);
                }
                break;
            case "purge_gas":
                for (int i = 29; i <= 36; i++) {
                    baseMeasures.add("xmeas_" + i);
                }
                break;
            case "product":
                for (int i = 37; i <= 41; i++) {
                    baseMeasures.add("xmeas_" + i);
                }
                break;
            default:
                break;
        }

        List<String> features = new ArrayList<>();
        for (String measure : baseMeasures) {
            features.add(measure + "_avg");
            features.add(measure + "_stddev");
            features.add(measure + "_trend");
        }
        features.add("edge_anomaly_score");
        features.add("message_count");
        features.add("time_since_last_anomaly");

        return features;
    }

    private double[] buildFeatureVector(AggregatedWindow window, String groupName) {
        List<String> features = getFeaturesForGroup(groupName);
        double[] vector = new double[features.size()];

        Map<String, AggregatedWindow.MeasurementStats> measurements = window.getAggregatedMeasurements();
        AnomalyScoreStats edgeStats = window.getAggregatedAnomalyScores().get(groupName);

        for (int i = 0; i < features.size(); i++) {
            String feature = features.get(i);

            if (feature.endsWith("_avg")) {
                String measureName = feature.substring(0, feature.length() - 4);
                AggregatedWindow.MeasurementStats stats = measurements.get(measureName);
                vector[i] = stats != null ? stats.getAvg() : 0.0;
            } else if (feature.endsWith("_stddev")) {
                String measureName = feature.substring(0, feature.length() - 7);
                AggregatedWindow.MeasurementStats stats = measurements.get(measureName);
                vector[i] = stats != null ? stats.getStdDev() : 0.0;
            } else if (feature.endsWith("_trend")) {
                String measureName = feature.substring(0, feature.length() - 6);
                AggregatedWindow.MeasurementStats stats = measurements.get(measureName);
                vector[i] = stats != null ? (stats.getLast() - stats.getFirst()) / stats.getCount() : 0.0;
            } else if (feature.equals("edge_anomaly_score")) {
                vector[i] = edgeStats != null ? edgeStats.getMax() : 0.0;
            } else if (feature.equals("message_count")) {
                vector[i] = window.getMessageCount();
            } else if (feature.equals("time_since_last_anomaly")) {
                vector[i] = calculateTimeSinceLastAnomaly(window, groupName);
            } else {
                vector[i] = 0.0;
            }
        }

        return vector;
    }

    private double calculateSecondaryScore(ScoringState state, String groupName, double[] featureVector) {
        ShingleBuilder shingleBuilder = state.shingleBuilders.get(groupName);
        double[] pointBuffer = state.pointBuffers.get(groupName);
        double[] shingleBuffer = state.shingleBuffers.get(groupName);
        LineTransformer algorithm = state.algorithms.get(groupName);

        if (featureVector.length != pointBuffer.length) {
            logger.warn("Feature vector length mismatch for group {}: expected {}, got {}",
                    groupName, pointBuffer.length, featureVector.length);
            return 0.0;
        }

        System.arraycopy(featureVector, 0, pointBuffer, 0, pointBuffer.length);
        shingleBuilder.addPoint(pointBuffer);

        if (shingleBuilder.isFull()) {
            shingleBuilder.getShingle(shingleBuffer);
            List<String> result = algorithm.getResultValues(shingleBuffer);
            if (result != null && !result.isEmpty() && !"NA".equals(result.get(0))) {
                try {
                    return Double.parseDouble(result.get(0));
                } catch (NumberFormatException e) {
                    logger.warn("Failed to parse secondary score: {}", result.get(0));
                }
            }
        }

        return 0.0;
    }

    private void updateScoreHistory(ScoringState state, String groupName, double score) {
        LinkedList<Double> history = state.scoreHistory.get(groupName);
        history.addLast(score);
        while (history.size() > 1000) {
            history.removeFirst();
        }
    }

    private void updateAdaptiveThreshold(ScoringState state, String groupName) {
        LinkedList<Double> history = state.scoreHistory.get(groupName);
        if (history.size() < 100) {
            return;
        }

        List<Double> sortedScores = new ArrayList<>(history);
        Collections.sort(sortedScores);

        int percentile99Index = (int) (sortedScores.size() * 0.99);
        double percentile99 = sortedScores.get(percentile99Index);

        double mean = sortedScores.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double variance = sortedScores.stream().mapToDouble(s -> Math.pow(s - mean, 2)).average().orElse(0.0);
        double stdDev = Math.sqrt(variance);

        double adaptiveThreshold = Math.max(anomalyThreshold, percentile99 * 0.9);
        state.adaptiveThresholds.put(groupName, adaptiveThreshold);
    }

    private double calculateTimeSinceLastAnomaly(AggregatedWindow window, String groupName) {
        try {
            String lastAnomalyTimeStr = window.getMetadata().get("last_anomaly_timestamp_ms");
            if (lastAnomalyTimeStr != null) {
                long lastAnomalyTime = Long.parseLong(lastAnomalyTimeStr);
                return window.getWindowEndMs() - lastAnomalyTime;
            }
        } catch (Exception e) {
            logger.debug("Failed to calculate time since last anomaly", e);
        }
        return Double.MAX_VALUE;
    }

    private boolean checkAlertSuppression(String groupName, double score, double threshold, Context ctx) throws Exception {
        Long lastAlert = lastAlertTimestamp.value();
        long now = ctx.timerService().currentProcessingTime();

        if (lastAlert != null) {
            long timeSinceLastAlert = now - lastAlert;
            double ratio = score / threshold;

            if (timeSinceLastAlert < 60000 && ratio < 3.0) {
                logger.debug("Alert suppressed for group {}, time since last: {}ms, ratio: {}",
                        groupName, timeSinceLastAlert, ratio);
                return false;
            }

            if (timeSinceLastAlert < 300000 && ratio < 1.5) {
                logger.debug("Alert suppressed for group {}, time since last: {}ms, ratio: {}",
                        groupName, timeSinceLastAlert, ratio);
                return false;
            }
        }

        lastAlertTimestamp.update(now);
        return true;
    }

    private void updateGlobalAnomalyRate(ScoringState state, Map<String, AnomalyScoreStats> scores) {
        long anomalyCount = scores.values().stream()
                .filter(s -> s.getMax() > anomalyThreshold)
                .count();
        double currentRate = (double) anomalyCount / scores.size();
        state.globalAnomalyRate = (state.globalAnomalyRate * (state.totalWindowsProcessed - 1) + currentRate)
                / state.totalWindowsProcessed;
    }
}
