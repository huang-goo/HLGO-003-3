/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.edge.operators;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.runner.LineTransformer;
import com.amazon.randomcutforest.util.ShingleBuilder;
import com.amazonaws.services.common.model.AnomalyMessage;
import com.amazonaws.services.common.model.AnomalyMessage.ProcessingStage;
import com.amazonaws.services.kinesisanalytics.operators.AnomalyDetector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

public class EdgeAnomalyDetectionFunction extends KeyedProcessFunction<String, AnomalyMessage, AnomalyMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(EdgeAnomalyDetectionFunction.class);

    private final ParameterTool parameter;
    private final List<String> measureGroups;

    private transient ValueState<DetectorState> detectorState;

    public static class DetectorState implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        Map<String, AnomalyDetector> detectors;
        Map<String, ShingleBuilder> shingleBuilders;
        Map<String, double[]> pointBuffers;
        Map<String, double[]> shingleBuffers;
        Map<String, Double> lastScores;
        long lastProcessedTimestamp;
        int processedCount;

        public DetectorState() {
            this.detectors = new HashMap<>();
            this.shingleBuilders = new HashMap<>();
            this.pointBuffers = new HashMap<>();
            this.shingleBuffers = new HashMap<>();
            this.lastScores = new HashMap<>();
            this.processedCount = 0;
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

    public EdgeAnomalyDetectionFunction(ParameterTool parameter, List<String> measureGroups) {
        this.parameter = parameter;
        this.measureGroups = measureGroups;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<DetectorState> stateDescriptor = new ValueStateDescriptor<>(
                "anomalyDetectorState", DetectorState.class);
        detectorState = getRuntimeContext().getState(stateDescriptor);

        logger.info("EdgeAnomalyDetectionFunction initialized with {} measure groups", measureGroups.size());
    }

    @Override
    public void processElement(AnomalyMessage message, Context ctx, Collector<AnomalyMessage> out) throws Exception {
        if (message.isDuplicate()) {
            out.collect(message);
            return;
        }

        DetectorState state = detectorState.value();
        if (state == null) {
            state = new DetectorState();
            initializeDetectors(state, message);
            detectorState.update(state);
        }

        for (String groupName : measureGroups) {
            List<String> measureNames = getMeasuresForGroup(groupName);
            Double[] values = extractMeasureValues(message, measureNames);

            if (values != null && values.length > 0) {
                double score = processMeasurements(state, groupName, values, measureNames.size());
                message.addAnomalyScore(groupName, score);
                state.lastScores.put(groupName, score);
            }
        }

        state.processedCount++;
        state.lastProcessedTimestamp = message.getEdgeTimestampMs();
        message.setProcessingStage(ProcessingStage.EDGE_PREPROCESSED);

        out.collect(message);
    }

    private void initializeDetectors(DetectorState state, AnomalyMessage message) {
        for (String groupName : measureGroups) {
            List<String> measureNames = getMeasuresForGroup(groupName);
            int dimensions = measureNames.size();

            Function<RandomCutForest, LineTransformer> algorithmInitializer = AnomalyScoreTransformer::new;
            AnomalyDetector detector = new AnomalyDetector(parameter, algorithmInitializer);

            int shingleSize = Integer.parseInt(parameter.get("RcfShingleSize", "1"));
            boolean shingleCyclic = Boolean.parseBoolean(parameter.get("RcfShingleCyclic", "false"));
            ShingleBuilder shingleBuilder = new ShingleBuilder(dimensions, shingleSize, shingleCyclic);

            state.detectors.put(groupName, detector);
            state.shingleBuilders.put(groupName, shingleBuilder);
            state.pointBuffers.put(groupName, new double[dimensions]);
            state.shingleBuffers.put(groupName, new double[shingleBuilder.getShingledPointSize()]);

            logger.debug("Initialized detector for group: {}, dimensions: {}", groupName, dimensions);
        }
    }

    private double processMeasurements(DetectorState state, String groupName, Double[] values, int dimensions) {
        AnomalyDetector detector = state.detectors.get(groupName);
        ShingleBuilder shingleBuilder = state.shingleBuilders.get(groupName);
        double[] pointBuffer = state.pointBuffers.get(groupName);
        double[] shingleBuffer = state.shingleBuffers.get(groupName);

        if (values.length != pointBuffer.length) {
            logger.warn("Wrong number of values for group {}. Expected {}, found {}",
                    groupName, pointBuffer.length, values.length);
            return 0.0;
        }

        for (int i = 0; i < pointBuffer.length; i++) {
            pointBuffer[i] = values[i];
        }

        shingleBuilder.addPoint(pointBuffer);

        if (shingleBuilder.isFull()) {
            shingleBuilder.getShingle(shingleBuffer);

            prepareAlgorithmIfNeeded(detector, shingleBuilder.getShingledPointSize());

            List<String> result = detector.getAlgorithm().getResultValues(shingleBuffer);
            if (result != null && !result.isEmpty() && !"NA".equals(result.get(0))) {
                try {
                    return Double.parseDouble(result.get(0));
                } catch (NumberFormatException e) {
                    logger.warn("Failed to parse anomaly score: {}", result.get(0));
                }
            }
        }

        return 0.0;
    }

    private void prepareAlgorithmIfNeeded(AnomalyDetector detector, int shingledPointSize) {
        if (detector.getAlgorithm() == null) {
            RandomCutForest forest = RandomCutForest.builder()
                    .numberOfTrees(Integer.parseInt(parameter.get("RcfNumberOfTrees", "50")))
                    .sampleSize(Integer.parseInt(parameter.get("RcfSampleSize", "128")))
                    .dimensions(shingledPointSize)
                    .lambda(Double.parseDouble(parameter.get("RcfLambda", "0.00078125")))
                    .randomSeed(Integer.parseInt(parameter.get("RcfRandomSeed", "42")))
                    .build();

            Function<RandomCutForest, LineTransformer> algorithmInitializer = AnomalyScoreTransformer::new;
            detector.setAlgorithm(algorithmInitializer.apply(forest));
        }
    }

    private List<String> getMeasuresForGroup(String groupName) {
        List<String> measures = new ArrayList<>();
        for (int i = 1; i <= 41; i++) {
            String measureName = "xmeas_" + i;
            if (isMeasureInGroup(i, groupName)) {
                measures.add(measureName);
            }
        }
        return measures;
    }

    private boolean isMeasureInGroup(int measureIndex, String groupName) {
        switch (groupName) {
            case "reactor_feed":
                return measureIndex >= 23 && measureIndex <= 28;
            case "purge_gas":
                return measureIndex >= 29 && measureIndex <= 36;
            case "product":
                return measureIndex >= 37 && measureIndex <= 41;
            default:
                return false;
        }
    }

    private Double[] extractMeasureValues(AnomalyMessage message, List<String> measureNames) {
        Map<String, Double> measurements = message.getMeasurements();
        if (measurements == null) {
            return null;
        }

        Double[] values = new Double[measureNames.size()];
        int validCount = 0;
        for (int i = 0; i < measureNames.size(); i++) {
            Double value = measurements.get(measureNames.get(i));
            values[i] = value != null ? value : 0.0;
            if (value != null) {
                validCount++;
            }
        }

        if (validCount == 0) {
            return null;
        }

        return values;
    }
}
