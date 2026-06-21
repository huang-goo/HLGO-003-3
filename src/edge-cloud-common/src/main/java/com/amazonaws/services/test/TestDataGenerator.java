/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.test;

import com.amazonaws.services.common.model.AnomalyMessage;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TestDataGenerator {

    private static final Logger logger = LoggerFactory.getLogger(TestDataGenerator.class);
    private static final Gson gson = new Gson();
    private static final Random random = new Random(42);

    private final String deviceId;
    private long currentSequence;
    private long baseTimestamp;

    private final int numMeasures;
    private final double[] baseValues;
    private final double[] normalRanges;

    public TestDataGenerator(String deviceId) {
        this(deviceId, 41);
    }

    public TestDataGenerator(String deviceId, int numMeasures) {
        this.deviceId = deviceId;
        this.numMeasures = numMeasures;
        this.currentSequence = 0;
        this.baseTimestamp = System.currentTimeMillis() - 3600000;
        this.baseValues = new double[numMeasures];
        this.normalRanges = new double[numMeasures];

        for (int i = 0; i < numMeasures; i++) {
            baseValues[i] = 10 + random.nextDouble() * 90;
            normalRanges[i] = 1 + random.nextDouble() * 5;
        }
    }

    public AnomalyMessage generateNormalMessage() {
        return generateMessage(false);
    }

    public AnomalyMessage generateAnomalyMessage() {
        return generateMessage(true);
    }

    public AnomalyMessage generateMessage(boolean anomaly) {
        AnomalyMessage message = new AnomalyMessage();
        message.setDeviceId(deviceId);
        message.setSequenceNumber(currentSequence++);
        message.setEdgeTimestampMs(baseTimestamp + currentSequence * 100);

        Map<String, Double> measurements = new HashMap<>();
        for (int i = 1; i <= numMeasures; i++) {
            String measureName = "xmeas_" + i;
            double value;
            if (anomaly && random.nextDouble() < 0.3) {
                value = baseValues[i - 1] + normalRanges[i - 1] * (3 + random.nextDouble() * 5);
            } else {
                value = baseValues[i - 1] + (random.nextDouble() - 0.5) * normalRanges[i - 1];
            }
            measurements.put(measureName, value);
        }
        message.setMeasurements(measurements);

        return message;
    }

    public List<AnomalyMessage> generateNormalBatch(int count) {
        List<AnomalyMessage> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            messages.add(generateNormalMessage());
        }
        return messages;
    }

    public List<AnomalyMessage> generateMixedBatch(int count, double anomalyRatio) {
        List<AnomalyMessage> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            if (random.nextDouble() < anomalyRatio) {
                messages.add(generateAnomalyMessage());
            } else {
                messages.add(generateNormalMessage());
            }
        }
        return messages;
    }

    public AnomalyMessage generateDuplicate(AnomalyMessage original) {
        AnomalyMessage duplicate = new AnomalyMessage();
        duplicate.setDeviceId(original.getDeviceId());
        duplicate.setSequenceNumber(original.getSequenceNumber());
        duplicate.setEdgeTimestampMs(original.getEdgeTimestampMs());
        duplicate.setMeasurements(new HashMap<>(original.getMeasurements()));
        duplicate.setIdempotencyKey(original.getIdempotencyKey());
        return duplicate;
    }

    public AnomalyMessage generateOutOfOrder(AnomalyMessage message, long timeOffsetMs) {
        AnomalyMessage outOfOrder = new AnomalyMessage();
        outOfOrder.setDeviceId(message.getDeviceId());
        outOfOrder.setSequenceNumber(message.getSequenceNumber());
        outOfOrder.setEdgeTimestampMs(message.getEdgeTimestampMs() - timeOffsetMs);
        outOfOrder.setMeasurements(new HashMap<>(message.getMeasurements()));
        return outOfOrder;
    }

    public AnomalyMessage generateWithClockDrift(AnomalyMessage message, long driftMs) {
        AnomalyMessage drifted = new AnomalyMessage();
        drifted.setDeviceId(message.getDeviceId());
        drifted.setSequenceNumber(message.getSequenceNumber());
        drifted.setEdgeTimestampMs(message.getEdgeTimestampMs() + driftMs);
        drifted.setMeasurements(new HashMap<>(message.getMeasurements()));
        return drifted;
    }

    public String toJson(AnomalyMessage message) {
        return gson.toJson(message);
    }

    public AnomalyMessage fromJson(String json) {
        return gson.fromJson(json, AnomalyMessage.class);
    }

    public void reset() {
        currentSequence = 0;
        baseTimestamp = System.currentTimeMillis() - 3600000;
    }

    public void setBaseTimestamp(long timestamp) {
        this.baseTimestamp = timestamp;
    }

    public long getCurrentSequence() {
        return currentSequence;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public static void main(String[] args) {
        TestDataGenerator generator = new TestDataGenerator("test-device-001");

        System.out.println("Generating test data...");
        System.out.println("========================");

        AnomalyMessage normal = generator.generateNormalMessage();
        System.out.println("Normal message: " + normal.toJson());

        AnomalyMessage anomaly = generator.generateAnomalyMessage();
        System.out.println("Anomaly message: " + anomaly.toJson());

        System.out.println("\nGenerating batch of 5 normal messages:");
        List<AnomalyMessage> batch = generator.generateNormalBatch(5);
        for (int i = 0; i < batch.size(); i++) {
            System.out.println(i + ": " + batch.get(i).toJson());
        }

        System.out.println("\nGenerating duplicate message:");
        AnomalyMessage duplicate = generator.generateDuplicate(normal);
        System.out.println("Original: " + normal.getIdempotencyKey());
        System.out.println("Duplicate: " + duplicate.getIdempotencyKey());
        System.out.println("Are duplicates: " + normal.getIdempotencyKey().equals(duplicate.getIdempotencyKey()));
    }
}
