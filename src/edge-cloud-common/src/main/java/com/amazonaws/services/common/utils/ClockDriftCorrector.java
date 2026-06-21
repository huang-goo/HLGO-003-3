/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class ClockDriftCorrector implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(ClockDriftCorrector.class);

    private static final int DEFAULT_WINDOW_SIZE = 100;
    private static final long DEFAULT_MAX_ACCEPTABLE_DRIFT_MS = 5000;
    private static final long DEFAULT_SYNCHRONIZATION_INTERVAL_MS = 60000;

    private final int windowSize;
    private final long maxAcceptableDriftMs;
    private final long synchronizationIntervalMs;

    private final Queue<OffsetSample> offsetSamples;
    private long currentOffsetMs;
    private long lastSynchronizationTimestampMs;
    private long lastEdgeTimestampMs;
    private boolean isInitialized;
    private int totalSamples;
    private int driftExceededCount;

    public static class OffsetSample implements Serializable {
        private static final long serialVersionUID = 1L;
        long edgeTimestampMs;
        long cloudTimestampMs;
        long calculatedOffsetMs;
        long networkLatencyMs;

        public OffsetSample(long edgeTimestampMs, long cloudTimestampMs, long networkLatencyMs) {
            this.edgeTimestampMs = edgeTimestampMs;
            this.cloudTimestampMs = cloudTimestampMs;
            this.networkLatencyMs = networkLatencyMs;
            this.calculatedOffsetMs = cloudTimestampMs - edgeTimestampMs - networkLatencyMs / 2;
        }

        public long getCalculatedOffsetMs() {
            return calculatedOffsetMs;
        }

        public long getEdgeTimestampMs() {
            return edgeTimestampMs;
        }

        public long getCloudTimestampMs() {
            return cloudTimestampMs;
        }

        public long getNetworkLatencyMs() {
            return networkLatencyMs;
        }
    }

    public ClockDriftCorrector() {
        this(DEFAULT_WINDOW_SIZE, DEFAULT_MAX_ACCEPTABLE_DRIFT_MS, DEFAULT_SYNCHRONIZATION_INTERVAL_MS);
    }

    public ClockDriftCorrector(int windowSize, long maxAcceptableDriftMs, long synchronizationIntervalMs) {
        this.windowSize = windowSize;
        this.maxAcceptableDriftMs = maxAcceptableDriftMs;
        this.synchronizationIntervalMs = synchronizationIntervalMs;
        this.offsetSamples = new LinkedList<>();
        this.currentOffsetMs = 0;
        this.lastSynchronizationTimestampMs = 0;
        this.lastEdgeTimestampMs = 0;
        this.isInitialized = false;
        this.totalSamples = 0;
        this.driftExceededCount = 0;
    }

    public synchronized void recordOffsetSample(long edgeTimestampMs, long cloudTimestampMs, long networkLatencyMs) {
        OffsetSample sample = new OffsetSample(edgeTimestampMs, cloudTimestampMs, networkLatencyMs);
        offsetSamples.offer(sample);
        totalSamples++;

        if (offsetSamples.size() > windowSize) {
            offsetSamples.poll();
        }

        if (offsetSamples.size() >= Math.min(10, windowSize / 10)) {
            recalculateOffset();
            isInitialized = true;
        }

        lastEdgeTimestampMs = edgeTimestampMs;
        lastSynchronizationTimestampMs = cloudTimestampMs;

        if (Math.abs(sample.getCalculatedOffsetMs() - currentOffsetMs) > maxAcceptableDriftMs) {
            driftExceededCount++;
            logger.warn("Clock drift exceeded acceptable threshold: drift={}ms, max={}ms",
                    sample.getCalculatedOffsetMs() - currentOffsetMs, maxAcceptableDriftMs);
        }
    }

    private void recalculateOffset() {
        List<Long> offsets = new ArrayList<>();
        for (OffsetSample sample : offsetSamples) {
            offsets.add(sample.getCalculatedOffsetMs());
        }

        long medianOffset = calculateMedian(offsets);
        double meanOffset = calculateMean(offsets);
        double stdDev = calculateStdDev(offsets, meanOffset);

        List<Long> filteredOffsets = new ArrayList<>();
        for (long offset : offsets) {
            if (Math.abs(offset - medianOffset) <= 2 * stdDev) {
                filteredOffsets.add(offset);
            }
        }

        if (!filteredOffsets.isEmpty()) {
            currentOffsetMs = (long) calculateMean(filteredOffsets);
        }

        logger.debug("Clock offset recalculated: offset={}ms, samples={}, filteredSamples={}",
                currentOffsetMs, offsets.size(), filteredOffsets.size());
    }

    private long calculateMedian(List<Long> values) {
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(Long::compareTo);
        int middle = sorted.size() / 2;
        if (sorted.size() % 2 == 0) {
            return (sorted.get(middle - 1) + sorted.get(middle)) / 2;
        } else {
            return sorted.get(middle);
        }
    }

    private double calculateMean(List<Long> values) {
        long sum = 0;
        for (long value : values) {
            sum += value;
        }
        return (double) sum / values.size();
    }

    private double calculateStdDev(List<Long> values, double mean) {
        double sumSquaredDiff = 0;
        for (long value : values) {
            sumSquaredDiff += Math.pow(value - mean, 2);
        }
        return Math.sqrt(sumSquaredDiff / values.size());
    }

    public synchronized long correctTimestamp(long edgeTimestampMs) {
        if (!isInitialized) {
            return edgeTimestampMs;
        }
        return edgeTimestampMs + currentOffsetMs;
    }

    public synchronized boolean needsSynchronization(long currentCloudTimestampMs) {
        return !isInitialized ||
                (currentCloudTimestampMs - lastSynchronizationTimestampMs) > synchronizationIntervalMs;
    }

    public synchronized boolean isDriftExcessive(long edgeTimestampMs) {
        if (!isInitialized) {
            return false;
        }
        long expectedCloudTime = edgeTimestampMs + currentOffsetMs;
        long actualCloudTime = System.currentTimeMillis();
        return Math.abs(actualCloudTime - expectedCloudTime) > maxAcceptableDriftMs;
    }

    public synchronized long getCurrentOffsetMs() {
        return currentOffsetMs;
    }

    public synchronized boolean isInitialized() {
        return isInitialized;
    }

    public synchronized long getLastSynchronizationTimestampMs() {
        return lastSynchronizationTimestampMs;
    }

    public synchronized int getTotalSamples() {
        return totalSamples;
    }

    public synchronized int getDriftExceededCount() {
        return driftExceededCount;
    }

    public synchronized int getCurrentWindowSize() {
        return offsetSamples.size();
    }

    public synchronized double getOffsetStdDev() {
        if (offsetSamples.size() < 2) {
            return 0.0;
        }
        List<Long> offsets = new ArrayList<>();
        for (OffsetSample sample : offsetSamples) {
            offsets.add(sample.getCalculatedOffsetMs());
        }
        double mean = calculateMean(offsets);
        return calculateStdDev(offsets, mean);
    }

    public synchronized void reset() {
        offsetSamples.clear();
        currentOffsetMs = 0;
        isInitialized = false;
        totalSamples = 0;
        driftExceededCount = 0;
        logger.info("ClockDriftCorrector reset");
    }

    @Override
    public String toString() {
        return "ClockDriftCorrector{" +
                "currentOffsetMs=" + currentOffsetMs +
                ", isInitialized=" + isInitialized +
                ", totalSamples=" + totalSamples +
                ", driftExceededCount=" + driftExceededCount +
                ", windowSize=" + offsetSamples.size() +
                '}';
    }
}
