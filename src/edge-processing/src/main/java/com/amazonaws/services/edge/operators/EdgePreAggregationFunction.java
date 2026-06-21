/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.edge.operators;

import com.amazonaws.services.common.model.AggregatedWindow;
import com.amazonaws.services.common.model.AnomalyMessage;
import com.amazonaws.services.common.model.AnomalyMessage.ProcessingStage;
import com.amazonaws.services.common.utils.BloomFilter;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class EdgePreAggregationFunction extends RichWindowFunction<AnomalyMessage, AggregatedWindow, String, TimeWindow> {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(EdgePreAggregationFunction.class);

    private static final int BLOOM_FILTER_EXPECTED_INSERTIONS = 100000;
    private static final double BLOOM_FILTER_FPP = 0.01;

    private final long windowSizeMs;
    private final long slideSizeMs;
    private final double dedupTimeToleranceMs;

    private transient MapState<String, Long> sequenceNumberState;
    private transient ValueState<BloomFilter> bloomFilterState;
    private transient ValueState<Long> lastWindowEndState;

    public EdgePreAggregationFunction(long windowSizeMs, long slideSizeMs) {
        this(windowSizeMs, slideSizeMs, 1000);
    }

    public EdgePreAggregationFunction(long windowSizeMs, long slideSizeMs, double dedupTimeToleranceMs) {
        this.windowSizeMs = windowSizeMs;
        this.slideSizeMs = slideSizeMs;
        this.dedupTimeToleranceMs = dedupTimeToleranceMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        MapStateDescriptor<String, Long> sequenceDescriptor = new MapStateDescriptor<>(
                "sequenceNumbers", String.class, Long.class);
        sequenceNumberState = getRuntimeContext().getMapState(sequenceDescriptor);

        ValueStateDescriptor<BloomFilter> bloomFilterDescriptor = new ValueStateDescriptor<>(
                "bloomFilter", BloomFilter.class);
        bloomFilterState = getRuntimeContext().getState(bloomFilterDescriptor);

        ValueStateDescriptor<Long> lastWindowDescriptor = new ValueStateDescriptor<>(
                "lastWindowEnd", Long.class);
        lastWindowEndState = getRuntimeContext().getState(lastWindowDescriptor);

        logger.info("EdgePreAggregationFunction initialized: windowSize={}ms, slideSize={}ms",
                windowSizeMs, slideSizeMs);
    }

    @Override
    public void apply(String deviceId, TimeWindow window, Iterable<AnomalyMessage> messages,
                      Collector<AggregatedWindow> out) throws Exception {

        BloomFilter bloomFilter = bloomFilterState.value();
        if (bloomFilter == null) {
            bloomFilter = new BloomFilter(BLOOM_FILTER_EXPECTED_INSERTIONS, BLOOM_FILTER_FPP);
            bloomFilterState.update(bloomFilter);
        }

        if (bloomFilter.isNearCapacity()) {
            logger.info("BloomFilter near capacity, rotating for device: {}", deviceId);
            bloomFilter = new BloomFilter(BLOOM_FILTER_EXPECTED_INSERTIONS, BLOOM_FILTER_FPP);
            bloomFilterState.update(bloomFilter);
        }

        AggregatedWindow aggregatedWindow = new AggregatedWindow(
                deviceId, window.getStart(), window.getEnd());

        List<AnomalyMessage> sortedMessages = new ArrayList<>();
        for (AnomalyMessage msg : messages) {
            sortedMessages.add(msg);
        }
        sortedMessages.sort(Comparator.comparingLong(AnomalyMessage::getEdgeTimestampMs));

        int duplicateCount = 0;
        int outOfOrderCount = 0;
        long lastTimestamp = 0;

        for (AnomalyMessage message : sortedMessages) {
            if (message.getEdgeTimestampMs() <= lastTimestamp) {
                outOfOrderCount++;
            }
            lastTimestamp = message.getEdgeTimestampMs();

            String dedupKey = message.generateDeduplicationKey();

            if (bloomFilter.mightContain(dedupKey)) {
                message.setDuplicate(true);
                message.setProcessingStage(ProcessingStage.EDGE_DEDUPED);
                message.addMetadata("dedup_reason", "bloom_filter_hit");
                duplicateCount++;
                continue;
            }

            if (isDuplicateByTimeAndContent(message, dedupTimeToleranceMs)) {
                message.setDuplicate(true);
                message.setProcessingStage(ProcessingStage.EDGE_DEDUPED);
                message.addMetadata("dedup_reason", "time_window_duplicate");
                duplicateCount++;
                continue;
            }

            if (!validateSequenceNumber(message)) {
                message.addMetadata("seq_gap_detected", "true");
                outOfOrderCount++;
            }

            bloomFilter.put(dedupKey);
            aggregatedWindow.addMessage(message);

            message.setProcessingStage(ProcessingStage.EDGE_AGGREGATED);
            message.addMetadata("window_id", aggregatedWindow.getWindowId());
        }

        if (bloomFilter.isNearCapacity()) {
            bloomFilterState.update(bloomFilter);
        }

        if (aggregatedWindow.getMessageCount() > 0) {
            aggregatedWindow.getMetadata().put("duplicate_count", String.valueOf(duplicateCount));
            aggregatedWindow.getMetadata().put("out_of_order_count", String.valueOf(outOfOrderCount));
            aggregatedWindow.getMetadata().put("bloom_filter_insertions",
                    String.valueOf(bloomFilter.getInsertions()));

            if (duplicateCount > 0 || outOfOrderCount > 0) {
                aggregatedWindow.setSuspectWindow(true);
            }

            out.collect(aggregatedWindow);

            lastWindowEndState.update(window.getEnd());

            logger.debug("Aggregated window: device={}, window={}, messages={}, duplicates={}, outOfOrder={}",
                    deviceId, window, aggregatedWindow.getMessageCount(), duplicateCount, outOfOrderCount);
        } else {
            logger.debug("Empty window after deduplication: device={}, window={}", deviceId, window);
        }
    }

    private boolean isDuplicateByTimeAndContent(AnomalyMessage message, double toleranceMs) throws Exception {
        String dedupKey = message.generateDeduplicationKey();
        String contentHash = String.valueOf(dedupKey.hashCode());

        Long lastSeenTime = sequenceNumberState.get("content_" + contentHash);
        if (lastSeenTime != null) {
            if (Math.abs(message.getEdgeTimestampMs() - lastSeenTime) <= toleranceMs) {
                return true;
            }
        }
        sequenceNumberState.put("content_" + contentHash, message.getEdgeTimestampMs());
        return false;
    }

    private boolean validateSequenceNumber(AnomalyMessage message) throws Exception {
        String key = "seq_" + message.getDeviceId();
        Long lastSequence = sequenceNumberState.get(key);
        long currentSequence = message.getSequenceNumber();

        if (lastSequence != null) {
            long gap = currentSequence - lastSequence;
            if (gap <= 0) {
                return false;
            }
            if (gap > 1) {
                logger.warn("Sequence gap detected: device={}, last={}, current={}, gap={}",
                        message.getDeviceId(), lastSequence, currentSequence, gap);
            }
        }

        sequenceNumberState.put(key, currentSequence);
        return true;
    }
}
