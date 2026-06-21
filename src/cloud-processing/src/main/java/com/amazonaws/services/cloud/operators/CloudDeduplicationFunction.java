/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.cloud.operators;

import com.amazonaws.services.common.model.AggregatedWindow;
import com.amazonaws.services.common.model.AnomalyMessage;
import com.amazonaws.services.common.model.AnomalyMessage.ProcessingStage;
import com.amazonaws.services.common.utils.BloomFilter;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class CloudDeduplicationFunction extends KeyedProcessFunction<String, String, AggregatedWindow> {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(CloudDeduplicationFunction.class);

    private static final int BLOOM_FILTER_EXPECTED_INSERTIONS = 1000000;
    private static final double BLOOM_FILTER_FPP = 0.0001;
    private static final long IDEMPOTENCY_TTL_MS = 24 * 60 * 60 * 1000;

    private transient ValueState<BloomFilter> bloomFilterState;
    private transient MapState<String, Long> idempotencyState;
    private transient ValueState<Long> lastWindowEndState;
    private transient ListState<String> pendingMessagesState;

    private final Gson gson;

    public CloudDeduplicationFunction() {
        this.gson = new Gson();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<BloomFilter> bloomFilterDescriptor = new ValueStateDescriptor<>(
                "cloudBloomFilter", BloomFilter.class);
        bloomFilterState = getRuntimeContext().getState(bloomFilterDescriptor);

        MapStateDescriptor<String, Long> idempotencyDescriptor = new MapStateDescriptor<>(
                "idempotencyKeys", String.class, Long.class);
        idempotencyState = getRuntimeContext().getMapState(idempotencyDescriptor);

        ValueStateDescriptor<Long> lastWindowDescriptor = new ValueStateDescriptor<>(
                "lastCloudWindowEnd", Long.class);
        lastWindowEndState = getRuntimeContext().getState(lastWindowDescriptor);

        ListStateDescriptor<String> pendingDescriptor = new ListStateDescriptor<>(
                "pendingMessages", String.class);
        pendingMessagesState = getRuntimeContext().getListState(pendingDescriptor);

        logger.info("CloudDeduplicationFunction initialized");
    }

    @Override
    public void processElement(String payload, Context ctx, Collector<AggregatedWindow> out) throws Exception {
        BloomFilter bloomFilter = bloomFilterState.value();
        if (bloomFilter == null) {
            bloomFilter = new BloomFilter(BLOOM_FILTER_EXPECTED_INSERTIONS, BLOOM_FILTER_FPP);
            bloomFilterState.update(bloomFilter);
        }

        if (bloomFilter.isNearCapacity()) {
            logger.info("Cloud BloomFilter near capacity, rotating");
            bloomFilter = new BloomFilter(BLOOM_FILTER_EXPECTED_INSERTIONS, BLOOM_FILTER_FPP);
            bloomFilterState.update(bloomFilter);
        }

        AggregatedWindow window = parseWindowPayload(payload);

        if (window == null) {
            logger.warn("Failed to parse payload, skipping");
            return;
        }

        String dedupKey = generateDeduplicationKey(window);

        if (bloomFilter.mightContain(dedupKey)) {
            logger.debug("Duplicate window detected by BloomFilter: {}", window.getWindowId());
            return;
        }

        for (String idempotencyKey : window.getIdempotencyKeys()) {
            if (idempotencyState.contains(idempotencyKey)) {
                logger.debug("Duplicate message detected by idempotency key: {}", idempotencyKey);
                return;
            }
        }

        String transactionId = window.getMetadata() != null ?
                window.getMetadata().get("transaction_id") : null;
        if (transactionId != null && idempotencyState.contains("tx_" + transactionId)) {
            logger.debug("Duplicate transaction detected: {}", transactionId);
            return;
        }

        bloomFilter.put(dedupKey);
        for (String idempotencyKey : window.getIdempotencyKeys()) {
            idempotencyState.put(idempotencyKey, System.currentTimeMillis());
        }
        if (transactionId != null) {
            idempotencyState.put("tx_" + transactionId, System.currentTimeMillis());
        }

        window.getMetadata().put("cloud_received_timestamp_ms", String.valueOf(System.currentTimeMillis()));
        window.getMetadata().put("cloud_dedup_passed", "true");

        Long lastWindowEnd = lastWindowEndState.value();
        if (lastWindowEnd != null && window.getWindowStartMs() < lastWindowEnd) {
            window.getMetadata().put("out_of_order_window", "true");
            logger.warn("Out-of-order window detected: current={}, last={}",
                    window.getWindowEndMs(), lastWindowEnd);
        }

        if (window.getWindowEndMs() > (lastWindowEnd != null ? lastWindowEnd : 0)) {
            lastWindowEndState.update(window.getWindowEndMs());
        }

        if (bloomFilter.isNearCapacity()) {
            bloomFilterState.update(bloomFilter);
        }

        out.collect(window);

        logger.debug("Window passed cloud deduplication: {}, messages: {}",
                window.getWindowId(), window.getMessageCount());
    }

    private AggregatedWindow parseWindowPayload(String payload) {
        try {
            JsonObject jsonObject = JsonParser.parseString(payload).getAsJsonObject();

            if (jsonObject.has("window_id")) {
                return gson.fromJson(jsonObject, AggregatedWindow.class);
            }

            return parseLegacyFormat(payload);
        } catch (Exception e) {
            logger.warn("Failed to parse window payload", e);
            return parseLegacyFormat(payload);
        }
    }

    private AggregatedWindow parseLegacyFormat(String payload) {
        try {
            AnomalyMessage message = AnomalyMessage.fromJson(payload);
            if (message != null && message.getDeviceId() != null) {
                long windowSize = 60000;
                long windowStart = (message.getEdgeTimestampMs() / windowSize) * windowSize;
                long windowEnd = windowStart + windowSize;

                AggregatedWindow window = new AggregatedWindow(message.getDeviceId(), windowStart, windowEnd);
                window.addMessage(message);
                return window;
            }
        } catch (Exception e) {
            logger.debug("Failed to parse as AnomalyMessage", e);
        }
        return null;
    }

    private String generateDeduplicationKey(AggregatedWindow window) {
        StringBuilder sb = new StringBuilder();
        sb.append(window.getDeviceId()).append("|");
        sb.append(window.getWindowStartMs()).append("|");
        sb.append(window.getWindowEndMs()).append("|");
        sb.append(window.getMessageCount()).append("|");

        List<String> sortedIds = new ArrayList<>(window.getMessageIds());
        Collections.sort(sortedIds);
        for (String id : sortedIds) {
            sb.append(id).append(",");
        }

        return sb.toString();
    }
}
