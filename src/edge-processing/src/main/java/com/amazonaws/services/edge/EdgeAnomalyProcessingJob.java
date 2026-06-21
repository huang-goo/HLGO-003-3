/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.edge;

import com.amazonaws.services.common.model.AggregatedWindow;
import com.amazonaws.services.common.model.AnomalyMessage;
import com.amazonaws.services.edge.operators.EdgeAnomalyDetectionFunction;
import com.amazonaws.services.edge.operators.EdgePreAggregationFunction;
import com.amazonaws.services.edge.streammanager.EnhancedStreamManagerSink;
import com.amazonaws.services.edge.streammanager.EnhancedStreamManagerSource;
import com.amazonaws.services.kinesisanalytics.utils.ParameterToolUtils;
import com.amazonaws.services.timestream.TimestreamPoint;
import com.amazonaws.services.timestream.TimestreamSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class EdgeAnomalyProcessingJob {

    private static final Logger logger = LoggerFactory.getLogger(EdgeAnomalyProcessingJob.class);

    private static final List<String> MEASURE_GROUPS = Arrays.asList(
            "reactor_feed",
            "purge_gas",
            "product"
    );

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        configureEnvironment(env);

        ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);
        env.getConfig().setGlobalJobParameters(parameter);

        logParameters(parameter);

        String ggSourceStreamName = parameter.get("ggSourceStreamName", "TesimSourceStream");
        String ggTargetStreamName = parameter.get("ggTargetStreamName", "EdgeProcessedStream");
        String ggStreamHost = parameter.get("ggStreamHost", "localhost");
        String ggStreamPort = parameter.get("ggStreamPort", "8089");
        String region = parameter.get("region", "eu-central-1");
        String kinesisExportStreamName = parameter.get("kinesisExportStreamName", "tep-ingest-greengrass");
        String offlineStoragePath = parameter.get("offlineStoragePath", "/tmp/edge-offline-storage");
        int ggStreamBatchSize = Integer.parseInt(parameter.get("ggStreamBatchSize", "50"));
        long windowSizeMs = Long.parseLong(parameter.get("windowSizeMs", "60000"));
        long slideSizeMs = Long.parseLong(parameter.get("slideSizeMs", "60000"));

        String timestreamRegion = parameter.get("TimeStreamRegion", "eu-central-1");
        String timestreamDbName = parameter.get("TimeStreamDbName", "kdaflink");
        String timestreamTableName = parameter.get("TimeStreamTableName", "edge_anomalies");
        int timestreamBatchSize = Integer.parseInt(parameter.get("TimeStreamIngestBatchSize", "50"));

        boolean enableTimestream = Boolean.parseBoolean(parameter.get("enableTimestream", "false"));

        DataStream<AnomalyMessage> sourceStream = env.addSource(
                        new EnhancedStreamManagerSource(
                                ggSourceStreamName,
                                ggStreamHost,
                                ggStreamPort,
                                "tep-device-001"
                        ))
                .name("EnhancedStreamManagerSource")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AnomalyMessage>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((message, timestamp) -> message.getEdgeTimestampMs())
                );

        SingleOutputStreamOperator<AnomalyMessage> detectedStream = sourceStream
                .keyBy(AnomalyMessage::getDeviceId)
                .process(new EdgeAnomalyDetectionFunction(parameter, MEASURE_GROUPS))
                .name("EdgeAnomalyDetection");

        SingleOutputStreamOperator<AggregatedWindow> aggregatedStream = detectedStream
                .filter(message -> !message.isDuplicate())
                .keyBy(AnomalyMessage::getDeviceId)
                .window(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.milliseconds(windowSizeMs),
                        org.apache.flink.streaming.api.windowing.time.Time.milliseconds(slideSizeMs)))
                .apply(new EdgePreAggregationFunction(windowSizeMs, slideSizeMs))
                .name("EdgePreAggregation");

        aggregatedStream.addSink(
                        new EnhancedStreamManagerSink(
                                region,
                                ggTargetStreamName,
                                ggStreamHost,
                                ggStreamPort,
                                kinesisExportStreamName,
                                ggStreamBatchSize,
                                offlineStoragePath
                        ))
                .name("EnhancedStreamManagerSink");

        if (enableTimestream) {
            DataStream<List<TimestreamPoint>> timestreamStream = aggregatedStream
                    .map(EdgeAnomalyProcessingJob::convertToTimestreamPoints)
                    .name("ConvertToTimestream");

            timestreamStream.addSink(
                            new TimestreamSink(timestreamRegion, timestreamDbName, timestreamTableName, timestreamBatchSize))
                    .name("EdgeTimestreamSink");
        }

        logger.info("Execution Plan:\n{}", env.getExecutionPlan());
        env.execute("Edge Anomaly Processing Job");
    }

    private static void configureEnvironment(StreamExecutionEnvironment env) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(
                org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(300000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                5,
                Time.of(10, TimeUnit.SECONDS)
        ));

        env.setParallelism(2);
    }

    private static void logParameters(ParameterTool parameter) {
        List<String> params = new ArrayList<>();
        String nl = "\n";

        params.add(nl + "region: " + parameter.get("region") + nl);
        params.add("ggStreamHost: " + parameter.get("ggStreamHost") + nl);
        params.add("ggStreamPort: " + parameter.get("ggStreamPort") + nl);
        params.add("ggSourceStreamName: " + parameter.get("ggSourceStreamName") + nl);
        params.add("ggTargetStreamName: " + parameter.get("ggTargetStreamName") + nl);
        params.add("ggStreamBatchSize: " + parameter.get("ggStreamBatchSize") + nl);
        params.add("kinesisExportStreamName: " + parameter.get("kinesisExportStreamName") + nl);
        params.add("offlineStoragePath: " + parameter.get("offlineStoragePath") + nl);
        params.add("windowSizeMs: " + parameter.get("windowSizeMs") + nl);
        params.add("slideSizeMs: " + parameter.get("slideSizeMs") + nl);
        params.add("enableTimestream: " + parameter.get("enableTimestream") + nl);
        params.add("RcfShingleSize: " + parameter.get("RcfShingleSize") + nl);
        params.add("RcfShingleCyclic: " + parameter.get("RcfShingleCyclic") + nl);
        params.add("RcfNumberOfTrees: " + parameter.get("RcfNumberOfTrees") + nl);
        params.add("RcfSampleSize: " + parameter.get("RcfSampleSize") + nl);
        params.add("RcfLambda: " + parameter.get("RcfLambda") + nl);
        params.add("RcfRandomSeed: " + parameter.get("RcfRandomSeed") + nl);

        logger.info("APPLICATION PARAMETERS: " + nl + params);
    }

    private static List<TimestreamPoint> convertToTimestreamPoints(AggregatedWindow window) {
        List<TimestreamPoint> points = new ArrayList<>();
        long timestamp = window.getWindowEndMs();

        for (Map.Entry<String, AggregatedWindow.MeasurementStats> entry :
                window.getAggregatedMeasurements().entrySet()) {
            String measureName = entry.getKey();
            AggregatedWindow.MeasurementStats stats = entry.getValue();

            addTimestreamPoint(points, window.getDeviceId(), timestamp, measureName + "_avg", stats.getAvg());
            addTimestreamPoint(points, window.getDeviceId(), timestamp, measureName + "_max", stats.getMax());
            addTimestreamPoint(points, window.getDeviceId(), timestamp, measureName + "_min", stats.getMin());
            addTimestreamPoint(points, window.getDeviceId(), timestamp, measureName + "_stddev", stats.getStdDev());
        }

        for (Map.Entry<String, AggregatedWindow.AnomalyScoreStats> entry :
                window.getAggregatedAnomalyScores().entrySet()) {
            String groupName = entry.getKey();
            AggregatedWindow.AnomalyScoreStats stats = entry.getValue();

            addTimestreamPoint(points, window.getDeviceId(), timestamp, groupName + "_score_max", stats.getMax());
            addTimestreamPoint(points, window.getDeviceId(), timestamp, groupName + "_score_avg", stats.getAvg());
            addTimestreamPoint(points, window.getDeviceId(), timestamp, groupName + "_anomaly_count", stats.getAnomalyCount());
        }

        addTimestreamPoint(points, window.getDeviceId(), timestamp, "message_count", window.getMessageCount());
        addTimestreamPoint(points, window.getDeviceId(), timestamp, "window_duration_ms",
                window.getWindowEndMs() - window.getWindowStartMs());

        return points;
    }

    private static void addTimestreamPoint(List<TimestreamPoint> points, String deviceId,
                                           long timestamp, String measureName, double value) {
        TimestreamPoint point = new TimestreamPoint();
        point.setTime(timestamp);
        point.setTimeUnit("MILLISECONDS");
        point.setMeasureName(measureName);
        point.setMeasureValue(String.valueOf(value));
        point.setMeasureValueType("DOUBLE");
        point.setDimensions(Collections.singletonMap("device_id", deviceId));
        points.add(point);
    }
}
