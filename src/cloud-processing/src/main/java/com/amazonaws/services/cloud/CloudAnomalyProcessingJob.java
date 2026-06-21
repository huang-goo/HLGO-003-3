/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.cloud;

import com.amazonaws.services.cloud.notification.AlertNotificationSink;
import com.amazonaws.services.cloud.operators.CloudDeduplicationFunction;
import com.amazonaws.services.cloud.operators.CloudSecondaryScoringFunction;
import com.amazonaws.services.common.model.AggregatedWindow;
import com.amazonaws.services.common.model.AnomalyAlert;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.kinesisanalytics.utils.ParameterToolUtils;
import com.amazonaws.services.timestream.TimestreamInitializer;
import com.amazonaws.services.timestream.TimestreamPoint;
import com.amazonaws.services.timestream.TimestreamSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CloudAnomalyProcessingJob {

    private static final Logger logger = LoggerFactory.getLogger(CloudAnomalyProcessingJob.class);

    private static final List<String> MEASURE_GROUPS = Arrays.asList(
            "reactor_feed",
            "purge_gas",
            "product"
    );

    private static final String DEFAULT_STREAM_NAME = "tep-ingest-greengrass";
    private static final String DEFAULT_REGION_NAME = "eu-central-1";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        configureEnvironment(env);

        ParameterTool parameter = loadParameters(env, args);
        env.getConfig().setGlobalJobParameters(parameter);

        logParameters(parameter);

        String region = parameter.get("KinesisRegion", DEFAULT_REGION_NAME);
        String inputStreamName = parameter.get("InputStreamName", DEFAULT_STREAM_NAME);

        String timestreamRegion = parameter.get("TimeStreamRegion", "eu-central-1");
        String timestreamDbName = parameter.get("TimeStreamDbName", "kdaflink");
        String timestreamTableName = parameter.get("TimeStreamTableName", "cloud_anomalies");
        String alertTimestreamTableName = parameter.get("AlertTimestreamTableName", "cloud_alerts");
        int timestreamBatchSize = Integer.parseInt(parameter.get("TimeStreamIngestBatchSize", "50"));

        String snsTopicArn = parameter.get("SnsTopicArn", "");
        String cloudwatchNamespace = parameter.get("CloudWatchNamespace", "EdgeCloudAnomalyDetection");

        boolean enableAlerts = Boolean.parseBoolean(parameter.get("enableAlerts", "true"));
        boolean enableSns = Boolean.parseBoolean(parameter.get("enableSns", "false"));
        boolean enableCloudWatch = Boolean.parseBoolean(parameter.get("enableCloudWatch", "true"));

        TimestreamInitializer timestreamInitializer = new TimestreamInitializer(timestreamRegion);
        timestreamInitializer.createDatabase(timestreamDbName);
        timestreamInitializer.createTable(timestreamDbName, timestreamTableName);
        timestreamInitializer.createTable(timestreamDbName, alertTimestreamTableName);

        final OutputTag<String> csvOutputTag = new OutputTag<String>("csv-output") {};
        final OutputTag<AnomalyAlert> alertOutputTag = CloudSecondaryScoringFunction.ALERT_OUTPUT_TAG;

        DataStream<String> rawInput = createKinesisSource(env, parameter, inputStreamName);

        SingleOutputStreamOperator<String> withCsvSideOutput = rawInput
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value);

                        JsonNode jsonTree = new ObjectMapper().readTree(value);
                        Builder csvSchemaBuilder = CsvSchema.builder();
                        jsonTree.fieldNames().forEachRemaining(csvSchemaBuilder::addColumn);
                        CsvSchema csvSchema = csvSchemaBuilder.build();
                        CsvMapper csvMapper = new CsvMapper();
                        String csvOut = csvMapper.writerFor(JsonNode.class)
                                .with(csvSchema)
                                .writeValueAsString(jsonTree);
                        ctx.output(csvOutputTag, csvOut);
                    }
                })
                .name("RawDataWithCsvSideOutput");

        DataStream<AggregatedWindow> dedupedStream = withCsvSideOutput
                .keyBy(value -> {
                    try {
                        com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(value).getAsJsonObject();
                        if (json.has("device_id")) {
                            return json.get("device_id").getAsString();
                        }
                    } catch (Exception e) {
                        logger.debug("Failed to extract device_id from payload");
                    }
                    return "unknown";
                })
                .process(new CloudDeduplicationFunction())
                .name("CloudDeduplication");

        dedupedStream = dedupedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<AggregatedWindow>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withTimestampAssigner((window, timestamp) -> window.getWindowEndMs())
        );

        SingleOutputStreamOperator<AggregatedWindow> scoredStream = dedupedStream
                .keyBy(AggregatedWindow::getDeviceId)
                .process(new CloudSecondaryScoringFunction(parameter, MEASURE_GROUPS))
                .name("CloudSecondaryScoring");

        DataStream<List<TimestreamPoint>> timestreamStream = scoredStream
                .map(CloudAnomalyProcessingJob::convertToTimestreamPoints)
                .name("ConvertToTimestream");

        timestreamStream.addSink(
                        new TimestreamSink(timestreamRegion, timestreamDbName, timestreamTableName, timestreamBatchSize))
                .name("MainTimestreamSink");

        if (enableAlerts) {
            DataStream<AnomalyAlert> alertStream = scoredStream.getSideOutput(alertOutputTag);

            AlertNotificationSink alertSink = new AlertNotificationSink();
            alertSink.addChannel(new AlertNotificationSink.LogNotificationChannel());
            alertSink.addChannel(new AlertNotificationSink.ConsoleNotificationChannel());

            if (enableSns && !snsTopicArn.isEmpty()) {
                alertSink.addChannel(new AlertNotificationSink.SnsNotificationChannel(snsTopicArn, region));
            }

            if (enableCloudWatch) {
                alertSink.addChannel(new AlertNotificationSink.CloudWatchMetricsChannel(cloudwatchNamespace, region));
            }

            alertStream.addSink(alertSink)
                    .name("AlertNotificationSink");

            DataStream<List<TimestreamPoint>> alertTimestreamStream = alertStream
                    .map(CloudAnomalyProcessingJob::convertAlertToTimestreamPoints)
                    .name("ConvertAlertToTimestream");

            alertTimestreamStream.addSink(
                            new TimestreamSink(timestreamRegion, timestreamDbName, alertTimestreamTableName, timestreamBatchSize))
                    .name("AlertTimestreamSink");
        }

        DataStream<String> csvSideStream = ((SingleOutputStreamOperator<String>) withCsvSideOutput).getSideOutput(csvOutputTag);

        logger.info("Execution Plan:\n{}", env.getExecutionPlan());
        env.execute("Cloud Anomaly Processing Job - Secondary Scoring and Alerting");
    }

    private static void configureEnvironment(StreamExecutionEnvironment env) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(300000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                5,
                Time.of(10, TimeUnit.SECONDS)
        ));

        env.setParallelism(4);
    }

    private static ParameterTool loadParameters(StreamExecutionEnvironment env, String[] args) throws Exception {
        if (env instanceof LocalStreamEnvironment) {
            return ParameterToolUtils.fromArgsAndApplicationProperties(args);
        } else {
            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
            Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");

            if (flinkProperties == null) {
                throw new RuntimeException("Unable to load FlinkApplicationProperties from Kinesis Analytics Runtime.");
            }

            return ParameterToolUtils.fromApplicationProperties(flinkProperties);
        }
    }

    private static DataStream<String> createKinesisSource(StreamExecutionEnvironment env,
                                                          ParameterTool parameter,
                                                          String streamName) {
        Properties kinesisConsumerConfig = new Properties();
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION,
                parameter.get("KinesisRegion", DEFAULT_REGION_NAME));
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
        kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
                parameter.get("SHARD_GETRECORDS_INTERVAL_MILLIS", "1000"));
        kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
                parameter.get("SHARD_GETRECORDS_MAX", "10000"));
        kinesisConsumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(
                        parameter.get("InputStreamName", streamName),
                        new SimpleStringSchema(),
                        kinesisConsumerConfig))
                .name("KinesisSource");
    }

    private static void logParameters(ParameterTool parameter) {
        List<String> params = new ArrayList<>();
        String nl = "\n";

        params.add(nl + "InputStreamName: " + parameter.get("InputStreamName") + nl);
        params.add("KinesisRegion: " + parameter.get("KinesisRegion") + nl);
        params.add("TimeStreamRegion: " + parameter.get("TimeStreamRegion") + nl);
        params.add("TimeStreamDbName: " + parameter.get("TimeStreamDbName") + nl);
        params.add("TimeStreamTableName: " + parameter.get("TimeStreamTableName") + nl);
        params.add("AlertTimestreamTableName: " + parameter.get("AlertTimestreamTableName") + nl);
        params.add("TimeStreamIngestBatchSize: " + parameter.get("TimeStreamIngestBatchSize") + nl);
        params.add("AnomalyThreshold: " + parameter.get("AnomalyThreshold", "1.5") + nl);
        params.add("enableAlerts: " + parameter.get("enableAlerts", "true") + nl);
        params.add("enableSns: " + parameter.get("enableSns", "false") + nl);
        params.add("SnsTopicArn: " + parameter.get("SnsTopicArn", "") + nl);
        params.add("enableCloudWatch: " + parameter.get("enableCloudWatch", "true") + nl);
        params.add("CloudWatchNamespace: " + parameter.get("CloudWatchNamespace", "EdgeCloudAnomalyDetection") + nl);
        params.add("CloudRcfShingleSize: " + parameter.get("CloudRcfShingleSize", "4") + nl);
        params.add("CloudRcfNumberOfTrees: " + parameter.get("CloudRcfNumberOfTrees", "100") + nl);
        params.add("CloudRcfSampleSize: " + parameter.get("CloudRcfSampleSize", "256") + nl);

        logger.info("APPLICATION PARAMETERS: " + nl + params);
    }

    private static List<TimestreamPoint> convertToTimestreamPoints(AggregatedWindow window) {
        List<TimestreamPoint> points = new ArrayList<>();
        long timestamp = window.getWindowEndMs();
        String deviceId = window.getDeviceId();

        for (Map.Entry<String, AggregatedWindow.MeasurementStats> entry :
                window.getAggregatedMeasurements().entrySet()) {
            String measureName = entry.getKey();
            AggregatedWindow.MeasurementStats stats = entry.getValue();

            addTimestreamPoint(points, deviceId, timestamp, measureName + "_avg", stats.getAvg(), window.getWindowId());
            addTimestreamPoint(points, deviceId, timestamp, measureName + "_max", stats.getMax(), window.getWindowId());
            addTimestreamPoint(points, deviceId, timestamp, measureName + "_min", stats.getMin(), window.getWindowId());
            addTimestreamPoint(points, deviceId, timestamp, measureName + "_stddev", stats.getStdDev(), window.getWindowId());
            addTimestreamPoint(points, deviceId, timestamp, measureName + "_count", stats.getCount(), window.getWindowId());
        }

        for (Map.Entry<String, AggregatedWindow.AnomalyScoreStats> entry :
                window.getAggregatedAnomalyScores().entrySet()) {
            String groupName = entry.getKey();
            AggregatedWindow.AnomalyScoreStats stats = entry.getValue();

            addTimestreamPoint(points, deviceId, timestamp, groupName + "_score_max", stats.getMax(), window.getWindowId());
            addTimestreamPoint(points, deviceId, timestamp, groupName + "_score_avg", stats.getAvg(), window.getWindowId());
            addTimestreamPoint(points, deviceId, timestamp, groupName + "_anomaly_count", stats.getAnomalyCount(), window.getWindowId());
            addTimestreamPoint(points, deviceId, timestamp, groupName + "_anomaly_ratio", stats.getAnomalyRatio(), window.getWindowId());
        }

        addTimestreamPoint(points, deviceId, timestamp, "message_count", window.getMessageCount(), window.getWindowId());

        if (window.getMetadata() != null) {
            for (Map.Entry<String, String> entry : window.getMetadata().entrySet()) {
                try {
                    double value = Double.parseDouble(entry.getValue());
                    addTimestreamPoint(points, deviceId, timestamp, entry.getKey(), value, window.getWindowId());
                } catch (NumberFormatException e) {
                }
            }
        }

        return points;
    }

    private static List<TimestreamPoint> convertAlertToTimestreamPoints(AnomalyAlert alert) {
        List<TimestreamPoint> points = new ArrayList<>();
        long timestamp = alert.getTimestampMs();
        String deviceId = alert.getDeviceId();

        Map<String, String> dimensions = new HashMap<>();
        dimensions.put("device_id", deviceId);
        dimensions.put("alert_id", alert.getAlertId());
        dimensions.put("alert_type", alert.getAlertType().toString());
        dimensions.put("severity", alert.getSeverity().toString());
        dimensions.put("anomaly_group", alert.getAnomalyGroup());

        TimestreamPoint scorePoint = new TimestreamPoint();
        scorePoint.setTime(timestamp);
        scorePoint.setTimeUnit("MILLISECONDS");
        scorePoint.setMeasureName("anomaly_score");
        scorePoint.setMeasureValue(String.valueOf(alert.getAnomalyScore()));
        scorePoint.setMeasureValueType("DOUBLE");
        scorePoint.setDimensions(dimensions);
        points.add(scorePoint);

        TimestreamPoint thresholdPoint = new TimestreamPoint();
        thresholdPoint.setTime(timestamp);
        thresholdPoint.setTimeUnit("MILLISECONDS");
        thresholdPoint.setMeasureName("anomaly_threshold");
        thresholdPoint.setMeasureValue(String.valueOf(alert.getAnomalyThreshold()));
        thresholdPoint.setMeasureValueType("DOUBLE");
        thresholdPoint.setDimensions(dimensions);
        points.add(thresholdPoint);

        TimestreamPoint severityPoint = new TimestreamPoint();
        severityPoint.setTime(timestamp);
        severityPoint.setTimeUnit("MILLISECONDS");
        severityPoint.setMeasureName("severity_level");
        severityPoint.setMeasureValue(String.valueOf(alert.getSeverity().getLevel()));
        severityPoint.setMeasureValueType("BIGINT");
        severityPoint.setDimensions(dimensions);
        points.add(severityPoint);

        if (alert.getMeasurementsInvolved() != null) {
            for (Map.Entry<String, Double> entry : alert.getMeasurementsInvolved().entrySet()) {
                TimestreamPoint measurePoint = new TimestreamPoint();
                measurePoint.setTime(timestamp);
                measurePoint.setTimeUnit("MILLISECONDS");
                measurePoint.setMeasureName("measurement_" + entry.getKey());
                measurePoint.setMeasureValue(String.valueOf(entry.getValue()));
                measurePoint.setMeasureValueType("DOUBLE");
                measurePoint.setDimensions(dimensions);
                points.add(measurePoint);
            }
        }

        return points;
    }

    private static void addTimestreamPoint(List<TimestreamPoint> points, String deviceId,
                                           long timestamp, String measureName, double value, String windowId) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put("device_id", deviceId);
        dimensions.put("window_id", windowId);

        TimestreamPoint point = new TimestreamPoint();
        point.setTime(timestamp);
        point.setTimeUnit("MILLISECONDS");
        point.setMeasureName(measureName);
        point.setMeasureValue(String.valueOf(value));
        point.setMeasureValueType("DOUBLE");
        point.setDimensions(dimensions);
        points.add(point);
    }
}
