/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.test;

import com.amazonaws.services.common.model.AggregatedWindow;
import com.amazonaws.services.common.model.AnomalyMessage;
import com.amazonaws.services.common.utils.BloomFilter;
import com.amazonaws.services.common.utils.ClockDriftCorrector;
import com.amazonaws.services.common.utils.OfflineMessageStore;
import com.amazonaws.services.common.transaction.TwoPhaseCommitCoordinator;
import com.amazonaws.services.common.transaction.TwoPhaseCommitCoordinator.TransactionParticipant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class EndToEndIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(EndToEndIntegrationTest.class);

    private static final String TEST_DEVICE_ID = "integration-test-device-001";

    public static void main(String[] args) {
        System.out.println("================================================");
        System.out.println("Edge-Cloud Anomaly Detection End-to-End Test");
        System.out.println("================================================");
        System.out.println();

        boolean allPassed = true;

        allPassed &= testBloomFilterDeduplication();
        allPassed &= testClockDriftCorrection();
        allPassed &= testOfflineMessageStore();
        allPassed &= testTwoPhaseCommit();
        allPassed &= testMessageSerialization();
        allPassed &= testAnomalyScoreAggregation();

        System.out.println();
        System.out.println("================================================");
        System.out.println("Test Results: " + (allPassed ? "ALL PASSED" : "SOME FAILED"));
        System.out.println("================================================");

        System.exit(allPassed ? 0 : 1);
    }

    private static boolean testBloomFilterDeduplication() {
        System.out.println("Test 1: BloomFilter Deduplication");
        System.out.println("---------------------------------");

        try {
            BloomFilter bloomFilter = new BloomFilter(10000, 0.01);
            TestDataGenerator generator = new TestDataGenerator(TEST_DEVICE_ID);

            int totalMessages = 1000;
            int duplicateMessages = 100;
            Set<String> seenKeys = new HashSet<>();
            int falsePositives = 0;

            for (int i = 0; i < totalMessages; i++) {
                AnomalyMessage msg = generator.generateNormalMessage();
                String dedupKey = msg.generateDeduplicationKey();

                boolean mightContain = bloomFilter.mightContain(dedupKey);
                boolean shouldContain = seenKeys.contains(dedupKey);

                if (mightContain && !shouldContain) {
                    falsePositives++;
                }

                bloomFilter.put(dedupKey);
                seenKeys.add(dedupKey);
            }

            AnomalyMessage original = generator.generateNormalMessage();
            String originalKey = original.generateDeduplicationKey();
            bloomFilter.put(originalKey);

            AnomalyMessage duplicate = generator.generateDuplicate(original);
            String duplicateKey = duplicate.generateDeduplicationKey();

            boolean duplicateDetected = bloomFilter.mightContain(duplicateKey);
            double fpp = (double) falsePositives / totalMessages;

            System.out.printf("  Total messages: %d%n", totalMessages);
            System.out.printf("  False positives: %d (%.2f%%)%n", falsePositives, fpp * 100);
            System.out.printf("  Duplicate detected: %s%n", duplicateDetected);
            System.out.printf("  Expected FPP: %.2f%%, Actual: %.2f%%%n",
                    bloomFilter.getFalsePositiveProbability() * 100,
                    bloomFilter.getExpectedFalsePositiveProbability() * 100);

            boolean passed = duplicateDetected && fpp < 0.02;
            System.out.println("  Result: " + (passed ? "PASSED" : "FAILED"));
            System.out.println();
            return passed;

        } catch (Exception e) {
            logger.error("Test failed", e);
            System.out.println("  Result: FAILED - " + e.getMessage());
            System.out.println();
            return false;
        }
    }

    private static boolean testClockDriftCorrection() {
        System.out.println("Test 2: Clock Drift Correction");
        System.out.println("------------------------------");

        try {
            ClockDriftCorrector corrector = new ClockDriftCorrector();
            TestDataGenerator generator = new TestDataGenerator(TEST_DEVICE_ID);

            long knownOffset = 5000;
            int samples = 200;

            for (int i = 0; i < samples; i++) {
                AnomalyMessage msg = generator.generateNormalMessage();
                long edgeTime = msg.getEdgeTimestampMs();
                long cloudTime = edgeTime + knownOffset + (long) (random.nextGaussian() * 100);
                long latency = 50 + (long) (random.nextDouble() * 50);

                corrector.recordOffsetSample(edgeTime, cloudTime, latency);
            }

            long estimatedOffset = corrector.getCurrentOffsetMs();
            long error = Math.abs(estimatedOffset - knownOffset);

            System.out.printf("  Known offset: %dms%n", knownOffset);
            System.out.printf("  Estimated offset: %dms%n", estimatedOffset);
            System.out.printf("  Error: %dms%n", error);
            System.out.printf("  Initialized: %s%n", corrector.isInitialized());
            System.out.printf("  Samples: %d%n", corrector.getTotalSamples());
            System.out.printf("  Offset std dev: %.2fms%n", corrector.getOffsetStdDev());

            boolean passed = corrector.isInitialized() && error < 500;
            System.out.println("  Result: " + (passed ? "PASSED" : "FAILED"));
            System.out.println();
            return passed;

        } catch (Exception e) {
            logger.error("Test failed", e);
            System.out.println("  Result: FAILED - " + e.getMessage());
            System.out.println();
            return false;
        }
    }

    private static boolean testOfflineMessageStore() {
        System.out.println("Test 3: Offline Message Store");
        System.out.println("-----------------------------");

        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("offline-test-");
            OfflineMessageStore store = new OfflineMessageStore(tempDir.toString());
            TestDataGenerator generator = new TestDataGenerator(TEST_DEVICE_ID);

            int messagesToStore = 10;
            List<String> storedIds = new ArrayList<>();

            for (int i = 0; i < messagesToStore; i++) {
                AnomalyMessage msg = generator.generateNormalMessage();
                int priority = i % 3 == 0 ? 1 : 0;
                boolean stored = store.storeMessage(msg.toJson(), priority);
                if (stored) {
                    storedIds.add(msg.getMessageId());
                }
            }

            System.out.printf("  Messages stored: %d%n", store.getTotalStored());
            System.out.printf("  Pending: %d%n", store.getPendingMessageCount());

            List<OfflineMessageStore.StoredMessage> messages = store.getMessagesForForwarding(5);
            System.out.printf("  Retrieved for forwarding: %d%n", messages.size());

            for (int i = 0; i < messages.size() / 2; i++) {
                store.confirmDelivery(messages.get(i).getMessageId());
            }

            for (int i = messages.size() / 2; i < messages.size(); i++) {
                store.failDelivery(messages.get(i).getMessageId(), true);
            }

            System.out.printf("  Forwarded: %d%n", store.getTotalForwarded());
            System.out.printf("  Remaining pending: %d%n", store.getPendingMessageCount());
            System.out.printf("  Storage utilization: %.2f%%%n", store.getStorageUtilization() * 100);

            boolean passed = store.getTotalStored() == messagesToStore
                    && store.getTotalForwarded() > 0
                    && store.getPendingMessageCount() > 0;

            store.close();
            System.out.println("  Result: " + (passed ? "PASSED" : "FAILED"));
            System.out.println();
            return passed;

        } catch (Exception e) {
            logger.error("Test failed", e);
            System.out.println("  Result: FAILED - " + e.getMessage());
            System.out.println();
            return false;
        } finally {
            if (tempDir != null) {
                try {
                    deleteDirectory(tempDir);
                } catch (IOException e) {
                    logger.warn("Failed to clean up temp directory", e);
                }
            }
        }
    }

    private static boolean testTwoPhaseCommit() {
        System.out.println("Test 4: Two-Phase Commit Coordinator");
        System.out.println("------------------------------------");

        try {
            TwoPhaseCommitCoordinator coordinator = new TwoPhaseCommitCoordinator();

            List<TransactionParticipant> participants = new ArrayList<>();
            participants.add(createMockParticipant("source", true, true));
            participants.add(createMockParticipant("processor", true, true));
            participants.add(createMockParticipant("sink", true, true));

            boolean commitResult = coordinator.executeTwoPhaseCommit(1001L, participants);

            System.out.printf("  Total transactions: %d%n", coordinator.getTotalTransactions());
            System.out.printf("  Committed: %d%n", coordinator.getCommittedTransactions());
            System.out.printf("  Aborted: %d%n", coordinator.getAbortedTransactions());
            System.out.printf("  Commit success rate: %.2f%%%n", coordinator.getCommitSuccessRate() * 100);
            System.out.printf("  2PC result: %s%n", commitResult);

            List<TransactionParticipant> failingParticipants = new ArrayList<>();
            failingParticipants.add(createMockParticipant("source", true, true));
            failingParticipants.add(createMockParticipant("failing-processor", false, false));
            failingParticipants.add(createMockParticipant("sink", true, true));

            boolean failedResult = coordinator.executeTwoPhaseCommit(1002L, failingParticipants);

            System.out.printf("  After failing test:%n");
            System.out.printf("    Total: %d, Committed: %d, Aborted: %d%n",
                    coordinator.getTotalTransactions(),
                    coordinator.getCommittedTransactions(),
                    coordinator.getAbortedTransactions());

            boolean passed = commitResult && !failedResult
                    && coordinator.getCommittedTransactions() == 1
                    && coordinator.getAbortedTransactions() == 1;

            System.out.println("  Result: " + (passed ? "PASSED" : "FAILED"));
            System.out.println();
            return passed;

        } catch (Exception e) {
            logger.error("Test failed", e);
            System.out.println("  Result: FAILED - " + e.getMessage());
            System.out.println();
            return false;
        }
    }

    private static boolean testMessageSerialization() {
        System.out.println("Test 5: Message Serialization");
        System.out.println("------------------------------");

        try {
            TestDataGenerator generator = new TestDataGenerator(TEST_DEVICE_ID);
            AnomalyMessage original = generator.generateAnomalyMessage();
            original.addAnomalyScore("reactor_feed", 2.5);
            original.addAnomalyScore("purge_gas", 1.8);
            original.addMetadata("test_key", "test_value");
            original.setProcessingStage(AnomalyMessage.ProcessingStage.EDGE_AGGREGATED);

            String json = original.toJson();
            AnomalyMessage deserialized = AnomalyMessage.fromJson(json);

            boolean idMatch = original.getMessageId().equals(deserialized.getMessageId());
            boolean idempotencyMatch = original.getIdempotencyKey().equals(deserialized.getIdempotencyKey());
            boolean deviceMatch = original.getDeviceId().equals(deserialized.getDeviceId());
            boolean seqMatch = original.getSequenceNumber() == deserialized.getSequenceNumber();
            boolean timeMatch = original.getEdgeTimestampMs() == deserialized.getEdgeTimestampMs();
            boolean stageMatch = original.getProcessingStage() == deserialized.getProcessingStage();
            boolean measuresMatch = original.getMeasurements().equals(deserialized.getMeasurements());
            boolean scoresMatch = original.getAnomalyScores().equals(deserialized.getAnomalyScores());

            System.out.printf("  Message ID match: %s%n", idMatch);
            System.out.printf("  Idempotency key match: %s%n", idempotencyMatch);
            System.out.printf("  Device ID match: %s%n", deviceMatch);
            System.out.printf("  Sequence number match: %s%n", seqMatch);
            System.out.printf("  Timestamp match: %s%n", timeMatch);
            System.out.printf("  Processing stage match: %s%n", stageMatch);
            System.out.printf("  Measurements match: %s%n", measuresMatch);
            System.out.printf("  Anomaly scores match: %s%n", scoresMatch);
            System.out.printf("  JSON size: %d bytes%n", json.getBytes().length);

            boolean passed = idMatch && idempotencyMatch && deviceMatch && seqMatch
                    && timeMatch && stageMatch && measuresMatch && scoresMatch;

            System.out.println("  Result: " + (passed ? "PASSED" : "FAILED"));
            System.out.println();
            return passed;

        } catch (Exception e) {
            logger.error("Test failed", e);
            System.out.println("  Result: FAILED - " + e.getMessage());
            System.out.println();
            return false;
        }
    }

    private static boolean testAnomalyScoreAggregation() {
        System.out.println("Test 6: Anomaly Score Aggregation");
        System.out.println("---------------------------------");

        try {
            TestDataGenerator generator = new TestDataGenerator(TEST_DEVICE_ID);
            long windowStart = System.currentTimeMillis() - 60000;
            long windowEnd = System.currentTimeMillis();

            AggregatedWindow window = new AggregatedWindow(TEST_DEVICE_ID, windowStart, windowEnd);

            int normalMessages = 80;
            int anomalyMessages = 20;

            for (int i = 0; i < normalMessages; i++) {
                AnomalyMessage msg = generator.generateNormalMessage();
                msg.addAnomalyScore("reactor_feed", 0.5 + random.nextDouble() * 0.5);
                window.addMessage(msg);
            }

            for (int i = 0; i < anomalyMessages; i++) {
                AnomalyMessage msg = generator.generateAnomalyMessage();
                msg.addAnomalyScore("reactor_feed", 1.5 + random.nextDouble() * 2.0);
                window.addMessage(msg);
            }

            System.out.printf("  Window: %s%n", window.getWindowId());
            System.out.printf("  Message count: %d%n", window.getMessageCount());
            System.out.printf("  Has high anomaly score (threshold=1.5): %s%n",
                    window.hasHighAnomalyScore(1.5));

            Map<String, AggregatedWindow.AnomalyScoreStats> scores = window.getAggregatedAnomalyScores();
            for (Map.Entry<String, AggregatedWindow.AnomalyScoreStats> entry : scores.entrySet()) {
                AggregatedWindow.AnomalyScoreStats stats = entry.getValue();
                System.out.printf("  Group: %s%n", entry.getKey());
                System.out.printf("    Count: %d%n", stats.getCount());
                System.out.printf("    Max: %.4f%n", stats.getMax());
                System.out.printf("    Avg: %.4f%n", stats.getAvg());
                System.out.printf("    Anomaly count: %d%n", stats.getAnomalyCount());
                System.out.printf("    Anomaly ratio: %.2f%%%n", stats.getAnomalyRatio() * 100);
            }

            boolean passed = window.getMessageCount() == (normalMessages + anomalyMessages)
                    && window.hasHighAnomalyScore(1.5)
                    && scores.containsKey("reactor_feed")
                    && scores.get("reactor_feed").getAnomalyCount() == anomalyMessages;

            System.out.println("  Result: " + (passed ? "PASSED" : "FAILED"));
            System.out.println();
            return passed;

        } catch (Exception e) {
            logger.error("Test failed", e);
            System.out.println("  Result: FAILED - " + e.getMessage());
            System.out.println();
            return false;
        }
    }

    private static TransactionParticipant createMockParticipant(String id, boolean prepareResult, boolean commitResult) {
        return new TransactionParticipant() {
            @Override
            public String getParticipantId() {
                return id;
            }

            @Override
            public boolean prepare(String transactionId, long checkpointId) throws Exception {
                logger.debug("Participant {} preparing transaction {}", id, transactionId);
                return prepareResult;
            }

            @Override
            public boolean commit(String transactionId, long checkpointId) throws Exception {
                logger.debug("Participant {} committing transaction {}", id, transactionId);
                return commitResult;
            }

            @Override
            public boolean abort(String transactionId, long checkpointId) throws Exception {
                logger.debug("Participant {} aborting transaction {}", id, transactionId);
                return true;
            }
        };
    }

    private static void deleteDirectory(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.list(path).forEach(child -> {
                try {
                    deleteDirectory(child);
                } catch (IOException e) {
                    logger.warn("Failed to delete {}", child, e);
                }
            });
        }
        Files.deleteIfExists(path);
    }

    private static final Random random = new Random(42);
}
