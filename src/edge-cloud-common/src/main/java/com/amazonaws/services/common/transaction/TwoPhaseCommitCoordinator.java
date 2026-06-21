/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.common.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TwoPhaseCommitCoordinator implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(TwoPhaseCommitCoordinator.class);

    private static final long DEFAULT_TRANSACTION_TIMEOUT_MS = 300000;
    private static final int DEFAULT_MAX_RETRIES = 3;

    private final long transactionTimeoutMs;
    private final int maxRetries;

    private final Map<String, TransactionState> activeTransactions;
    private final AtomicLong totalTransactions;
    private final AtomicLong committedTransactions;
    private final AtomicLong abortedTransactions;

    public enum TransactionStatus {
        INITIATED,
        PREPARING,
        PREPARED,
        COMMITTING,
        COMMITTED,
        ABORTING,
        ABORTED,
        TIMED_OUT
    }

    public static class TransactionState implements Serializable {
        private static final long serialVersionUID = 1L;

        String transactionId;
        long checkpointId;
        long startTimeMs;
        long lastUpdateTimeMs;
        TransactionStatus status;
        List<String> participantIds;
        Map<String, Boolean> preparedParticipants;
        Map<String, Boolean> committedParticipants;
        int retryCount;
        Map<String, Object> metadata;

        public TransactionState(String transactionId, long checkpointId) {
            this.transactionId = transactionId;
            this.checkpointId = checkpointId;
            this.startTimeMs = System.currentTimeMillis();
            this.lastUpdateTimeMs = this.startTimeMs;
            this.status = TransactionStatus.INITIATED;
            this.participantIds = new ArrayList<>();
            this.preparedParticipants = new ConcurrentHashMap<>();
            this.committedParticipants = new ConcurrentHashMap<>();
            this.retryCount = 0;
            this.metadata = new HashMap<>();
        }

        public boolean allParticipantsPrepared() {
            return !participantIds.isEmpty() &&
                    preparedParticipants.size() == participantIds.size() &&
                    preparedParticipants.values().stream().allMatch(Boolean::booleanValue);
        }

        public boolean allParticipantsCommitted() {
            return !participantIds.isEmpty() &&
                    committedParticipants.size() == participantIds.size() &&
                    committedParticipants.values().stream().allMatch(Boolean::booleanValue);
        }

        public boolean isTimedOut(long timeoutMs) {
            return (System.currentTimeMillis() - startTimeMs) > timeoutMs;
        }
    }

    public interface TransactionParticipant extends Serializable {
        String getParticipantId();
        boolean prepare(String transactionId, long checkpointId) throws Exception;
        boolean commit(String transactionId, long checkpointId) throws Exception;
        boolean abort(String transactionId, long checkpointId) throws Exception;
    }

    public TwoPhaseCommitCoordinator() {
        this(DEFAULT_TRANSACTION_TIMEOUT_MS, DEFAULT_MAX_RETRIES);
    }

    public TwoPhaseCommitCoordinator(long transactionTimeoutMs, int maxRetries) {
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.maxRetries = maxRetries;
        this.activeTransactions = new ConcurrentHashMap<>();
        this.totalTransactions = new AtomicLong(0);
        this.committedTransactions = new AtomicLong(0);
        this.abortedTransactions = new AtomicLong(0);
    }

    public synchronized String beginTransaction(long checkpointId, List<String> participantIds) {
        String transactionId = UUID.randomUUID().toString();
        TransactionState state = new TransactionState(transactionId, checkpointId);
        state.participantIds = participantIds != null ? new ArrayList<>(participantIds) : new ArrayList<>();
        state.status = TransactionStatus.PREPARING;
        activeTransactions.put(transactionId, state);
        totalTransactions.incrementAndGet();

        logger.debug("Transaction started: id={}, checkpoint={}, participants={}",
                transactionId, checkpointId, participantIds);

        return transactionId;
    }

    public synchronized boolean prepare(String transactionId, String participantId, boolean success) {
        TransactionState state = activeTransactions.get(transactionId);
        if (state == null) {
            logger.warn("Prepare called for unknown transaction: {}", transactionId);
            return false;
        }

        state.preparedParticipants.put(participantId, success);
        state.lastUpdateTimeMs = System.currentTimeMillis();

        if (!success) {
            logger.warn("Participant {} failed to prepare for transaction: {}", participantId, transactionId);
            state.status = TransactionStatus.ABORTING;
            return false;
        }

        if (state.allParticipantsPrepared()) {
            state.status = TransactionStatus.PREPARED;
            logger.debug("All participants prepared for transaction: {}", transactionId);
            return true;
        }

        return true;
    }

    public synchronized boolean commit(String transactionId, String participantId, boolean success) {
        TransactionState state = activeTransactions.get(transactionId);
        if (state == null) {
            logger.warn("Commit called for unknown transaction: {}", transactionId);
            return false;
        }

        state.committedParticipants.put(participantId, success);
        state.lastUpdateTimeMs = System.currentTimeMillis();

        if (!success) {
            logger.warn("Participant {} failed to commit for transaction: {}", participantId, transactionId);
            return false;
        }

        if (state.allParticipantsCommitted()) {
            state.status = TransactionStatus.COMMITTED;
            activeTransactions.remove(transactionId);
            committedTransactions.incrementAndGet();
            logger.debug("Transaction committed successfully: {}", transactionId);
            return true;
        }

        return true;
    }

    public synchronized boolean abortTransaction(String transactionId, String reason) {
        TransactionState state = activeTransactions.get(transactionId);
        if (state == null) {
            logger.warn("Abort called for unknown transaction: {}", transactionId);
            return false;
        }

        state.status = TransactionStatus.ABORTED;
        activeTransactions.remove(transactionId);
        abortedTransactions.incrementAndGet();

        logger.info("Transaction aborted: id={}, reason={}", transactionId, reason);
        return true;
    }

    public synchronized void checkTimeouts() {
        long now = System.currentTimeMillis();
        List<String> timedOutTransactions = new ArrayList<>();

        for (Map.Entry<String, TransactionState> entry : activeTransactions.entrySet()) {
            TransactionState state = entry.getValue();
            if (state.isTimedOut(transactionTimeoutMs)) {
                state.status = TransactionStatus.TIMED_OUT;
                timedOutTransactions.add(entry.getKey());
                logger.warn("Transaction timed out: id={}, age={}ms",
                        entry.getKey(), now - state.startTimeMs);
            }
        }

        for (String txId : timedOutTransactions) {
            activeTransactions.remove(txId);
            abortedTransactions.incrementAndGet();
        }
    }

    public boolean executeTwoPhaseCommit(long checkpointId, List<TransactionParticipant> participants) {
        if (participants == null || participants.isEmpty()) {
            logger.warn("No participants for checkpoint: {}", checkpointId);
            return true;
        }

        List<String> participantIds = new ArrayList<>();
        for (TransactionParticipant p : participants) {
            participantIds.add(p.getParticipantId());
        }

        String transactionId = beginTransaction(checkpointId, participantIds);
        TransactionState state = activeTransactions.get(transactionId);

        if (state == null) {
            return false;
        }

        state.status = TransactionStatus.PREPARING;

        boolean allPrepared = true;
        for (TransactionParticipant participant : participants) {
            try {
                boolean prepared = participant.prepare(transactionId, checkpointId);
                if (!prepare(transactionId, participant.getParticipantId(), prepared)) {
                    allPrepared = false;
                    break;
                }
            } catch (Exception e) {
                logger.error("Error during prepare phase for participant {}",
                        participant.getParticipantId(), e);
                prepare(transactionId, participant.getParticipantId(), false);
                allPrepared = false;
                break;
            }
        }

        if (!allPrepared) {
            abortAllParticipants(transactionId, checkpointId, participants, "Prepare phase failed");
            return false;
        }

        state.status = TransactionStatus.COMMITTING;

        boolean allCommitted = true;
        for (TransactionParticipant participant : participants) {
            try {
                boolean committed = participant.commit(transactionId, checkpointId);
                if (!commit(transactionId, participant.getParticipantId(), committed)) {
                    allCommitted = false;
                    break;
                }
            } catch (Exception e) {
                logger.error("Error during commit phase for participant {}",
                        participant.getParticipantId(), e);
                commit(transactionId, participant.getParticipantId(), false);
                allCommitted = false;
                break;
            }
        }

        if (!allCommitted) {
            logger.error("Transaction commit failed: {}", transactionId);
            return false;
        }

        return true;
    }

    private void abortAllParticipants(String transactionId, long checkpointId,
                                      List<TransactionParticipant> participants, String reason) {
        for (TransactionParticipant participant : participants) {
            try {
                participant.abort(transactionId, checkpointId);
            } catch (Exception e) {
                logger.error("Error during abort for participant {}",
                        participant.getParticipantId(), e);
            }
        }
        abortTransaction(transactionId, reason);
    }

    public TransactionStatus getTransactionStatus(String transactionId) {
        TransactionState state = activeTransactions.get(transactionId);
        return state != null ? state.status : null;
    }

    public int getActiveTransactionCount() {
        checkTimeouts();
        return activeTransactions.size();
    }

    public long getTotalTransactions() {
        return totalTransactions.get();
    }

    public long getCommittedTransactions() {
        return committedTransactions.get();
    }

    public long getAbortedTransactions() {
        return abortedTransactions.get();
    }

    public double getCommitSuccessRate() {
        long total = totalTransactions.get();
        if (total == 0) return 1.0;
        return (double) committedTransactions.get() / total;
    }

    public synchronized void resetStats() {
        totalTransactions.set(0);
        committedTransactions.set(0);
        abortedTransactions.set(0);
        activeTransactions.clear();
    }
}
