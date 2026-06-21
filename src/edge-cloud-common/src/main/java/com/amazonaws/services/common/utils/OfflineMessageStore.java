/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0 */

package com.amazonaws.services.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class OfflineMessageStore implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(OfflineMessageStore.class);

    private static final int DEFAULT_MAX_RETRY_COUNT = 5;
    private static final long DEFAULT_BASE_RETRY_DELAY_MS = 1000;
    private static final long DEFAULT_MAX_RETRY_DELAY_MS = 60000;
    private static final long DEFAULT_MAX_STORAGE_SIZE_BYTES = 1024 * 1024 * 1024;
    private static final long DEFAULT_MESSAGE_TTL_MS = 24 * 60 * 60 * 1000;

    private final Path storagePath;
    private final int maxRetryCount;
    private final long baseRetryDelayMs;
    private final long maxRetryDelayMs;
    private final long maxStorageSizeBytes;
    private final long messageTtlMs;

    private final BlockingQueue<StoredMessage> pendingMessages;
    private final Map<String, StoredMessage> inFlightMessages;
    private final AtomicLong totalStored;
    private final AtomicLong totalForwarded;
    private final AtomicLong totalFailed;

    public static class StoredMessage implements Serializable, Comparable<StoredMessage> {
        private static final long serialVersionUID = 1L;

        String messageId;
        String payload;
        long originalTimestampMs;
        long storedTimestampMs;
        long lastRetryTimestampMs;
        int retryCount;
        int priority;
        boolean isPersisted;

        public StoredMessage(String messageId, String payload, int priority) {
            this.messageId = messageId;
            this.payload = payload;
            this.originalTimestampMs = System.currentTimeMillis();
            this.storedTimestampMs = this.originalTimestampMs;
            this.lastRetryTimestampMs = 0;
            this.retryCount = 0;
            this.priority = priority;
            this.isPersisted = false;
        }

        public long getNextRetryDelayMs(long baseDelay, long maxDelay) {
            long delay = baseDelay * (long) Math.pow(2, retryCount);
            return Math.min(delay, maxDelay);
        }

        public boolean isExpired(long ttlMs) {
            return (System.currentTimeMillis() - originalTimestampMs) > ttlMs;
        }

        public boolean shouldRetry(int maxRetryCount) {
            return retryCount < maxRetryCount;
        }

        @Override
        public int compareTo(StoredMessage other) {
            if (this.priority != other.priority) {
                return Integer.compare(other.priority, this.priority);
            }
            return Long.compare(this.originalTimestampMs, other.originalTimestampMs);
        }

        public String getMessageId() {
            return messageId;
        }

        public String getPayload() {
            return payload;
        }

        public long getOriginalTimestampMs() {
            return originalTimestampMs;
        }

        public int getRetryCount() {
            return retryCount;
        }

        public void incrementRetryCount() {
            this.retryCount++;
            this.lastRetryTimestampMs = System.currentTimeMillis();
        }
    }

    public OfflineMessageStore(String storageDir) throws IOException {
        this(storageDir, DEFAULT_MAX_RETRY_COUNT, DEFAULT_BASE_RETRY_DELAY_MS,
                DEFAULT_MAX_RETRY_DELAY_MS, DEFAULT_MAX_STORAGE_SIZE_BYTES, DEFAULT_MESSAGE_TTL_MS);
    }

    public OfflineMessageStore(String storageDir, int maxRetryCount, long baseRetryDelayMs,
                               long maxRetryDelayMs, long maxStorageSizeBytes, long messageTtlMs) throws IOException {
        this.storagePath = Paths.get(storageDir);
        this.maxRetryCount = maxRetryCount;
        this.baseRetryDelayMs = baseRetryDelayMs;
        this.maxRetryDelayMs = maxRetryDelayMs;
        this.maxStorageSizeBytes = maxStorageSizeBytes;
        this.messageTtlMs = messageTtlMs;

        this.pendingMessages = new PriorityBlockingQueue<>();
        this.inFlightMessages = new ConcurrentHashMap<>();
        this.totalStored = new AtomicLong(0);
        this.totalForwarded = new AtomicLong(0);
        this.totalFailed = new AtomicLong(0);

        initializeStorage();
        loadPersistedMessages();
    }

    private void initializeStorage() throws IOException {
        if (!Files.exists(storagePath)) {
            Files.createDirectories(storagePath);
            logger.info("Created offline storage directory: {}", storagePath);
        }
        if (!Files.isDirectory(storagePath)) {
            throw new IOException("Storage path is not a directory: " + storagePath);
        }
    }

    private void loadPersistedMessages() {
        try {
            DirectoryStream<Path> stream = Files.newDirectoryStream(storagePath, "*.msg");
            for (Path file : stream) {
                try {
                    StoredMessage message = loadMessage(file);
                    if (message != null && !message.isExpired(messageTtlMs)) {
                        pendingMessages.offer(message);
                        totalStored.incrementAndGet();
                    }
                } catch (Exception e) {
                    logger.warn("Failed to load persisted message from {}", file, e);
                    try {
                        Files.delete(file);
                    } catch (IOException ex) {
                        logger.warn("Failed to delete corrupted message file {}", file, ex);
                    }
                }
            }
            logger.info("Loaded {} persisted messages from offline storage", pendingMessages.size());
        } catch (IOException e) {
            logger.error("Failed to load persisted messages", e);
        }
    }

    private StoredMessage loadMessage(Path file) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file.toFile()))) {
            return (StoredMessage) ois.readObject();
        }
    }

    private void persistMessage(StoredMessage message) throws IOException {
        Path file = storagePath.resolve(message.messageId + ".msg");
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file.toFile()))) {
            oos.writeObject(message);
            message.isPersisted = true;
        }
    }

    private void deletePersistedMessage(String messageId) {
        try {
            Path file = storagePath.resolve(messageId + ".msg");
            Files.deleteIfExists(file);
        } catch (IOException e) {
            logger.warn("Failed to delete persisted message {}", messageId, e);
        }
    }

    public synchronized boolean storeMessage(String payload, int priority) throws IOException {
        if (getCurrentStorageSize() >= maxStorageSizeBytes) {
            logger.warn("Offline storage is full, dropping message");
            return false;
        }

        String messageId = UUID.randomUUID().toString();
        StoredMessage message = new StoredMessage(messageId, payload, priority);

        persistMessage(message);
        pendingMessages.offer(message);
        totalStored.incrementAndGet();

        logger.debug("Stored message for offline delivery: {}, total pending: {}",
                messageId, pendingMessages.size());

        return true;
    }

    public List<StoredMessage> getMessagesForForwarding(int maxMessages) {
        List<StoredMessage> messages = new ArrayList<>();
        long now = System.currentTimeMillis();

        while (messages.size() < maxMessages && !pendingMessages.isEmpty()) {
            StoredMessage message = pendingMessages.peek();
            if (message == null) break;

            if (message.isExpired(messageTtlMs)) {
                pendingMessages.poll();
                deletePersistedMessage(message.messageId);
                totalFailed.incrementAndGet();
                logger.warn("Message expired, removing: {}", message.messageId);
                continue;
            }

            if (!message.shouldRetry(maxRetryCount)) {
                pendingMessages.poll();
                deletePersistedMessage(message.messageId);
                totalFailed.incrementAndGet();
                logger.warn("Message exceeded max retries, removing: {}", message.messageId);
                continue;
            }

            long nextRetryTime = message.lastRetryTimestampMs +
                    message.getNextRetryDelayMs(baseRetryDelayMs, maxRetryDelayMs);
            if (now < nextRetryTime && message.retryCount > 0) {
                break;
            }

            message = pendingMessages.poll();
            if (message != null) {
                message.incrementRetryCount();
                inFlightMessages.put(message.messageId, message);
                messages.add(message);
            }
        }

        return messages;
    }

    public synchronized void confirmDelivery(String messageId) {
        StoredMessage message = inFlightMessages.remove(messageId);
        if (message != null) {
            deletePersistedMessage(messageId);
            totalForwarded.incrementAndGet();
            logger.debug("Message delivery confirmed: {}", messageId);
        }
    }

    public synchronized void failDelivery(String messageId, boolean retry) {
        StoredMessage message = inFlightMessages.remove(messageId);
        if (message != null) {
            if (retry && message.shouldRetry(maxRetryCount) && !message.isExpired(messageTtlMs)) {
                pendingMessages.offer(message);
                logger.debug("Message marked for retry: {}, retryCount: {}", messageId, message.retryCount);
            } else {
                deletePersistedMessage(messageId);
                totalFailed.incrementAndGet();
                logger.warn("Message delivery failed permanently: {}", messageId);
            }
        }
    }

    public synchronized boolean hasPendingMessages() {
        return !pendingMessages.isEmpty() || !inFlightMessages.isEmpty();
    }

    public synchronized int getPendingMessageCount() {
        return pendingMessages.size();
    }

    public synchronized int getInFlightMessageCount() {
        return inFlightMessages.size();
    }

    private long getCurrentStorageSize() {
        try {
            long size = 0;
            DirectoryStream<Path> stream = Files.newDirectoryStream(storagePath, "*.msg");
            for (Path file : stream) {
                size += Files.size(file);
            }
            return size;
        } catch (IOException e) {
            logger.error("Failed to calculate storage size", e);
            return 0;
        }
    }

    public long getTotalStored() {
        return totalStored.get();
    }

    public long getTotalForwarded() {
        return totalForwarded.get();
    }

    public long getTotalFailed() {
        return totalFailed.get();
    }

    public double getStorageUtilization() {
        return (double) getCurrentStorageSize() / maxStorageSizeBytes;
    }

    public synchronized void cleanupExpiredMessages() {
        int removed = 0;
        Iterator<StoredMessage> iterator = pendingMessages.iterator();
        while (iterator.hasNext()) {
            StoredMessage message = iterator.next();
            if (message.isExpired(messageTtlMs)) {
                iterator.remove();
                deletePersistedMessage(message.messageId);
                totalFailed.incrementAndGet();
                removed++;
            }
        }
        if (removed > 0) {
            logger.info("Cleaned up {} expired messages", removed);
        }
    }

    public synchronized void close() {
        logger.info("Closing OfflineMessageStore. Stats: stored={}, forwarded={}, failed={}, pending={}",
                totalStored.get(), totalForwarded.get(), totalFailed.get(), pendingMessages.size());
    }
}
