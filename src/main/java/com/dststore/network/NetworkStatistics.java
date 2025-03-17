package com.dststore.network;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Encapsulates network statistics collection and reporting for the SimulatedNetwork.
 * This class tracks various metrics such as message counts by type, node, and outcome.
 */
public class NetworkStatistics {
    private static final Logger LOGGER = Logger.getLogger(NetworkStatistics.class.getName());

    // Node-specific statistics
    private final Map<String, Integer> messagesSentByNode;
    private final Map<String, Integer> messagesReceivedByNode;
    private final Map<String, Integer> messagesDroppedByNode;
    private final Map<String, Integer> messagesDelayedByNode;

    // Message type statistics
    private final Map<String, Integer> messagesByType;
    private final Map<String, Integer> droppedMessagesByType;

    // Lock for thread-safe statistics updates
    private final ReentrantReadWriteLock statsLock;

    /**
     * Creates a new NetworkStatistics instance.
     */
    public NetworkStatistics() {
        this.messagesSentByNode = new HashMap<>();
        this.messagesReceivedByNode = new HashMap<>();
        this.messagesDroppedByNode = new HashMap<>();
        this.messagesDelayedByNode = new HashMap<>();
        this.messagesByType = new ConcurrentHashMap<>();
        this.droppedMessagesByType = new ConcurrentHashMap<>();
        this.statsLock = new ReentrantReadWriteLock();
    }

    /**
     * Resets all statistics to zero.
     */
    public void reset() {
        statsLock.writeLock().lock();
        try {
            messagesSentByNode.clear();
            messagesReceivedByNode.clear();
            messagesDroppedByNode.clear();
            messagesDelayedByNode.clear();
            messagesByType.clear();
            droppedMessagesByType.clear();
        } finally {
            statsLock.writeLock().unlock();
        }
    }

    /**
     * Tracks a message being sent by a node.
     *
     * @param nodeId The ID of the sending node
     * @param messageType The type of the message
     */
    public void trackMessageSent(String nodeId, String messageType) {
        statsLock.writeLock().lock();
        try {
            incrementMessageCount(messagesSentByNode, nodeId);
            incrementMessageCount(messagesByType, messageType);
        } finally {
            statsLock.writeLock().unlock();
        }
    }

    /**
     * Tracks a message being received by a node.
     *
     * @param nodeId The ID of the receiving node
     * @param messageType The type of the message
     */
    public void trackMessageReceived(String nodeId, String messageType) {
        statsLock.writeLock().lock();
        try {
            incrementMessageCount(messagesReceivedByNode, nodeId);
        } finally {
            statsLock.writeLock().unlock();
        }
    }

    /**
     * Tracks a message being dropped.
     *
     * @param nodeId The ID of the node that would have received the message
     * @param messageType The type of the message
     */
    public void trackMessageDropped(String nodeId, String messageType) {
        statsLock.writeLock().lock();
        try {
            incrementMessageCount(messagesDroppedByNode, nodeId);
            incrementMessageCount(droppedMessagesByType, messageType);
        } finally {
            statsLock.writeLock().unlock();
        }
    }

    /**
     * Tracks a message being delayed.
     *
     * @param nodeId The ID of the node that will receive the delayed message
     * @param messageType The type of the message
     */
    public void trackMessageDelayed(String nodeId, String messageType) {
        statsLock.writeLock().lock();
        try {
            incrementMessageCount(messagesDelayedByNode, nodeId);
        } finally {
            statsLock.writeLock().unlock();
        }
    }

    /**
     * Increment the count for a specific key in the statistics map.
     *
     * @param statsMap The statistics map to update
     * @param key The key to increment
     */
    private void incrementMessageCount(Map<String, Integer> statsMap, String key) {
        statsMap.compute(key, (k, v) -> (v == null) ? 1 : v + 1);
    }

    /**
     * Gets comprehensive network statistics.
     *
     * @param pendingMessageQueueSize The current size of the pending message queue
     * @param networkConfig A map containing network configuration parameters
     * @return A map of statistics
     */
    public Map<String, Object> getStatistics(int pendingMessageQueueSize, Map<String, Object> networkConfig) {
        Map<String, Object> stats = new HashMap<>();

        statsLock.readLock().lock();
        try {
            // Message type statistics
            stats.put("messagesByType", new HashMap<>(messagesByType));
            stats.put("droppedMessagesByType", new HashMap<>(droppedMessagesByType));

            // Node-specific statistics
            stats.put("messagesSentByNode", new HashMap<>(messagesSentByNode));
            stats.put("messagesReceivedByNode", new HashMap<>(messagesReceivedByNode));
            stats.put("messagesDroppedByNode", new HashMap<>(messagesDroppedByNode));
            stats.put("messagesDelayedByNode", new HashMap<>(messagesDelayedByNode));

            // Aggregate statistics
            int totalMessages = messagesByType.values().stream().mapToInt(Integer::intValue).sum();
            int totalDropped = droppedMessagesByType.values().stream().mapToInt(Integer::intValue).sum();

            stats.put("totalMessages", totalMessages);
            stats.put("totalDropped", totalDropped);

            double dropRate = totalMessages > 0 ? (double) totalDropped / totalMessages : 0.0;
            stats.put("dropRate", dropRate);
        } finally {
            statsLock.readLock().unlock();
        }

        // Add network configuration
        stats.putAll(networkConfig);
        
        // Add queue information
        stats.put("pendingMessageQueueSize", pendingMessageQueueSize);
        
        return stats;
    }
} 