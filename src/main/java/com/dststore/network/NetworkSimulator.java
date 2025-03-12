package com.dststore.network;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Simulates network conditions for deterministic testing of distributed systems.
 * <p>
 * This class provides the ability to simulate various network conditions including:
 * - Network partitions (nodes that cannot communicate with each other)
 * - Message loss (randomly dropping messages based on a probability)
 * - Message delays (adding latency to message delivery)
 * - Bandwidth limitations (restricting the number of messages processed per tick)
 * - Custom message filtering (for targeted message manipulation)
 * </p>
 * <p>
 * The simulator uses a tick-based approach for time simulation, allowing for
 * deterministic and reproducible tests. It works in conjunction with MessageBus
 * to provide controlled network behavior for testing distributed system components.
 * </p>
 * <p>
 * Example usage:
 * <pre>
 * // Create a message bus with network simulation
 * MessageBus bus = new MessageBus();
 * NetworkSimulator simulator = bus.enableNetworkSimulation();
 * 
 * // Configure network conditions
 * simulator.withMessageLossRate(0.1)     // 10% message loss
 *          .withLatency(2, 5)            // 2-5 ticks of latency
 *          .withBandwidthLimit(5);       // Process max 5 messages per tick
 * 
 * // Create network partitions
 * int partition1 = simulator.createPartition("node1", "node2"); 
 * int partition2 = simulator.createPartition("node3", "node4");
 * 
 * // Allow one-way communication between partitions
 * simulator.linkPartitions(partition1, partition2);
 * </pre>
 * </p>
 */
public class NetworkSimulator {
    private static final Logger LOGGER = Logger.getLogger(NetworkSimulator.class.getName());
    
    // Default values
    private static final double DEFAULT_MESSAGE_LOSS_RATE = 0.0;
    private static final int DEFAULT_MIN_LATENCY = 0;
    private static final int DEFAULT_MAX_LATENCY = 0;
    private static final int DEFAULT_MAX_MESSAGES_PER_TICK = Integer.MAX_VALUE;
    
    // Configuration
    private volatile double messageLossRate = DEFAULT_MESSAGE_LOSS_RATE;
    private volatile int minLatencyTicks = DEFAULT_MIN_LATENCY;
    private volatile int maxLatencyTicks = DEFAULT_MAX_LATENCY;
    private volatile int maxMessagesPerTick = DEFAULT_MAX_MESSAGES_PER_TICK;
    
    // Thread safety for message handling
    private final ReentrantReadWriteLock delayedMessagesLock = new ReentrantReadWriteLock();
    private final Map<Long, List<DelayedMessage>> delayedMessages = new HashMap<>();
    
    // Message filtering
    private final List<MessageFilter> messageFilters = new CopyOnWriteArrayList<>();
    
    // Network partitioning
    private final List<Set<String>> partitions = new CopyOnWriteArrayList<>();
    private final Map<Integer, Set<Integer>> partitionLinks = new ConcurrentHashMap<>();
    
    // Statistics
    private final Map<String, Integer> messagesByType = new ConcurrentHashMap<>();
    private final Map<String, Integer> droppedMessagesByType = new ConcurrentHashMap<>();
    private final Map<String, Integer> delayedMessagesByType = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock statsLock = new ReentrantReadWriteLock();
    
    /**
     * Interface for custom message filtering logic.
     */
    @FunctionalInterface
    public interface MessageFilter {
        /**
         * Determines whether a message should be delivered.
         *
         * @param message The message being sent
         * @param from    The sender node ID
         * @param to      The recipient node ID
         * @return true if the message should be delivered, false to drop it
         */
        boolean shouldDeliver(Object message, String from, String to);
    }
    
    /**
     * Class representing a message that has been delayed for future delivery.
     */
    public static class DelayedMessage {
        private final Object message;
        private final String from;
        private final String to;
        private final long deliveryTick;
        
        /**
         * Creates a new delayed message.
         *
         * @param message      The message content
         * @param from         The sender node ID
         * @param to           The recipient node ID
         * @param deliveryTick The tick at which this message should be delivered
         */
        public DelayedMessage(Object message, String from, String to, long deliveryTick) {
            this.message = Objects.requireNonNull(message, "Message cannot be null");
            this.from = Objects.requireNonNull(from, "Sender ID cannot be null");
            this.to = Objects.requireNonNull(to, "Recipient ID cannot be null");
            this.deliveryTick = deliveryTick;
        }
        
        /**
         * Gets the message content.
         *
         * @return The message object
         */
        public Object getMessage() {
            return message;
        }
        
        /**
         * Gets the sender ID.
         *
         * @return The ID of the sending node
         */
        public String getFrom() {
            return from;
        }
        
        /**
         * Gets the recipient ID.
         *
         * @return The ID of the receiving node
         */
        public String getTo() {
            return to;
        }
        
        /**
         * Gets the scheduled delivery tick.
         *
         * @return The tick at which this message should be delivered
         */
        public long getDeliveryTick() {
            return deliveryTick;
        }
        
        @Override
        public String toString() {
            return "DelayedMessage{" +
                "from='" + from + '\'' +
                ", to='" + to + '\'' +
                ", deliveryTick=" + deliveryTick +
                ", messageType=" + (message != null ? message.getClass().getSimpleName() : "null") +
                '}';
        }
    }
    
    /**
     * Sets the message loss rate for the simulator.
     * 
     * @param rate The probability (0.0 to 1.0) that a message will be dropped
     * @return This NetworkSimulator instance for method chaining
     * @throws IllegalArgumentException If rate is less than 0 or greater than 1
     */
    public synchronized NetworkSimulator withMessageLossRate(double rate) {
        if (rate < 0.0 || rate > 1.0) {
            throw new IllegalArgumentException("Message loss rate must be between 0.0 and 1.0");
        }
        this.messageLossRate = rate;
        LOGGER.log(Level.INFO, "Set message loss rate to {0}", rate);
        return this;
    }
    
    /**
     * Sets the latency range for messages.
     * 
     * @param minTicks Minimum number of ticks to delay a message
     * @param maxTicks Maximum number of ticks to delay a message
     * @return This NetworkSimulator instance for method chaining
     * @throws IllegalArgumentException If minTicks is negative or maxTicks is less than minTicks
     */
    public synchronized NetworkSimulator withLatency(int minTicks, int maxTicks) {
        if (minTicks < 0) {
            throw new IllegalArgumentException("Minimum latency cannot be negative");
        }
        if (maxTicks < minTicks) {
            throw new IllegalArgumentException("Maximum latency cannot be less than minimum latency");
        }
        this.minLatencyTicks = minTicks;
        this.maxLatencyTicks = maxTicks;
        LOGGER.log(Level.INFO, "Set latency range to {0}-{1} ticks", new Object[]{minTicks, maxTicks});
        return this;
    }
    
    /**
     * Sets the maximum number of messages that can be processed per tick.
     * 
     * @param maxMessages Maximum number of messages
     * @return This NetworkSimulator instance for method chaining
     * @throws IllegalArgumentException If maxMessages is less than or equal to 0
     */
    public synchronized NetworkSimulator withBandwidthLimit(int maxMessages) {
        if (maxMessages <= 0) {
            throw new IllegalArgumentException("Maximum messages per tick must be positive");
        }
        this.maxMessagesPerTick = maxMessages;
        LOGGER.log(Level.INFO, "Set bandwidth limit to {0} messages per tick", maxMessages);
        return this;
    }
    
    /**
     * Adds a custom message filter to the simulator.
     * 
     * @param filter The filter to add
     * @return This NetworkSimulator instance for method chaining
     * @throws NullPointerException If filter is null
     */
    public NetworkSimulator withMessageFilter(MessageFilter filter) {
        Objects.requireNonNull(filter, "Message filter cannot be null");
        messageFilters.add(filter);
        LOGGER.log(Level.INFO, "Added message filter: {0}", filter.getClass().getName());
        return this;
    }
    
    /**
     * Creates a new network partition containing the specified nodes.
     * Nodes in different partitions cannot communicate unless their partitions are linked.
     * 
     * @param nodeIds The IDs of nodes to include in this partition
     * @return The partition ID (index)
     * @throws IllegalArgumentException If nodeIds is empty
     */
    public synchronized int createPartition(String... nodeIds) {
        if (nodeIds == null || nodeIds.length == 0) {
            throw new IllegalArgumentException("Partition must contain at least one node");
        }
        
        Set<String> partition = new HashSet<>(Arrays.asList(nodeIds));
        partitions.add(partition);
        int partitionId = partitions.size() - 1;
        
        LOGGER.log(Level.INFO, "Created partition {0} with nodes: {1}", 
                  new Object[]{partitionId, String.join(", ", partition)});
        return partitionId;
    }
    
    /**
     * Establishes a one-way link from one partition to another, allowing messages
     * to flow from nodes in partition1 to nodes in partition2.
     * 
     * @param partition1 The source partition ID
     * @param partition2 The destination partition ID
     * @return This NetworkSimulator instance for method chaining
     * @throws IllegalArgumentException If either partition ID is invalid
     */
    public synchronized NetworkSimulator linkPartitions(int partition1, int partition2) {
        if (partition1 < 0 || partition1 >= partitions.size()) {
            throw new IllegalArgumentException("Invalid source partition ID: " + partition1);
        }
        if (partition2 < 0 || partition2 >= partitions.size()) {
            throw new IllegalArgumentException("Invalid destination partition ID: " + partition2);
        }
        
        partitionLinks.computeIfAbsent(partition1, k -> new HashSet<>()).add(partition2);
        LOGGER.log(Level.INFO, "Linked partition {0} to partition {1}", new Object[]{partition1, partition2});
        return this;
    }
    
    /**
     * Clears all network partitions and links.
     * 
     * @return This NetworkSimulator instance for method chaining
     */
    public synchronized NetworkSimulator clearPartitions() {
        partitions.clear();
        partitionLinks.clear();
        LOGGER.log(Level.INFO, "Cleared all network partitions and links");
        return this;
    }
    
    /**
     * Removes a node from all partitions.
     * 
     * @param nodeId The ID of the node to remove
     * @return This NetworkSimulator instance for method chaining
     */
    public synchronized NetworkSimulator removeNodeFromPartitions(String nodeId) {
        for (Set<String> partition : partitions) {
            partition.remove(nodeId);
        }
        LOGGER.log(Level.INFO, "Removed node {0} from all partitions", nodeId);
        return this;
    }
    
    /**
     * Adds a node to a specific partition.
     * 
     * @param partitionId The ID of the partition
     * @param nodeId The ID of the node to add
     * @return This NetworkSimulator instance for method chaining
     * @throws IllegalArgumentException If the partition ID is invalid
     */
    public synchronized NetworkSimulator addNodeToPartition(int partitionId, String nodeId) {
        if (partitionId < 0 || partitionId >= partitions.size()) {
            throw new IllegalArgumentException("Invalid partition ID: " + partitionId);
        }
        
        partitions.get(partitionId).add(nodeId);
        LOGGER.log(Level.INFO, "Added node {0} to partition {1}", new Object[]{nodeId, partitionId});
        return this;
    }
    
    /**
     * Determines if two nodes can communicate based on the current partition configuration.
     * 
     * @param from The sender node ID
     * @param to The recipient node ID
     * @return true if communication is allowed, false otherwise
     */
    private boolean canCommunicate(String from, String to) {
        // If no partitions exist, all nodes can communicate
        if (partitions.isEmpty()) {
            return true;
        }
        
        // Find which partitions the nodes belong to
        int fromPartition = -1;
        int toPartition = -1;
        
        for (int i = 0; i < partitions.size(); i++) {
            Set<String> partition = partitions.get(i);
            if (partition.contains(from)) {
                fromPartition = i;
            }
            if (partition.contains(to)) {
                toPartition = i;
            }
        }
        
        // If either node is not in any partition, they can communicate
        if (fromPartition == -1 || toPartition == -1) {
            return true;
        }
        
        // If they're in the same partition, they can communicate
        if (fromPartition == toPartition) {
            return true;
        }
        
        // Check if there's a link from the sender's partition to the receiver's partition
        Set<Integer> links = partitionLinks.get(fromPartition);
        return links != null && links.contains(toPartition);
    }
    
    /**
     * Processes an outbound message according to the current network conditions.
     * 
     * @param message The message being sent
     * @param from The sender node ID
     * @param to The recipient node ID
     * @return true if the message should be delivered immediately, false if it was dropped or delayed
     * @throws NullPointerException If any parameter is null
     */
    public boolean processOutboundMessage(Object message, String from, String to) {
        Objects.requireNonNull(message, "Message cannot be null");
        Objects.requireNonNull(from, "Sender cannot be null");
        Objects.requireNonNull(to, "Recipient cannot be null");
        
        // Track message statistics
        String messageType = message.getClass().getSimpleName();
        incrementMessageCount(messageType);
        
        // Check if communication is allowed based on partitions
        if (!canCommunicate(from, to)) {
            LOGGER.log(Level.FINE, "Message from {0} to {1} dropped due to network partition", 
                      new Object[]{from, to});
            incrementDroppedMessageCount(messageType);
            return false;
        }
        
        // Apply custom message filters
        for (MessageFilter filter : messageFilters) {
            try {
                if (!filter.shouldDeliver(message, from, to)) {
                    LOGGER.log(Level.FINE, "Message from {0} to {1} dropped by filter: {2}", 
                              new Object[]{from, to, filter.getClass().getSimpleName()});
                    incrementDroppedMessageCount(messageType);
                    return false;
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error in message filter: " + e.getMessage(), e);
                // Continue with other filters even if one fails
            }
        }
        
        // Apply random message loss
        if (messageLossRate > 0 && ThreadLocalRandom.current().nextDouble() < messageLossRate) {
            LOGGER.log(Level.FINE, "Message from {0} to {1} dropped due to simulated network loss", 
                      new Object[]{from, to});
            incrementDroppedMessageCount(messageType);
            return false;
        }
        
        // Apply latency if configured
        if (maxLatencyTicks > 0) {
            int latency = minLatencyTicks;
            if (maxLatencyTicks > minLatencyTicks) {
                latency = ThreadLocalRandom.current().nextInt(minLatencyTicks, maxLatencyTicks + 1);
            }
            
            if (latency > 0) {
                DelayedMessage delayedMessage = new DelayedMessage(message, from, to, System.currentTimeMillis() + latency);
                addDelayedMessage(delayedMessage, latency);
                incrementDelayedMessageCount(messageType);
                LOGGER.log(Level.FINE, "Message from {0} to {1} delayed by {2} ticks", 
                          new Object[]{from, to, latency});
                return false;
            }
        }
        
        // Message should be delivered immediately
        return true;
    }
    
    /**
     * Adds a message to the delayed message queue.
     * 
     * @param message The message to delay
     * @param delayTicks The number of ticks to delay the message
     */
    private void addDelayedMessage(DelayedMessage message, int delayTicks) {
        delayedMessagesLock.writeLock().lock();
        try {
            long deliveryTick = delayTicks;
            delayedMessages.computeIfAbsent(deliveryTick, k -> new ArrayList<>()).add(message);
        } finally {
            delayedMessagesLock.writeLock().unlock();
        }
    }
    
    /**
     * Gets all messages ready for delivery at the current tick.
     * 
     * @param currentTick The current simulation tick
     * @return A list of delayed messages ready for delivery
     */
    public List<DelayedMessage> getMessagesForDelivery(long currentTick) {
        List<DelayedMessage> readyMessages = new ArrayList<>();
        
        delayedMessagesLock.writeLock().lock();
        try {
            // Gather all messages due up to and including the current tick
            for (long tick = 0; tick <= currentTick; tick++) {
                List<DelayedMessage> messages = delayedMessages.remove(tick);
                if (messages != null) {
                    readyMessages.addAll(messages);
                }
            }
            
            // Apply bandwidth limits if necessary
            if (readyMessages.size() > maxMessagesPerTick) {
                List<DelayedMessage> deferredMessages = 
                    readyMessages.subList(maxMessagesPerTick, readyMessages.size());
                
                // Move excess messages to the next tick
                for (DelayedMessage message : deferredMessages) {
                    long nextTick = currentTick + 1;
                    delayedMessages.computeIfAbsent(nextTick, k -> new ArrayList<>()).add(message);
                }
                
                // Keep only the messages within the bandwidth limit
                readyMessages = readyMessages.subList(0, maxMessagesPerTick);
                
                LOGGER.log(Level.FINE, "Bandwidth limit applied: {0} messages deferred to next tick", 
                          deferredMessages.size());
            }
        } finally {
            delayedMessagesLock.writeLock().unlock();
        }
        
        if (!readyMessages.isEmpty()) {
            LOGGER.log(Level.FINE, "Retrieved {0} messages ready for delivery at tick {1}", 
                      new Object[]{readyMessages.size(), currentTick});
        }
        
        return readyMessages;
    }
    
    /**
     * Increment the count for a specific message type in the statistics.
     * 
     * @param messageType The type of message
     */
    private void incrementMessageCount(String messageType) {
        statsLock.writeLock().lock();
        try {
            messagesByType.put(messageType, messagesByType.getOrDefault(messageType, 0) + 1);
        } finally {
            statsLock.writeLock().unlock();
        }
    }
    
    /**
     * Increment the count for a dropped message type in the statistics.
     * 
     * @param messageType The type of message
     */
    private void incrementDroppedMessageCount(String messageType) {
        statsLock.writeLock().lock();
        try {
            droppedMessagesByType.put(messageType, droppedMessagesByType.getOrDefault(messageType, 0) + 1);
        } finally {
            statsLock.writeLock().unlock();
        }
    }
    
    /**
     * Increment the count for a delayed message type in the statistics.
     * 
     * @param messageType The type of message
     */
    private void incrementDelayedMessageCount(String messageType) {
        statsLock.writeLock().lock();
        try {
            delayedMessagesByType.put(messageType, delayedMessagesByType.getOrDefault(messageType, 0) + 1);
        } finally {
            statsLock.writeLock().unlock();
        }
    }
    
    /**
     * Gets statistics about message processing.
     * 
     * @return A map of statistics
     */
    public Map<String, Object> getStatistics() {
        statsLock.readLock().lock();
        try {
            Map<String, Object> stats = new HashMap<>();
            stats.put("messageLossRate", messageLossRate);
            stats.put("minLatencyTicks", minLatencyTicks);
            stats.put("maxLatencyTicks", maxLatencyTicks);
            stats.put("maxMessagesPerTick", maxMessagesPerTick);
            stats.put("messagesByType", new HashMap<>(messagesByType));
            stats.put("droppedMessagesByType", new HashMap<>(droppedMessagesByType));
            stats.put("delayedMessagesByType", new HashMap<>(delayedMessagesByType));
            stats.put("activePartitions", partitions.size());
            stats.put("partitionLinks", new HashMap<>(partitionLinks));
            
            // Count pending messages
            delayedMessagesLock.readLock().lock();
            try {
                stats.put("pendingMessages", delayedMessages.values().stream()
                    .mapToInt(List::size)
                    .sum());
                
                // Group pending messages by type
                Map<String, Long> pendingByType = delayedMessages.values().stream()
                    .flatMap(List::stream)
                    .collect(Collectors.groupingBy(
                        msg -> msg.getMessage().getClass().getSimpleName(),
                        Collectors.counting()
                    ));
                stats.put("pendingMessagesByType", pendingByType);
            } finally {
                delayedMessagesLock.readLock().unlock();
            }
            
            return stats;
        } finally {
            statsLock.readLock().unlock();
        }
    }
    
    /**
     * Resets all statistics counters.
     * 
     * @return This NetworkSimulator instance for method chaining
     */
    public synchronized NetworkSimulator resetStatistics() {
        statsLock.writeLock().lock();
        try {
            messagesByType.clear();
            droppedMessagesByType.clear();
            delayedMessagesByType.clear();
        } finally {
            statsLock.writeLock().unlock();
        }
        LOGGER.log(Level.INFO, "Network simulator statistics reset");
        return this;
    }
    
    /**
     * Reset all network conditions to default values.
     * 
     * @return This NetworkSimulator instance for method chaining
     */
    public synchronized NetworkSimulator reset() {
        delayedMessagesLock.writeLock().lock();
        try {
            delayedMessages.clear();
        } finally {
            delayedMessagesLock.writeLock().unlock();
        }
        
        messageLossRate = DEFAULT_MESSAGE_LOSS_RATE;
        minLatencyTicks = DEFAULT_MIN_LATENCY;
        maxLatencyTicks = DEFAULT_MAX_LATENCY;
        maxMessagesPerTick = DEFAULT_MAX_MESSAGES_PER_TICK;
        
        messageFilters.clear();
        partitions.clear();
        partitionLinks.clear();
        
        resetStatistics();
        
        LOGGER.log(Level.INFO, "Network simulator reset to default state");
        return this;
    }
} 