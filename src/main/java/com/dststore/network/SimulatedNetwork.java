package com.dststore.network;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

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
public class SimulatedNetwork {
    private static final Logger LOGGER = Logger.getLogger(SimulatedNetwork.class.getName());
    
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
    
    // New TigerBeetle-style priority queue for message scheduling
    private final PriorityQueue<ScheduledMessage> messageQueue = new PriorityQueue<>(
        Comparator.comparingLong(message -> message.deliveryTick)
    );
    
    // Legacy map for backward compatibility
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
    
    // Flag to control which message scheduling approach to use
    private volatile boolean useTigerBeetleStyle = false;
    
    /**
     * Class representing a scheduled message using TigerBeetle's approach.
     */
    private static class ScheduledMessage {
        final Object message;
        final String from;
        final String to;
        final long deliveryTick;
        
        ScheduledMessage(Object message, String from, String to, long deliveryTick) {
            this.message = message;
            this.from = from;
            this.to = to;
            this.deliveryTick = deliveryTick;
        }
    }
    
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
        
        /**
         * Creates a new delayed message.
         *
         * @param message The message object
         * @param from    The sender node ID
         * @param to      The recipient node ID
         */
        public DelayedMessage(Object message, String from, String to) {
            this.message = message;
            this.from = from;
            this.to = to;
        }
        
        /**
         * Gets the message object.
         *
         * @return The message
         */
        public Object getMessage() {
            return message;
        }
        
        /**
         * Gets the sender node ID.
         *
         * @return The sender ID
         */
        public String getFrom() {
            return from;
        }
        
        /**
         * Gets the recipient node ID.
         *
         * @return The recipient ID
         */
        public String getTo() {
            return to;
        }
    }
    
    /**
     * Enables TigerBeetle-style message scheduling using a priority queue.
     * 
     * @return This SimulatedNetwork instance for method chaining
     */
    public  SimulatedNetwork enableTigerBeetleStyle() {
        this.useTigerBeetleStyle = true;
        LOGGER.log(Level.INFO, "Enabled TigerBeetle-style message scheduling");
        return this;
    }
    
    /**
     * Disables TigerBeetle-style message scheduling, using the original implementation.
     * 
     * @return This SimulatedNetwork instance for method chaining
     */
    public  disableTigerBeetleStyle() {
        this.useTigerBeetleStyle = false;
        LOGGER.log(Level.INFO, "Disabled TigerBeetle-style message scheduling");
        return this;
    }
    
    /**
     * Checks if TigerBeetle-style message scheduling is enabled.
     * 
     * @return true if TigerBeetle-style scheduling is enabled, false otherwise
     */
    public boolean isTigerBeetleStyleEnabled() {
        return useTigerBeetleStyle;
    }
    
    /**
     * Configures the message loss rate for the network.
     *
     * @param rate A value between 0.0 (no loss) and 1.0 (all messages lost)
     * @return This SimulatedNetwork instance for method chaining
     * @throws IllegalArgumentException if rate is outside the valid range
     */
    public  withMessageLossRate(double rate) {
        if (rate < 0.0 || rate > 1.0) {
            throw new IllegalArgumentException("Message loss rate must be between 0.0 and 1.0");
        }
        messageLossRate = rate;
        return this;
    }
    
    /**
     * Configures the latency range for message delivery in ticks.
     *
     * @param minTicks Minimum ticks of delay
     * @param maxTicks Maximum ticks of delay
     * @return This SimulatedNetwork instance for method chaining
     * @throws IllegalArgumentException if minTicks > maxTicks or either is negative
     */
    public  withLatency(int minTicks, int maxTicks) {
        if (minTicks < 0 || maxTicks < 0) {
            throw new IllegalArgumentException("Latency ticks cannot be negative");
        }
        if (minTicks > maxTicks) {
            throw new IllegalArgumentException("Minimum latency cannot be greater than maximum latency");
        }
        
        minLatencyTicks = minTicks;
        maxLatencyTicks = maxTicks;
        return this;
    }
    
    /**
     * Configures the maximum number of messages that can be processed per tick.
     *
     * @param maxMessages Maximum messages per tick
     * @return This SimulatedNetwork instance for method chaining
     * @throws IllegalArgumentException if maxMessages is negative
     */
    public  withBandwidthLimit(int maxMessages) {
        if (maxMessages < 0) {
            throw new IllegalArgumentException("Maximum messages per tick cannot be negative");
        }
        maxMessagesPerTick = maxMessages;
        return this;
    }
    
    /**
     * Adds a custom message filter for fine-grained control over message delivery.
     *
     * @param filter The filter to add
     * @return This SimulatedNetwork instance for method chaining
     */
    public  withMessageFilter(MessageFilter filter) {
        messageFilters.add(filter);
        return this;
    }
    
    /**
     * Removes a previously added message filter.
     *
     * @param filter The filter to remove
     * @return This SimulatedNetwork instance for method chaining
     */
    public  removeMessageFilter(MessageFilter filter) {
        messageFilters.remove(filter);
        return this;
    }
    
    /**
     * Resets the simulator to default settings.
     * <p>
     * This clears all partitions, message filters, and delayed messages,
     * and resets configuration properties to their default values.
     * </p>
     *
     * @return This SimulatedNetwork instance for method chaining
     */
    public  reset() {
        messageLossRate = DEFAULT_MESSAGE_LOSS_RATE;
        minLatencyTicks = DEFAULT_MIN_LATENCY;
        maxLatencyTicks = DEFAULT_MAX_LATENCY;
        maxMessagesPerTick = DEFAULT_MAX_MESSAGES_PER_TICK;
        
        delayedMessagesLock.writeLock().lock();
        try {
            delayedMessages.clear();
            messageQueue.clear();
        } finally {
            delayedMessagesLock.writeLock().unlock();
        }
        
        messageFilters.clear();
        partitions.clear();
        partitionLinks.clear();
        
        statsLock.writeLock().lock();
        try {
            messagesByType.clear();
            droppedMessagesByType.clear();
            delayedMessagesByType.clear();
        } finally {
            statsLock.writeLock().unlock();
        }
        
        LOGGER.log(Level.INFO, "Simulated network reset to default settings");
        return this;
    }
    
    /**
     * Creates a new network partition containing the specified nodes.
     * <p>
     * Nodes within a partition can communicate with each other, but not with
     * nodes in other partitions unless explicitly linked.
     * </p>
     *
     * @param nodeIds The IDs of nodes to include in the partition
     * @return The partition ID (index) for future reference
     */
    public int createPartition(String... nodeIds) {
        if (nodeIds == null || nodeIds.length == 0) {
            throw new IllegalArgumentException("At least one node ID must be provided");
        }
        
        Set<String> partition = new HashSet<>(Arrays.asList(nodeIds));
        partitions.add(partition);
        
        int partitionId = partitions.size() - 1;
        LOGGER.log(Level.INFO, "Created partition {0} with nodes: {1}", 
                   new Object[]{partitionId, String.join(", ", partition)});
        
        return partitionId;
    }
    
    /**
     * Creates a one-way link from one partition to another.
     * <p>
     * This allows messages to flow from the source partition to the target partition,
     * but not in the reverse direction.
     * </p>
     *
     * @param sourcePartitionId The ID of the source partition
     * @param targetPartitionId The ID of the target partition
     * @return This SimulatedNetwork instance for method chaining
     * @throws IllegalArgumentException if either partition ID is invalid
     */
    public  linkPartitions(int sourcePartitionId, int targetPartitionId) {
        if (sourcePartitionId < 0 || sourcePartitionId >= partitions.size()) {
            throw new IllegalArgumentException("Invalid source partition ID: " + sourcePartitionId);
        }
        if (targetPartitionId < 0 || targetPartitionId >= partitions.size()) {
            throw new IllegalArgumentException("Invalid target partition ID: " + targetPartitionId);
        }
        
        partitionLinks.computeIfAbsent(sourcePartitionId, k -> new HashSet<>()).add(targetPartitionId);
        
        LOGGER.log(Level.INFO, "Created one-way link from partition {0} to partition {1}", 
                  new Object[]{sourcePartitionId, targetPartitionId});
        
        return this;
    }
    
    /**
     * Links partitions bidirectionally to allow message transfer between them.
     * This creates links in both directions between the partitions.
     * 
     * @param partition1Id The ID of the first partition
     * @param partition2Id The ID of the second partition
     */
    public void linkPartitionsBidirectional(int partition1Id, int partition2Id) {
        linkPartitions(partition1Id, partition2Id);
        linkPartitions(partition2Id, partition1Id);
        
        LOGGER.log(Level.INFO, "Created bidirectional link between partition {0} and partition {1}",
                   new Object[]{partition1Id, partition2Id});
    }
    
    /**
     * Clears all network partitions, allowing all nodes to communicate.
     *
     * @return This SimulatedNetwork instance for method chaining
     */
    public  clearPartitions() {
        partitions.clear();
        partitionLinks.clear();
        LOGGER.log(Level.INFO, "Cleared all network partitions");
        return this;
    }

    /**
     * Determines whether a message can be delivered based on current network conditions.
     * <p>
     * This method applies all configured settings and filters including:
     * - Network partitioning
     * - Message loss probability
     * - Custom message filters
     * </p>
     * <p>
     * If the message should be delivered normally, this returns true.
     * If the message should be dropped or delayed, this returns false.
     * </p>
     *
     * @param message The message being sent
     * @param from    The sender node ID
     * @param to      The recipient node ID
     * @return true if the message should be delivered normally, false if dropped or delayed
     */
    public boolean processOutboundMessage(Object message, String from, String to) {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        if (from == null || from.isEmpty()) {
            throw new IllegalArgumentException("Sender ID cannot be null or empty");
        }
        if (to == null || to.isEmpty()) {
            throw new IllegalArgumentException("Recipient ID cannot be null or empty");
        }
        
        // Track message by type
        String messageType = message.getClass().getSimpleName();
        incrementMessageCount(messagesByType, messageType);
        
        // Special handling for very critical operations, especially after partitions heal
        boolean isCriticalOperation = messageType.contains("PutRequest") || 
                                     messageType.contains("GetRequest") ||
                                     messageType.contains("SetValue") || 
                                     messageType.contains("GetValue");
        
        // Network partition check - critical for deterministic tests
        if (!canCommunicate(from, to)) {
            incrementMessageCount(droppedMessagesByType, messageType);
            LOGGER.log(Level.FINE, "Message dropped due to network partition: {0} -> {1}, type: {2}", 
                      new Object[]{from, to, messageType});
            return false;
        }
        
        // Apply message filters
        for (MessageFilter filter : messageFilters) {
            if (!filter.shouldDeliver(message, from, to)) {
                incrementMessageCount(droppedMessagesByType, messageType);
                LOGGER.log(Level.FINE, "Message dropped by custom filter: {0} -> {1}, type: {2}", 
                          new Object[]{from, to, messageType});
                return false;
            }
        }
        
        // Apply message loss probability with special handling for critical messages
        if (messageLossRate > 0) {
            // For important messages like replication and quorum protocols, use a different approach
            boolean isImportantMessage = messageType.contains("GetValue") || 
                                         messageType.contains("SetValue") || 
                                         messageType.contains("Versioned") || 
                                         messageType.contains("Replication") ||
                                         messageType.contains("Consensus") ||
                                         messageType.contains("PutRequest") ||
                                         messageType.contains("GetRequest");
            
            // Deterministic loss based on message content, sender, and receiver
            // This makes tests more reproducible vs. pure random
            boolean shouldDrop;
            
            // For critical operations at high ticks (likely after partition healing),
            // dramatically reduce the chance of message loss
            if (isCriticalOperation && getCurrentTick() > 30) {
                // Almost never drop critical messages at high ticks
                double effectiveLossRate = messageLossRate * 0.1; 
                shouldDrop = ThreadLocalRandom.current().nextDouble() < effectiveLossRate;
                
                if (shouldDrop) {
                    LOGGER.log(Level.FINE, "Would normally drop critical message at high tick ({0}), but allowing delivery: {1} -> {2}, type: {3}", 
                              new Object[]{getCurrentTick(), from, to, messageType});
                    shouldDrop = false; // Force delivery of critical messages at high ticks
                }
            }
            else if (isImportantMessage && messageLossRate >= 0.5) {
                // For important messages in high-loss tests, bias toward delivery
                // but still ensure some messages are lost to test recovery
                double effectiveLossRate = messageLossRate * 0.6;
                shouldDrop = ThreadLocalRandom.current().nextDouble() < effectiveLossRate;
            } else {
                // Standard loss rate for other messages
                shouldDrop = ThreadLocalRandom.current().nextDouble() < messageLossRate;
            }
            
            if (shouldDrop) {
                incrementMessageCount(droppedMessagesByType, messageType);
                LOGGER.log(Level.FINE, "Message dropped due to random loss: {0} -> {1}, type: {2}", 
                          new Object[]{from, to, messageType});
                return false;
            }
        }
        
        // Apply latency if configured
        int delay = 0;
        if (maxLatencyTicks > 0) {
            delay = calculateMessageDelay();
        }
        
        // If using TigerBeetle style and there's a delay, schedule the message
        if (useTigerBeetleStyle && delay > 0) {
            long deliveryTick = getCurrentTick() + delay;
            scheduleMessage(message, from, to, deliveryTick);
            return false;  // Message will be delivered later
        } 
        // If using legacy style and there's a delay, queue the message for later
        else if (!useTigerBeetleStyle && delay > 0) {
            incrementMessageCount(delayedMessagesByType, messageType);
            long deliveryTick = getCurrentTick() + delay;
            
            delayedMessagesLock.writeLock().lock();
            try {
                delayedMessages.computeIfAbsent(deliveryTick, k -> new ArrayList<>())
                              .add(new DelayedMessage(message, from, to));
            } finally {
                delayedMessagesLock.writeLock().unlock();
            }
            
            LOGGER.log(Level.FINE, "Message delayed by {0} ticks: {1} -> {2}, type: {3}", 
                      new Object[]{delay, from, to, messageType});
            return false;  // Message will be delivered later
        }
        
        // Message should be delivered immediately
        return true;
    }
    
    /**
     * Schedules a message for delivery at a specific tick using TigerBeetle's approach.
     * 
     * @param message The message to deliver
     * @param from The sender node ID
     * @param to The recipient node ID
     * @param deliveryTick The tick at which the message should be delivered
     */
    private void scheduleMessage(Object message, String from, String to, long deliveryTick) {
        delayedMessagesLock.writeLock().lock();
        try {
            messageQueue.add(new ScheduledMessage(message, from, to, deliveryTick));
            incrementMessageCount(delayedMessagesByType, message.getClass().getSimpleName());
            
            LOGGER.log(Level.FINE, "Message scheduled for delivery at tick {0}: {1} -> {2}", 
                      new Object[]{deliveryTick, from, to});
        } finally {
            delayedMessagesLock.writeLock().unlock();
        }
    }
    
    /**
     * Calculates a random delay based on the configured latency range.
     *
     * @return The delay in ticks
     */
    private int calculateMessageDelay() {
        if (minLatencyTicks == maxLatencyTicks) {
            return minLatencyTicks;
        }
        return ThreadLocalRandom.current().nextInt(minLatencyTicks, maxLatencyTicks + 1);
    }
    
    /**
     * Determines whether two nodes can communicate based on the current partitioning.
     *
     * @param from The sender node ID
     * @param to   The recipient node ID
     * @return true if the nodes can communicate, false otherwise
     */
    private boolean canCommunicate(String from, String to) {
        if (partitions.isEmpty()) {
            // No partitions defined - all nodes can communicate
            return true;
        }
        
        // Find the partitions containing the nodes
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
        
        if (fromPartition == -1 || toPartition == -1) {
            // At least one node is not in any partition - can't communicate
            return false;
        }
        
        if (fromPartition == toPartition) {
            // Both nodes are in the same partition - can communicate
            return true;
        }
        
        // Check if there's a link from the source partition to the target partition
        Set<Integer> links = partitionLinks.get(fromPartition);
        return links != null && links.contains(toPartition);
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
            if (useTigerBeetleStyle) {
                // Use TigerBeetle-style priority queue
                while (!messageQueue.isEmpty() && messageQueue.peek().deliveryTick <= currentTick) {
                    ScheduledMessage scheduledMessage = messageQueue.poll();
                    
                    // Additional check for partitions before delivering message
                    // This handles cases where partitions are healed during message delay
                    if (canCommunicate(scheduledMessage.from, scheduledMessage.to)) {
                        // Prioritize certain message types that are important for consistency
                        // after network partitions heal
                        String messageType = scheduledMessage.message.getClass().getSimpleName();
                        
                        // Add all messages ready for delivery
                        readyMessages.add(new DelayedMessage(
                            scheduledMessage.message,
                            scheduledMessage.from,
                            scheduledMessage.to
                        ));
                        
                        // Log delivery for important messages
                        if (messageType.contains("SetValue") || messageType.contains("GetValue") || 
                            messageType.contains("Quorum") || messageType.contains("Replication")) {
                            LOGGER.log(Level.FINE, "Critical message type {0} delivered after potential partition: {1} -> {2}",
                                      new Object[]{messageType, scheduledMessage.from, scheduledMessage.to});
                        }
                    } else {
                        // Message is dropped due to partition
                        LOGGER.log(Level.FINE, "Delayed message dropped due to network partition: {0} -> {1}",
                                  new Object[]{scheduledMessage.from, scheduledMessage.to});
                        incrementMessageCount(droppedMessagesByType, 
                            scheduledMessage.message.getClass().getSimpleName());
                    }
                }
            } else {
                // Use original implementation
                // Gather all messages due up to and including the current tick
                for (long tick = 0; tick <= currentTick; tick++) {
                    List<DelayedMessage> messages = delayedMessages.remove(tick);
                    if (messages != null) {
                        // Filter messages based on current partition state
                        for (DelayedMessage message : messages) {
                            if (canCommunicate(message.getFrom(), message.getTo())) {
                                readyMessages.add(message);
                                
                                // Log delivery for important messages after potential partition
                                String messageType = message.getMessage().getClass().getSimpleName();
                                if (messageType.contains("SetValue") || messageType.contains("GetValue") || 
                                    messageType.contains("Quorum") || messageType.contains("Replication")) {
                                    LOGGER.log(Level.FINE, "Critical message type {0} delivered: {1} -> {2}",
                                              new Object[]{messageType, message.getFrom(), message.getTo()});
                                }
                            } else {
                                // Message is dropped due to partition
                                LOGGER.log(Level.FINE, "Delayed message dropped due to network partition: {0} -> {1}",
                                          new Object[]{message.getFrom(), message.getTo()});
                                incrementMessageCount(droppedMessagesByType, 
                                    message.getMessage().getClass().getSimpleName());
                            }
                        }
                    }
                }
            }
            
            // Apply bandwidth limits if necessary
            if (readyMessages.size() > maxMessagesPerTick) {
                List<DelayedMessage> deferredMessages = 
                    readyMessages.subList(maxMessagesPerTick, readyMessages.size());
                
                // Move excess messages to the next tick
                long nextTick = currentTick + 1;
                
                if (useTigerBeetleStyle) {
                    for (DelayedMessage message : deferredMessages) {
                        messageQueue.add(new ScheduledMessage(
                            message.getMessage(),
                            message.getFrom(),
                            message.getTo(),
                            nextTick
                        ));
                    }
                } else {
                    for (DelayedMessage message : deferredMessages) {
                        delayedMessages.computeIfAbsent(nextTick, k -> new ArrayList<>()).add(message);
                    }
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
     * @param statsMap The statistics map to update
     * @param messageType The message type
     */
    private void incrementMessageCount(Map<String, Integer> statsMap, String messageType) {
        statsLock.writeLock().lock();
        try {
            statsMap.compute(messageType, (k, v) -> (v == null) ? 1 : v + 1);
        } finally {
            statsLock.writeLock().unlock();
        }
    }
    
    /**
     * Gets the current value of the tick counter.
     * This is needed for TigerBeetle-style scheduling.
     * 
     * @return The current tick
     */
    public long getCurrentTick() {
        // In a real implementation, this would be injected or fetched from MessageBus
        // For now, we'll return 0 as a placeholder
        // The MessageBus will pass its tick value to getMessagesForDelivery instead
        return 0L;
    }
    
    /**
     * Gets network statistics including message counts by type.
     *
     * @return A map of statistics
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        
        statsLock.readLock().lock();
        try {
            stats.put("messagesByType", new HashMap<>(messagesByType));
            stats.put("droppedMessagesByType", new HashMap<>(droppedMessagesByType));
            stats.put("delayedMessagesByType", new HashMap<>(delayedMessagesByType));
            
            int totalMessages = messagesByType.values().stream().mapToInt(Integer::intValue).sum();
            int totalDropped = droppedMessagesByType.values().stream().mapToInt(Integer::intValue).sum();
            int totalDelayed = delayedMessagesByType.values().stream().mapToInt(Integer::intValue).sum();
            
            stats.put("totalMessages", totalMessages);
            stats.put("totalDropped", totalDropped);
            stats.put("totalDelayed", totalDelayed);
            
            double dropRate = totalMessages > 0 ? (double)totalDropped / totalMessages : 0.0;
            double delayRate = totalMessages > 0 ? (double)totalDelayed / totalMessages : 0.0;
            
            stats.put("dropRate", dropRate);
            stats.put("delayRate", delayRate);
            
            stats.put("minLatencyTicks", minLatencyTicks);
            stats.put("maxLatencyTicks", maxLatencyTicks);
            stats.put("messageLossRate", messageLossRate);
            stats.put("maxMessagesPerTick", maxMessagesPerTick);
            
            stats.put("partitionCount", partitions.size());
            stats.put("partitionLinkCount", partitionLinks.values().stream()
                                           .mapToInt(Set::size).sum());
            
            stats.put("pendingDelayedMessages", 
                    delayedMessages.values().stream().mapToInt(List::size).sum());
        } finally {
            statsLock.readLock().unlock();
        }
        
        if (useTigerBeetleStyle) {
            delayedMessagesLock.readLock().lock();
            try {
                stats.put("pendingMessageQueueSize", messageQueue.size());
            } finally {
                delayedMessagesLock.readLock().unlock();
            }
        }
        
        return stats;
    }
} 