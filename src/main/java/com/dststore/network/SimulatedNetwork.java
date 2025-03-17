package com.dststore.network;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates a network with configurable conditions like message loss, latency, and partitioning.
 * 
 * TODO: Revisit the network partition design:
 * 1. The current implementation maintains partition identities even after bidirectional linking
 * 2. Consider if merging partitions would better represent network topology changes
 * 3. Evaluate if the link-based approach accurately simulates real network partitions
 * 4. Consider adding explicit partition merge/split operations for more realistic simulation
 * <p>
 * This class provides the ability to simulate various network conditions including:
 * - Network partitions (nodes that cannot communicate with each other)
 * - Message loss (randomly dropping messages based on a probability)
 * - Message delays (adding latency to message delivery)
 * - Bandwidth limitations (restricting the number of messages processed per tick)
 * </p>
 * <p>
 * The simulator uses a tick-based approach for time simulation, allowing for
 * deterministic and reproducible tests.
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
    private final ReentrantReadWriteLock messagesLock = new ReentrantReadWriteLock();

    // Current simulation tick
    private long currentTick = 0;
    
    // Message sequence counter for FIFO ordering when messages have the same delivery tick
    private final AtomicLong messageSequence = new AtomicLong(0);
    
    // Priority queue for message scheduling
    private final PriorityBlockingQueue<ScheduledMessage> messageQueue = new PriorityBlockingQueue<>(
            100, Comparator.<ScheduledMessage>comparingLong(message -> message.deliveryTick)
                .thenComparingLong(message -> message.sequenceNumber));

    // Message filtering
    private final List<MessageFilter> messageFilters;
    private final Map<String, Integer> messagesSentByNode;
    private final Map<String, Integer> messagesReceivedByNode;
    private final Map<String, Integer> messagesDroppedByNode;
    private final Map<String, Integer> messagesDelayedByNode;
    private final Random random;

    // Statistics
    private final Map<String, Integer> messagesByType = new ConcurrentHashMap<>();
    private final Map<String, Integer> droppedMessagesByType = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock statsLock = new ReentrantReadWriteLock();

    // Message delivery callback
    private final BiConsumer<Object, DeliveryContext> messageDeliveryCallback;

    private final Map<String, Set<String>> disconnectedNodes = new HashMap<>();

    /**
     * Class representing a scheduled message.
     */
    private static class ScheduledMessage {
        final Object message;
        final String from;
        final String to;
        final long deliveryTick;
        final long sequenceNumber;

        ScheduledMessage(Object message, String from, String to, long deliveryTick, 
                         long sequenceNumber) {
            this.message = message;
            this.from = from;
            this.to = to;
            this.deliveryTick = deliveryTick;
            this.sequenceNumber = sequenceNumber;
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
     * Context provided when delivering a message.
     */
    public static class DeliveryContext {
        private final String from;
        private final String to;

        public DeliveryContext(String from, String to) {
            this.from = from;
            this.to = to;
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

    public interface MessageHandler {
        void handleMessage(Object message, DeliveryContext context);
    }

    /**
     * Creates a new SimulatedNetwork with the specified message delivery callback.
     * 
     * @param messageDeliveryCallback The callback to invoke when messages are delivered
     * @throws IllegalArgumentException if messageDeliveryCallback is null
     */
    public SimulatedNetwork(BiConsumer<Object, DeliveryContext> messageDeliveryCallback) {
        if (messageDeliveryCallback == null) {
            throw new IllegalArgumentException("Message delivery callback cannot be null");
        }
        this.messageDeliveryCallback = messageDeliveryCallback;
        this.messageFilters = new ArrayList<>();
        this.messagesSentByNode = new HashMap<>();
        this.messagesReceivedByNode = new HashMap<>();
        this.messagesDroppedByNode = new HashMap<>();
        this.messagesDelayedByNode = new HashMap<>();
        this.random = new Random();
        this.currentTick = 0;
    }

    /**
     * Configures the message loss rate for the network.
     *
     * @param rate A value between 0.0 (no loss) and 1.0 (all messages lost)
     * @return This SimulatedNetwork instance for method chaining
     * @throws IllegalArgumentException if rate is not between 0.0 and 1.0
     */
    public SimulatedNetwork withMessageLossRate(double rate) {
        if (rate < 0.0 || rate > 1.0) {
            throw new IllegalArgumentException("Message loss rate must be between 0.0 and 1.0");
        }
        messageLossRate = rate;
        return this;
    }

    /**
     * Configures the latency range for message delivery in ticks.
     * 
     * @param minTicks Minimum latency in ticks
     * @param maxTicks Maximum latency in ticks
     * @return This SimulatedNetwork instance for method chaining
     */
    public SimulatedNetwork withLatency(int minTicks, int maxTicks) {
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
     */
    public SimulatedNetwork withBandwidthLimit(int maxMessages) {
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
    public SimulatedNetwork addMessageFilter(MessageFilter filter) {
        messageFilters.add(filter);
        return this;
    }

    /**
     * Removes a previously added message filter.
     *
     * @param filter The filter to remove
     * @return This SimulatedNetwork instance for method chaining
     */
    public SimulatedNetwork removeMessageFilter(MessageFilter filter) {
        messageFilters.remove(filter);
        return this;
    }

    /**
     * Resets the simulator to default settings.
     * <p>
     * This clears all partitions, message filters, and messages,
     * and resets configuration properties to their default values.
     * </p>
     *
     * @return This SimulatedNetwork instance for method chaining
     */
    public SimulatedNetwork reset() {
        messagesSentByNode.clear();
        messagesReceivedByNode.clear();
        messagesDroppedByNode.clear();
        messagesDelayedByNode.clear();
        currentTick = 0;
        LOGGER.info("Reset network state");

        // Reset network configuration to defaults
        messageLossRate = DEFAULT_MESSAGE_LOSS_RATE;
        minLatencyTicks = DEFAULT_MIN_LATENCY;
        maxLatencyTicks = DEFAULT_MAX_LATENCY;
        maxMessagesPerTick = DEFAULT_MAX_MESSAGES_PER_TICK;

        // Clear message queue
        messagesLock.writeLock().lock();
        try {
            messageQueue.clear();
        } finally {
            messagesLock.writeLock().unlock();
        }

        // Clear all filters and partitioning
        messageFilters.clear();

        // Reset statistics
        statsLock.writeLock().lock();
        try {
            messagesByType.clear();
            droppedMessagesByType.clear();
        } finally {
            statsLock.writeLock().unlock();
        }

        return this;
    }

    /**
     * Disconnects communication between two nodes.
     * Messages sent from sourceNode to targetNode will be dropped.
     *
     * @param sourceNode The node that will be unable to send messages
     * @param targetNode The node that will not receive messages
     */
    public void disconnectNodes(String sourceNode, String targetNode) {
        addMessageFilter((message, from, to) -> 
            !(from.equals(sourceNode) && to.equals(targetNode))
        );
        LOGGER.info("Disconnected " + sourceNode + " from sending to " + targetNode);
    }

    /**
     * Creates a bidirectional disconnect between two nodes.
     * Messages sent between these nodes in either direction will be dropped.
     *
     * @param node1 First node
     * @param node2 Second node
     */
    public void disconnectNodesBidirectional(String node1, String node2) {
        disconnectedNodes.computeIfAbsent(node1, k -> new HashSet<>()).add(node2);
        disconnectedNodes.computeIfAbsent(node2, k -> new HashSet<>()).add(node1);
    }

    /**
     * Removes all message filters, effectively reconnecting all nodes.
     */
    public void reconnectAll() {
        messageFilters.clear();
        disconnectedNodes.clear();
        LOGGER.info("Cleared all network disconnections");
    }

    /**
     * Checks if two nodes can communicate.
     *
     * @param from Source node
     * @param to Target node
     * @return true if messages can be delivered from source to target
     */
    public boolean canCommunicate(String from, String to) {
        // Apply all filters to a dummy message
        Object anyMessage = new Object();
        return messageFilters.stream()
            .allMatch(filter -> filter.shouldDeliver(anyMessage, from, to));
    }

    /**
     * Sends a message through the simulated network.
     * 
     * @param message The message to send
     * @param from The sender node ID
     * @param to The recipient node ID
     * @return true if the message will be delivered (now or later), false if it was dropped
     */
    public boolean sendMessage(Object message, String from, String to) {
        validateMessageParameters(message, from, to);
        
        // Apply network conditions to determine message fate
        if (!canDeliverMessage(message, from, to)) {
            return false; // Message was dropped
        }
        
        long deliveryTick = calculateDeliveryTick();
        scheduleMessageDelivery(message, from, to, deliveryTick);
        return true;
    }

    private void validateMessageParameters(Object message, String from, String to) {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        if (from == null || from.isEmpty()) {
            throw new IllegalArgumentException("Sender ID cannot be null or empty");
        }
        if (to == null || to.isEmpty()) {
            throw new IllegalArgumentException("Recipient ID cannot be null or empty");
        }
    }

    private long calculateDeliveryTick() {
        int delay = maxLatencyTicks > 0 ? calculateMessageDelay() : 0;
        return currentTick + (delay > 0 ? delay : 1); // At minimum, deliver on next tick
    }

    private void scheduleMessageDelivery(Object message, String from, String to, long deliveryTick) {
        messagesLock.writeLock().lock();
        try {
            messageQueue.add(new ScheduledMessage(message, from, to, deliveryTick, messageSequence.getAndIncrement()));
            
            String messageType = message.getClass().getSimpleName();
            LOGGER.log(Level.INFO, "Message type {0} scheduled for delivery at tick {1}: {2} -> {3}, current tick: {4}",
                    new Object[]{messageType, deliveryTick, from, to, currentTick});
        } finally {
            messagesLock.writeLock().unlock();
        }
    }

    /**
     * Determines whether a message can be delivered based on current network conditions.
     * <p>
     * This method applies all configured settings and filters including:
     * - Network partitioning
     * - Message loss probability
     * - Custom message filters
     * </p>
     *
     * @param message The message being sent
     * @param from    The sender node ID
     * @param to      The recipient node ID
     * @return true if the message should be delivered normally, false if dropped
     */
    private boolean canDeliverMessage(Object message, String from, String to) {
        validateMessageParameters(message, from, to);
        trackMessageByType(message);

        if (!canDeliverThroughPartition(message, from, to)) {
            return false;
        }

        if (!passesMessageFilters(message, from, to)) {
            return false;
        }

        if (!passesMessageLossCheck(message, from, to)) {
            return false;
        }

        return true;
    }

    private void trackMessageByType(Object message) {
        String messageType = message.getClass().getName();
        statsLock.writeLock().lock();
        try {
            incrementMessageCount(messagesByType, messageType);
        } finally {
            statsLock.writeLock().unlock();
        }
        LOGGER.log(Level.INFO, "Tracking outbound message type: {0}", messageType);
    }

    private boolean canDeliverThroughPartition(Object message, String from, String to) {
        if (!canCommunicate(from, to) || isDisconnected(from, to)) {
            String messageType = message.getClass().getName();
            statsLock.writeLock().lock();
            try {
                incrementMessageCount(droppedMessagesByType, messageType);
            } finally {
                statsLock.writeLock().unlock();
            }
            LOGGER.log(Level.INFO, "Message dropped due to network partition: {0} -> {1}, type: {2}",
                    new Object[]{from, to, messageType});
            return false;
        }
        return true;
    }

    private boolean passesMessageFilters(Object message, String from, String to) {
        for (MessageFilter filter : messageFilters) {
            if (!filter.shouldDeliver(message, from, to)) {
                String messageType = message.getClass().getName();
                statsLock.writeLock().lock();
                try {
                    incrementMessageCount(droppedMessagesByType, messageType);
                } finally {
                    statsLock.writeLock().unlock();
                }
                LOGGER.log(Level.INFO, "Message dropped by custom filter: {0} -> {1}, type: {2}",
                        new Object[]{from, to, messageType});
                return false;
            }
        }
        return true;
    }

    private boolean passesMessageLossCheck(Object message, String from, String to) {
        if (messageLossRate > 0) {
            boolean shouldDrop = random.nextDouble() < messageLossRate;

            if (shouldDrop) {
                String messageType = message.getClass().getName();
                statsLock.writeLock().lock();
                try {
                    incrementMessageCount(droppedMessagesByType, messageType);
                } finally {
                    statsLock.writeLock().unlock();
                }
                LOGGER.log(Level.INFO, "Message dropped due to random loss: {0} -> {1}, type: {2}, loss rate: {3}",
                        new Object[]{from, to, messageType, messageLossRate});
                return false;
            } else {
                LOGGER.log(Level.INFO, "Message passed random loss check: {0} -> {1}, type: {2}, loss rate: {3}",
                        new Object[]{from, to, message.getClass().getName(), messageLossRate});
            }
        }
        return true;
    }

    /**
     * Calculates a random delay based on the configured latency range.
     * Uses exponential distribution for jitter to better model real network conditions.
     *
     * @return The delay in ticks
     */
    private int calculateMessageDelay() {
        if (minLatencyTicks == maxLatencyTicks) {
            LOGGER.log(Level.INFO, "Using fixed delay of {0} ticks", minLatencyTicks);
            return minLatencyTicks;
        }
        
        // Base delay is always the minimum latency
        int baseDelay = minLatencyTicks;
        int maxJitter = maxLatencyTicks - minLatencyTicks;
        
        // Calculate jitter using exponential distribution
        int jitter = 0;
        if (maxJitter > 0) {
            double random = ThreadLocalRandom.current().nextDouble();
            // Use exponential distribution for jitter (more realistic)
            // Scale -ln(random) to the range [0, maxJitter]
            double expRandom = -Math.log(random);
            // Normalize to [0, 1] range by dividing by theoretical max (which is technically infinity, but we use 5.0)
            double normalizedRandom = Math.min(expRandom / 5.0, 1.0);
            // Scale to maxJitter
            jitter = (int) Math.floor(normalizedRandom * maxJitter);
            
            LOGGER.log(Level.INFO, "Exponential delay calculation: random={0}, expRandom={1}, normalizedRandom={2}, jitter={3}", 
                     new Object[]{random, expRandom, normalizedRandom, jitter});
        }
        
        int totalDelay = baseDelay + jitter;
        LOGGER.log(Level.INFO, "Calculated delay: {0} ticks (baseDelay={1}, jitter={2})", 
                 new Object[]{totalDelay, baseDelay, jitter});
        
        return totalDelay;
    }

    /**
     * Advances the simulation by one tick and processes any messages due for delivery.
     * 
     * @return The number of messages delivered during this tick
     */
    public int tick() {
        currentTick++;
        
        LOGGER.log(Level.INFO, "SimulatedNetwork advanced to tick {0}", currentTick);
        
        List<ScheduledMessage> messagesForThisTick = new ArrayList<>();
        int messagesDelivered = 0;
        
        // Collect messages for this tick
        messagesLock.writeLock().lock();
        try {
            LOGGER.log(Level.INFO, "Message queue size before processing: {0}", messageQueue.size());
            
            while (!messageQueue.isEmpty() && messageQueue.peek().deliveryTick <= currentTick) {
                ScheduledMessage msg = messageQueue.peek();
                LOGGER.log(Level.INFO, "Found message due for delivery: from {0} to {1}, type {2}, scheduled for tick {3}",
                        new Object[]{msg.from, msg.to, msg.message.getClass().getSimpleName(), msg.deliveryTick});
                messagesForThisTick.add(messageQueue.poll());
            }
            
            if (!messageQueue.isEmpty()) {
                ScheduledMessage nextMsg = messageQueue.peek();
                LOGGER.log(Level.INFO, "Next message in queue: from {0} to {1}, type {2}, scheduled for tick {3}",
                        new Object[]{nextMsg.from, nextMsg.to, nextMsg.message.getClass().getSimpleName(), nextMsg.deliveryTick});
            }
            // Messages are already ordered by delivery tick and sequence number thanks to the PriorityQueue
        } finally {
            messagesLock.writeLock().unlock();
        }
        
        LOGGER.log(Level.INFO, "Found {0} messages to deliver at tick {1}", 
                new Object[]{messagesForThisTick.size(), currentTick});
        
        // Apply bandwidth limit if necessary
        if (messagesForThisTick.size() > maxMessagesPerTick) {
            List<ScheduledMessage> deferredMessages = 
                messagesForThisTick.subList(maxMessagesPerTick, messagesForThisTick.size());
            
            // Reschedule excess messages to the next tick
            messagesLock.writeLock().lock();
            try {
                for (ScheduledMessage message : deferredMessages) {
                    messageQueue.add(new ScheduledMessage(
                        message.message,
                        message.from,
                        message.to,
                        currentTick + 1,
                        messageSequence.getAndIncrement()
                    ));
                }
            } finally {
                messagesLock.writeLock().unlock();
            }
            
            // Keep only the messages within the bandwidth limit
            messagesForThisTick = messagesForThisTick.subList(0, maxMessagesPerTick);
            
            LOGGER.log(Level.FINE, "Bandwidth limit applied: {0} messages deferred to next tick",
                    deferredMessages.size());
        }
        
        // Process messages
        for (ScheduledMessage message : messagesForThisTick) {
            // Check if nodes can still communicate (partitions might have changed)
            if (!canCommunicate(message.from, message.to)) {
                String messageType = message.message.getClass().getSimpleName();
                incrementMessageCount(droppedMessagesByType, messageType);
                LOGGER.log(Level.INFO, "Delayed message dropped due to network partition: {0} -> {1}, type: {2}",
                        new Object[]{message.from, message.to, messageType});
                continue;
            }
            
            // Update message statistics before delivery
            String messageType = message.message.getClass().getName();
            incrementMessageCount(messagesByType, messageType);
            
            // Deliver the message via class-level callback
            DeliveryContext context = new DeliveryContext(message.from, message.to);
            LOGGER.log(Level.INFO, "Delivering message from {0} to {1} at tick {2}, type: {3}",
                    new Object[]{message.from, message.to, currentTick, message.message.getClass().getSimpleName()});
            messageDeliveryCallback.accept(message.message, context);
            messagesDelivered++;
            
            LOGGER.log(Level.INFO, "Delivered message from {0} to {1} at tick {2}, type: {3}",
                    new Object[]{message.from, message.to, currentTick, message.message.getClass().getSimpleName()});
        }
        
        if (messagesDelivered > 0) {
            LOGGER.log(Level.INFO, "Processed {0} messages for tick {1}",
                    new Object[]{messagesDelivered, currentTick});
        }
        
        return messagesDelivered;
    }

    /**
     * Gets the current value of the tick counter.
     *
     * @return The current tick
     */
    public long getCurrentTick() {
        return currentTick;
    }

    /**
     * Increment the count for a specific message type in the statistics.
     *
     * @param statsMap    The statistics map to update
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

            int totalMessages = messagesByType.values().stream().mapToInt(Integer::intValue).sum();
            int totalDropped = droppedMessagesByType.values().stream().mapToInt(Integer::intValue).sum();

            stats.put("totalMessages", totalMessages);
            stats.put("totalDropped", totalDropped);

            double dropRate = totalMessages > 0 ? (double) totalDropped / totalMessages : 0.0;

            stats.put("dropRate", dropRate);

            stats.put("minLatencyTicks", minLatencyTicks);
            stats.put("maxLatencyTicks", maxLatencyTicks);
            stats.put("messageLossRate", messageLossRate);
            stats.put("maxMessagesPerTick", maxMessagesPerTick);
        } finally {
            statsLock.readLock().unlock();
        }

        messagesLock.readLock().lock();
        try {
            stats.put("pendingMessageQueueSize", messageQueue.size());
        } finally {
            messagesLock.readLock().unlock();
        }

        return stats;
    }

    private boolean isDisconnected(String from, String to) {
        Set<String> disconnectedFromSource = disconnectedNodes.getOrDefault(from, Collections.emptySet());
        return disconnectedFromSource.contains(to);
    }
}