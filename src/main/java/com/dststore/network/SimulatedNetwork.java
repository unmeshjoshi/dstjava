package com.dststore.network;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simulates a network with configurable conditions like message loss, latency, and partitioning.
 * <p>
 * TODO: Revisit the network partition design:
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

    // Default configuration constants
    private static final double DEFAULT_MESSAGE_LOSS_RATE = 0.0;
    private static final int DEFAULT_MIN_LATENCY = 0;
    private static final int DEFAULT_MAX_LATENCY = 0;
    private static final int DEFAULT_MAX_MESSAGES_PER_TICK = Integer.MAX_VALUE;
    
    // Default deterministic seed for reproducible test results
    private static final long DEFAULT_RANDOM_SEED = 12345L;

    // Configuration
    private volatile double messageLossRate = DEFAULT_MESSAGE_LOSS_RATE;
    private volatile int maxMessagesPerTick = DEFAULT_MAX_MESSAGES_PER_TICK;

    private final Random random;
    // Current simulation tick
    private long currentTick = 0;

    // Message queue for scheduling and delivering messages
    private final MessageQueue messageQueue;

    // Message delay calculator
    private final MessageDelayCalculator delayCalculator;

    // Message filtering
    private final List<MessageFilter> messageFilters;

    // Message delivery callback
    private final BiConsumer<Object, DeliveryContext> messageDeliveryCallback;

    private final Map<String, Set<String>> disconnectedNodes = new HashMap<>();

    // Statistics
    private final NetworkStatistics statistics;

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
     * Creates a new SimulatedNetwork with default deterministic settings.
     * Uses a fixed random seed for reproducible test results.
     *
     * @param messageDeliveryCallback The callback to handle message delivery
     */
    public SimulatedNetwork(BiConsumer<Object, DeliveryContext> messageDeliveryCallback) {
        this(messageDeliveryCallback, DEFAULT_RANDOM_SEED);
    }

    /**
     * Creates a new SimulatedNetwork with a custom random seed.
     * 
     * @param messageDeliveryCallback The callback to handle message delivery
     * @param randomSeed The seed for the random number generator (for deterministic behavior)
     */
    public SimulatedNetwork(BiConsumer<Object, DeliveryContext> messageDeliveryCallback, long randomSeed) {
        if (messageDeliveryCallback == null) {
            throw new IllegalArgumentException("Message delivery callback cannot be null");
        }
        this.messageDeliveryCallback = messageDeliveryCallback;
        this.messageFilters = new ArrayList<>();
        this.statistics = new NetworkStatistics();
        this.messageQueue = new MessageQueue();
        this.random = new Random(randomSeed);  // ← DETERMINISTIC!
        this.delayCalculator = new MessageDelayCalculator(DEFAULT_MIN_LATENCY, DEFAULT_MAX_LATENCY, this.random);
        this.currentTick = 0;
        
        LOGGER.log(Level.INFO, "Initialized SimulatedNetwork with deterministic random seed: {0}", randomSeed);
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
        delayCalculator.setLatency(minTicks, maxTicks);
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
        statistics.reset();
        currentTick = 0;
        LOGGER.info("Reset network state");

        // Reset network configuration to defaults
        messageLossRate = DEFAULT_MESSAGE_LOSS_RATE;
        delayCalculator.setLatency(DEFAULT_MIN_LATENCY, DEFAULT_MAX_LATENCY);
        maxMessagesPerTick = DEFAULT_MAX_MESSAGES_PER_TICK;

        messageQueue.clear();

        // Clear all filters and partitioning
        messageFilters.clear();

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
     * @param to   Target node
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
     * @param from    The sender node ID
     * @param to      The recipient node ID
     * @return true if the message will be delivered (now or later), false if it was dropped
     */
    public boolean sendMessage(Object message, String from, String to) {
        validateMessageParameters(message, from, to);

        // Apply network conditions to determine message fate
        if (!canDeliverMessage(message, from, to)) {
            return false; // Message was dropped
        }

        long deliveryTick = delayCalculator.calculateDeliveryTick(currentTick);
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

    private void scheduleMessageDelivery(Object message, String from, String to, long deliveryTick) {
        messageQueue.scheduleMessage(message, from, to, deliveryTick);
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
        var messageType = message.getClass().getName();
        statistics.trackMessageSent("", messageType); // Empty string for node ID since we don't track by node here
        LOGGER.log(Level.INFO, "Tracking outbound message type: {0}", messageType);
    }

    private boolean canDeliverThroughPartition(Object message, String from, String to) {
        if (!canCommunicate(from, to) || isDisconnected(from, to)) {
            var messageType = message.getClass().getName();
            statistics.trackMessageDropped(to, messageType);
            LOGGER.log(Level.INFO, "Message dropped due to network partition: {0} -> {1}, type: {2}",
                    new Object[]{from, to, messageType});
            return false;
        }
        return true;
    }

    private boolean passesMessageFilters(Object message, String from, String to) {
        for (MessageFilter filter : messageFilters) {
            if (!filter.shouldDeliver(message, from, to)) {
                var messageType = message.getClass().getName();
                statistics.trackMessageDropped(to, messageType);
                LOGGER.log(Level.INFO, "Message dropped by custom filter: {0} -> {1}, type: {2}",
                        new Object[]{from, to, messageType});
                return false;
            }
        }
        return true;
    }

    private boolean passesMessageLossCheck(Object message, String from, String to) {
        if (messageLossRate > 0) {
            var shouldDrop = random.nextDouble() < messageLossRate;

            if (shouldDrop) {
                var messageType = message.getClass().getName();
                statistics.trackMessageDropped(to, messageType);
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
     * Advances the simulation by one tick and processes any messages due for delivery.
     *
     * @return The number of messages delivered during this tick
     */
    public int tick() {
        currentTick++;

        LOGGER.log(Level.INFO, "SimulatedNetwork advanced to tick {0}", currentTick);
        LOGGER.log(Level.INFO, "Message queue size before processing: {0}", messageQueue.size());

        var messagesForThisTick = messageQueue.getMessagesForTick(currentTick, maxMessagesPerTick);

        if (!messageQueue.isEmpty()) {
            var nextMsg = messageQueue.peek();
            LOGGER.log(Level.INFO, "Next message in queue: from {0} to {1}, type {2}, scheduled for tick {3}",
                    new Object[]{nextMsg.getFrom(), nextMsg.getTo(), nextMsg.getMessage().getClass().getSimpleName(), nextMsg.getDeliveryTick()});
        }

        LOGGER.log(Level.INFO, "Found {0} messages to deliver at tick {1}",
                new Object[]{messagesForThisTick.size(), currentTick});

        // Process messages
        var messagesDelivered = deliverMessages(messagesForThisTick);

        if (messagesDelivered > 0) {
            LOGGER.log(Level.INFO, "Processed {0} messages for tick {1}",
                    new Object[]{messagesDelivered, currentTick});
        }

        return messagesDelivered;
    }

    private int deliverMessages(List<MessageQueue.ScheduledMessage> messagesForThisTick) {
        var messagesDelivered = 0;
        for (var message : messagesForThisTick) {
            // Check if nodes can still communicate (partitions might have changed)
            if (!canCommunicate(message.getFrom(), message.getTo()) || isDisconnected(message.getFrom(), message.getTo())) {
                var messageType = message.getMessage().getClass().getSimpleName();
                statistics.trackMessageDropped(message.getTo(), messageType);
                LOGGER.log(Level.INFO, "Delayed message dropped due to network partition: {0} -> {1}, type: {2}",
                        new Object[]{message.getFrom(), message.getTo(), messageType});
                continue;
            }

            // Update message statistics before delivery
            var messageType = message.getMessage().getClass().getName();
            statistics.trackMessageReceived(message.getTo(), messageType);

            // Deliver the message via class-level callback
            var context = new DeliveryContext(message.getFrom(), message.getTo());
            LOGGER.log(Level.INFO, "Delivering message from {0} to {1} at tick {2}, type: {3}",
                    new Object[]{message.getFrom(), message.getTo(), currentTick, message.getMessage().getClass().getSimpleName()});
            messageDeliveryCallback.accept(message.getMessage(), context);
            messagesDelivered++;

            LOGGER.log(Level.INFO, "Delivered message from {0} to {1} at tick {2}, type: {3}",
                    new Object[]{message.getFrom(), message.getTo(), currentTick, message.getMessage().getClass().getSimpleName()});
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
     * Gets network statistics including message counts by type.
     *
     * @return A map of statistics
     */
    public Map<String, Object> getStatistics() {
        var networkConfig = new HashMap<String, Object>();
        networkConfig.put("minLatencyTicks", delayCalculator.getMinLatencyTicks());
        networkConfig.put("maxLatencyTicks", delayCalculator.getMaxLatencyTicks());
        networkConfig.put("messageLossRate", messageLossRate);
        networkConfig.put("maxMessagesPerTick", maxMessagesPerTick);
        
        return statistics.getStatistics(messageQueue.size(), networkConfig);
    }

    private boolean isDisconnected(String from, String to) {
        var disconnectedFromSource = disconnectedNodes.getOrDefault(from, Collections.emptySet());
        return disconnectedFromSource.contains(to);
    }
}