package com.dststore.network;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Message bus for inter-node communication in distributed systems.
 * <p>
 * This class handles message passing between nodes in the system, with support for
 * simulated network conditions for testing. It uses a callback-based approach where
 * messages are delivered directly to node handlers when they arrive.
 * </p>
 */
public class MessageBus {
    private static final Logger LOGGER = Logger.getLogger(MessageBus.class.getName());

    // The network simulator for controlled testing
    private final SimulatedNetwork simulatedNetwork;

    // Current tick for simulation
    private final AtomicLong currentTick = new AtomicLong(0);

    // Registry of message handlers for nodes
    private final Map<String, MessageHandler> messageHandlers = new ConcurrentHashMap<>();

    // Map to identify node types by their ID
    private final Map<String, NodeType> nodeTypes = new ConcurrentHashMap<>();

    // Message serializer for serialization and deserialization
    private final MessageSerializer messageSerializer;

    // Message ID generator
    private final AtomicLong messageIdGenerator = new AtomicLong(0);

    /**
     * Enumeration of node types in the system.
     */
    public enum NodeType {
        REPLICA,
        CLIENT
    }

    /**
     * Interface for node message handlers.
     */
    public interface MessageHandler {
        void handleMessage(Object message, SimulatedNetwork.DeliveryContext from);
    }

    /**
     * Default constructor that creates a MessageBus with a new SimulatedNetwork instance.
     * The SimulatedNetwork is configured with a callback to deliver messages through this MessageBus.
     */
    public MessageBus() {

        // Initialize message serializer
        this.messageSerializer = new MessageSerializer();

        // Create a network with a callback to our deliverMessage method
        this.simulatedNetwork = new SimulatedNetwork((message, context) -> {
            deliverMessage(message, context.getFrom(), context.getTo());
        });

        LOGGER.log(Level.INFO, "MessageBus initialized with default network simulator");
    }

    public SimulatedNetwork getNetwork() {
        return simulatedNetwork;
    }


    public long getCurrentTick() {
        return currentTick.get();
    }

    /**
     * Registers a node and the message handler with the message bus.
     */
    public void registerNode(String nodeId, MessageHandler handler, NodeType nodeType) {
        if (nodeId == null || nodeId.isEmpty()) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }
        if (handler == null) {
            throw new IllegalArgumentException("Message handler cannot be null");
        }
        if (nodeType == null) {
            throw new IllegalArgumentException("Node type cannot be null");
        }

        messageHandlers.put(nodeId, handler);
        nodeTypes.put(nodeId, nodeType);
        LOGGER.log(Level.INFO, "Registered {0} node: {1}", new Object[]{nodeType, nodeId});
    }

    /**
     * Unregisters a node from the message bus.
     *
     * @param nodeId The ID of the node to unregister
     */
    public void unregisterNode(String nodeId) {
        messageHandlers.remove(nodeId);
        nodeTypes.remove(nodeId);
        LOGGER.log(Level.INFO, "Unregistered node: {0}", nodeId);
    }

    /**
     * Process a simulation tick and handles message delivery.
     *
     * @return The number of messages delivered during this tick
     */
    public int tick() {
        long newTick = currentTick.incrementAndGet();
        LOGGER.log(Level.FINE, "Advanced to tick {0}", newTick);

        // Advance the network simulator and deliver messages
        return simulatedNetwork.tick();
    }

    /**
     * Sends a message to a specific node (generic method).
     * This is kept for backward compatibility.
     
     */
    public boolean sendMessage(Object message, String from, String to) {
        validateTargetAndSender(to, from);

        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }

        try {
            // Serialize the message
            String serializedMessage = messageSerializer.serialize(message);

            // Send the serialized message through the network simulator
            boolean sent = simulatedNetwork.sendMessage(serializedMessage, from, to);

            // Log the message being sent
            LOGGER.log(Level.FINE, "Message from {0} to {1} scheduled for delivery: {2}",
                    new Object[]{from, to, sent ? "yes" : "dropped"});

            return sent;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to serialize message from {0} to {1}", new Object[]{from, to});
            return false;
        }
    }

    /**
     * Delivers a message to its target node.
     *
     * @param message The message to deliver
     * @param from    The sender node ID
     * @param to      The recipient node ID
     */
    private void deliverMessage(Object message, String from, String to) {
        // Find the message handler for the target node
        MessageHandler handler = messageHandlers.get(to);

        if (handler != null) {
            try {
                // Deserialize the message
                Object deserializedMessage = messageSerializer.deserialize((String) message);

                // Deliver the deserialized message
                handler.handleMessage(deserializedMessage, new SimulatedNetwork.DeliveryContext(from, to));
                LOGGER.log(Level.FINE, "Message delivered to handler for node: {0}", to);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error delivering message to handler for node: " + to, e);
            }
        } else {
            LOGGER.log(Level.WARNING, "No handler registered for node: {0}", to);
        }
    }

    /**
     * Validates the target and sender node IDs.
     *
     * @param targetNodeId The target node ID
     * @param senderId     The sender node ID
     * @throws IllegalArgumentException If either ID is null or empty
     */
    private void validateTargetAndSender(String targetNodeId, String senderId) {
        if (targetNodeId == null || targetNodeId.isEmpty()) {
            throw new IllegalArgumentException("Target node ID cannot be null or empty");
        }
        if (senderId == null || senderId.isEmpty()) {
            throw new IllegalArgumentException("Sender ID cannot be null or empty");
        }
    }


    /**
     * Gets message statistics by type from the SimulatedNetwork.
     * This method is provided for backward compatibility.
     *
     * @return A map of message types to counts
     */
    public Map<String, Integer> getMessageStats() {
        Map<String, Object> stats = simulatedNetwork.getStatistics();
        Map<String, Object> messagesByType = (Map<String, Object>) stats.getOrDefault("messagesByType", new HashMap<>());

        // Convert Object values to Integer
        Map<String, Integer> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : messagesByType.entrySet()) {
            if (entry.getValue() instanceof Number) {
                result.put(entry.getKey(), ((Number) entry.getValue()).intValue());
            }
        }

        return result;
    }

    /**
     * Resets the message bus state.
     */
    public void reset() {
        currentTick.set(0);
        simulatedNetwork.reset();
        LOGGER.log(Level.INFO, "MessageBus reset");
    }

    public Set<String> getRegisteredNodeIds() {
        return new HashSet<>(messageHandlers.keySet());
    }

    public Set<String> getRegisteredReplicaIds() {
        Set<String> replicaIds = new HashSet<>();
        for (Map.Entry<String, NodeType> entry : nodeTypes.entrySet()) {
            if (entry.getValue() == NodeType.REPLICA) {
                replicaIds.add(entry.getKey());
            }
        }
        return replicaIds;
    }

    public Set<String> getRegisteredClientIds() {
        Set<String> clientIds = new HashSet<>();
        for (Map.Entry<String, NodeType> entry : nodeTypes.entrySet()) {
            if (entry.getValue() == NodeType.CLIENT) {
                clientIds.add(entry.getKey());
            }
        }
        return clientIds;
    }

    /**
     * Gets the underlying simulated network.
     *
     * @return The SimulatedNetwork instance
     */
    public SimulatedNetwork getSimulatedNetwork() {
        return simulatedNetwork;
    }
} 