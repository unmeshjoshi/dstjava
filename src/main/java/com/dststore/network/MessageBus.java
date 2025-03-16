package com.dststore.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
    
    // Default queue size limits
    private static final int DEFAULT_MAX_QUEUE_SIZE = 1000;
    
    // The network simulator for controlled testing
    private final SimulatedNetwork simulatedNetwork;
    
    // Current tick for simulation
    private final AtomicLong currentTick = new AtomicLong(0);
    
    // Registry of message handlers for nodes
    private final Map<String, MessageHandler> messageHandlers = new ConcurrentHashMap<>();
    
    // Map to identify node types by their ID
    private final Map<String, NodeType> nodeTypes = new ConcurrentHashMap<>();
    
    // Lock for tick advancement
    private final ReentrantReadWriteLock tickLock = new ReentrantReadWriteLock();
    
    // JSON serializer for messages
    private final ObjectMapper objectMapper;
    
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
        /**
         * Handles a received message.
         * 
         * @param message The received message
         * @param from The sender node ID
         */
        void handleMessage(Object message, String from);
    }
    
    /**
     * Constructs a MessageBus with the provided network simulator.
     * 
     * @param simulatedNetwork The network simulator to use
     */
    public MessageBus(SimulatedNetwork simulatedNetwork) {
        if (simulatedNetwork == null) {
            throw new IllegalArgumentException("SimulatedNetwork cannot be null");
        }
        
        // Configure ObjectMapper with type validation for security
        PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
            .allowIfBaseType(Object.class)
            .allowIfSubType("com.dststore.message")
            .build();
            
        this.objectMapper = new ObjectMapper();
        this.objectMapper.activateDefaultTyping(ptv);
        
        this.simulatedNetwork = simulatedNetwork;
        LOGGER.log(Level.INFO, "MessageBus initialized");
    }
    
    /**
     * Default constructor that creates a MessageBus with a new SimulatedNetwork instance.
     * The SimulatedNetwork is configured with a callback to deliver messages through this MessageBus.
     */
    public MessageBus() {
        // Configure ObjectMapper with type validation for security
        PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
            .allowIfBaseType(Object.class)
            .allowIfSubType("com.dststore.message")
            .build();
            
        this.objectMapper = new ObjectMapper();
        this.objectMapper.activateDefaultTyping(ptv);
        
        // Create a network with a callback to our deliverMessage method
        this.simulatedNetwork = new SimulatedNetwork((message, context) -> {
            deliverMessage(message, context.getFrom(), context.getTo());
        });
        
        LOGGER.log(Level.INFO, "MessageBus initialized with default network simulator");
    }
    
    /**
     * Returns the SimulatedNetwork instance used by this MessageBus.
     * 
     * @return The SimulatedNetwork instance
     */
    public SimulatedNetwork getSimulatedNetwork() {
        return simulatedNetwork;
    }
    
    /**
     * Gets the current tick in the simulation.
     * 
     * @return The current tick
     */
    public long getCurrentTick() {
        return currentTick.get();
    }
    
    /**
     * Registers a node with the message bus.
     * 
     * @param nodeId The ID of the node
     * @param handler The message handler for the node
     * @param nodeType The type of the node
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
        tickLock.writeLock().lock();
        try {
            long newTick = currentTick.incrementAndGet();
            LOGGER.log(Level.FINE, "Advanced to tick {0}", newTick);
            
            // Advance the network simulator and deliver messages
            return simulatedNetwork.tick();
        } finally {
            tickLock.writeLock().unlock();
        }
    }
    
    /**
     * Original send method for backward compatibility.
     * Automatically routes the message based on node types.
     *
     * @param targetNodeId The ID of the target node
     * @param senderId The ID of the sender node
     * @param message The message to send
     * @throws IllegalArgumentException if targetNodeId or senderId is null or empty, or if message is null
     */
    public <T> void send(String targetNodeId, String senderId, T message) {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        
        validateTargetAndSender(targetNodeId, senderId);
        
        // Check node types to route appropriately
        NodeType targetType = nodeTypes.getOrDefault(targetNodeId, NodeType.REPLICA);
        NodeType senderType = nodeTypes.getOrDefault(senderId, NodeType.REPLICA);
        
        boolean sent;
        if (targetType == NodeType.CLIENT) {
            // This should be a replica-to-client message
            if (senderType != NodeType.REPLICA) {
                LOGGER.log(Level.WARNING, "Non-replica node {0} attempting to send to client {1}, using generic route",
                           new Object[]{senderId, targetNodeId});
                sent = sendMessage(message, senderId, targetNodeId);
            } else {
                sent = sendMessageToClient(message, senderId, targetNodeId);
            }
        } else if (targetType == NodeType.REPLICA) {
            // This is a message to a replica, could be from client or another replica
            if (senderType == NodeType.REPLICA) {
                sent = sendMessageToReplica(message, senderId, targetNodeId);
            } else {
                // Client-to-replica or unknown-to-replica
                sent = sendMessage(message, senderId, targetNodeId);
            }
        } else {
            // Unknown node type, use generic method
            sent = sendMessage(message, senderId, targetNodeId);
        }
        
        if (!sent) {
            LOGGER.log(Level.WARNING, "Message from {0} to {1} was dropped",
                     new Object[]{senderId, targetNodeId});
        }
    }
    
    /**
     * Sends a message to a specific replica node.
     * 
     * @param message The message to send
     * @param from The sender node ID
     * @param replicaId The recipient replica node ID
     * @return true if the message was queued for delivery, false if it was dropped
     * @throws IllegalArgumentException if any parameter is invalid
     * @throws IllegalStateException if the target node is not registered as a replica
     */
    public boolean sendMessageToReplica(Object message, String from, String replicaId) {
        validateSender(from);
        validateTargetReplica(replicaId);
        
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        
        // Send the message through the network simulator
        boolean sent = simulatedNetwork.sendMessage(message, from, replicaId);
        
        // Log the message being sent
        LOGGER.log(Level.FINE, "Message from {0} to replica {1} scheduled for delivery: {2}", 
                 new Object[]{from, replicaId, sent ? "yes" : "dropped"});
        
        return sent;
    }
    
    /**
     * Sends a message to a specific client node.
     * 
     * @param message The message to send
     * @param from The sender node ID (should be a replica)
     * @param clientId The recipient client node ID
     * @return true if the message was queued for delivery, false if it was dropped
     * @throws IllegalArgumentException if any parameter is invalid
     * @throws IllegalStateException if the sender is not a replica or the target is not a client
     */
    public boolean sendMessageToClient(Object message, String from, String clientId) {
        validateSenderReplica(from);
        validateTargetClient(clientId);
        
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        
        // Send the message through the network simulator
        boolean sent = simulatedNetwork.sendMessage(message, from, clientId);
        
        // Log the message being sent
        LOGGER.log(Level.FINE, "Message from replica {0} to client {1} scheduled for delivery: {2}", 
                 new Object[]{from, clientId, sent ? "yes" : "dropped"});
        
        return sent;
    }
    
    /**
     * Sends a message to a specific node (generic method).
     * This is kept for backward compatibility.
     * 
     * @param message The message to send
     * @param from The sender node ID
     * @param to The recipient node ID
     * @return true if the message was queued for delivery, false if it was dropped
     * @throws IllegalArgumentException if any parameter is null, or if from/to are empty
     */
    public boolean sendMessage(Object message, String from, String to) {
        validateTargetAndSender(to, from);
        
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        
        // Send the message through the network simulator
        boolean sent = simulatedNetwork.sendMessage(message, from, to);
        
        // Log the message being sent
        LOGGER.log(Level.FINE, "Message from {0} to {1} scheduled for delivery: {2}", 
                 new Object[]{from, to, sent ? "yes" : "dropped"});
        
        return sent;
    }
    
    /**
     * Delivers a message to its target node.
     * 
     * @param message The message to deliver
     * @param from The sender node ID
     * @param to The recipient node ID
     */
    private void deliverMessage(Object message, String from, String to) {
        // Find the message handler for the target node
        MessageHandler handler = messageHandlers.get(to);
        
        if (handler != null) {
            // If there's a registered handler, deliver directly via callback
            try {
                handler.handleMessage(message, from);
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
     * @param senderId The sender node ID
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
     * Validates the target replica node ID.
     * 
     * @param replicaId The target replica node ID
     * @throws IllegalArgumentException If the ID is null or empty
     * @throws IllegalStateException If the node is explicitly registered as non-replica
     */
    private void validateTargetReplica(String replicaId) {
        if (replicaId == null || replicaId.isEmpty()) {
            throw new IllegalArgumentException("Replica ID cannot be null or empty");
        }
        
        NodeType nodeType = nodeTypes.get(replicaId);
        // If node is not registered yet, assume it's a replica
        if (nodeType != null && nodeType != NodeType.REPLICA) {
            throw new IllegalStateException("Target node " + replicaId + " is not a replica");
        }
    }
    
    /**
     * Validates the target client node ID.
     * 
     * @param clientId The target client node ID
     * @throws IllegalArgumentException If the ID is null or empty
     * @throws IllegalStateException If the node is explicitly registered as non-client
     */
    private void validateTargetClient(String clientId) {
        if (clientId == null || clientId.isEmpty()) {
            throw new IllegalArgumentException("Client ID cannot be null or empty");
        }
        
        NodeType nodeType = nodeTypes.get(clientId);
        // If node is not registered yet, assume it's a client
        if (nodeType != null && nodeType != NodeType.CLIENT) {
            throw new IllegalStateException("Target node " + clientId + " is not a client");
        }
    }
    
    /**
     * Validates the sender node ID.
     * 
     * @param senderId The sender node ID
     * @throws IllegalArgumentException If the ID is null or empty
     */
    private void validateSender(String senderId) {
        if (senderId == null || senderId.isEmpty()) {
            throw new IllegalArgumentException("Sender ID cannot be null or empty");
        }
    }
    
    /**
     * Validates that the sender is a replica.
     * 
     * @param senderId The sender node ID
     * @throws IllegalArgumentException If the ID is null or empty
     * @throws IllegalStateException If the node is explicitly registered as non-replica
     */
    private void validateSenderReplica(String senderId) {
        if (senderId == null || senderId.isEmpty()) {
            throw new IllegalArgumentException("Sender ID cannot be null or empty");
        }
        
        NodeType nodeType = nodeTypes.get(senderId);
        // If node is not registered yet, assume it's a replica
        if (nodeType != null && nodeType != NodeType.REPLICA) {
            throw new IllegalStateException("Sender node " + senderId + " is not a replica");
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
        tickLock.writeLock().lock();
        try {
            currentTick.set(0);
            simulatedNetwork.reset();
            LOGGER.log(Level.INFO, "MessageBus reset");
        } finally {
            tickLock.writeLock().unlock();
        }
    }
    
    /**
     * Gets a set of node IDs for all registered nodes.
     * 
     * @return A set of node IDs
     */
    public Set<String> getRegisteredNodeIds() {
        return new HashSet<>(messageHandlers.keySet());
    }
    
    /**
     * Gets a set of node IDs for all registered replicas.
     * 
     * @return A set of replica node IDs
     */
    public Set<String> getRegisteredReplicaIds() {
        Set<String> replicaIds = new HashSet<>();
        for (Map.Entry<String, NodeType> entry : nodeTypes.entrySet()) {
            if (entry.getValue() == NodeType.REPLICA) {
                replicaIds.add(entry.getKey());
            }
        }
        return replicaIds;
    }
    
    /**
     * Gets a set of node IDs for all registered clients.
     * 
     * @return A set of client node IDs
     */
    public Set<String> getRegisteredClientIds() {
        Set<String> clientIds = new HashSet<>();
        for (Map.Entry<String, NodeType> entry : nodeTypes.entrySet()) {
            if (entry.getValue() == NodeType.CLIENT) {
                clientIds.add(entry.getKey());
            }
        }
        return clientIds;
    }
} 