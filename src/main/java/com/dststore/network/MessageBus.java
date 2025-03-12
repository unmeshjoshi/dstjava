package com.dststore.network;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The MessageBus provides messaging capabilities between nodes in the distributed system.
 * It handles message serialization, routing, and delivery.
 */
public class MessageBus {
    private static final Logger LOGGER = Logger.getLogger(MessageBus.class.getName());
    private static final int DEFAULT_MAX_QUEUE_SIZE = 1000;
    
    public static class MessageSerializationException extends RuntimeException {
        public MessageSerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
    
    public static class MessageDeserializationException extends RuntimeException {
        public MessageDeserializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
    
    public static class UnregisteredNodeException extends RuntimeException {
        public UnregisteredNodeException(String message) {
            super(message);
        }
    }
    
    private static class InternalMessage {
        final String senderId;
        final String targetId;
        final byte[] payload; // Serialized message data
        final String messageType; // Type information for deserialization
        final long messageId; // Unique message identifier
        
        InternalMessage(String senderId, String targetId, byte[] payload, String messageType, long messageId) {
            this.senderId = senderId;
            this.targetId = targetId;
            this.payload = payload;
            this.messageType = messageType;
            this.messageId = messageId;
        }
    }
    
    private final Map<String, Queue<InternalMessage>> incomingMessages = new ConcurrentHashMap<>();
    private final Queue<InternalMessage> pendingMessages = new ConcurrentLinkedQueue<>();
    private final ObjectMapper objectMapper;
    private final AtomicLong messageIdGenerator = new AtomicLong(0);
    private final int maxQueueSize;
    
    // Network simulator for controlled, deterministic testing
    private NetworkSimulator networkSimulator;
    private volatile boolean simulationEnabled = false;
    private final AtomicLong currentTick = new AtomicLong(0);
    
    public MessageBus() {
        this(DEFAULT_MAX_QUEUE_SIZE);
    }
    
    public MessageBus(int maxQueueSize) {
        if (maxQueueSize <= 0) {
            throw new IllegalArgumentException("Queue size must be positive");
        }
        
        // Configure ObjectMapper with type validation for security
        PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
            .allowIfBaseType(Object.class)
            .allowIfSubType("com.dststore.message")
            .build();
            
        this.objectMapper = new ObjectMapper();
        this.objectMapper.activateDefaultTyping(ptv);
        this.maxQueueSize = maxQueueSize;
        LOGGER.log(Level.INFO, "MessageBus initialized with max queue size {0}", maxQueueSize);
    }
    
    /**
     * Enable network simulation with default settings.
     * 
     * @return The NetworkSimulator instance for further configuration
     */
    public synchronized NetworkSimulator enableNetworkSimulation() {
        if (networkSimulator == null) {
            networkSimulator = new NetworkSimulator();
        }
        simulationEnabled = true;
        LOGGER.log(Level.INFO, "Network simulation enabled");
        return networkSimulator;
    }
    
    /**
     * Disable network simulation.
     */
    public synchronized void disableNetworkSimulation() {
        simulationEnabled = false;
        LOGGER.log(Level.INFO, "Network simulation disabled");
    }
    
    /**
     * Check if network simulation is enabled.
     * 
     * @return true if network simulation is enabled, false otherwise
     */
    public boolean isSimulationEnabled() {
        return simulationEnabled;
    }
    
    /**
     * Get the network simulator instance.
     * 
     * @return The NetworkSimulator instance
     */
    public synchronized NetworkSimulator getNetworkSimulator() {
        if (networkSimulator == null) {
            networkSimulator = new NetworkSimulator();
        }
        return networkSimulator;
    }
    
    /**
     * Register a node with the message bus.
     * 
     * @param nodeId The ID of the node to register
     * @throws IllegalArgumentException if nodeId is null or empty
     */
    public void registerNode(String nodeId) {
        if (nodeId == null || nodeId.isEmpty()) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }
        incomingMessages.put(nodeId, new ConcurrentLinkedQueue<>());
        LOGGER.log(Level.INFO, "Registered node {0}", nodeId);
    }
    
    /**
     * Send a message to a target node.
     * 
     * @param targetNodeId The ID of the target node
     * @param senderId The ID of the sender node
     * @param message The message to send
     * @param <T> The type of the message
     * @throws UnregisteredNodeException if the target node is not registered
     * @throws MessageSerializationException if the message cannot be serialized
     */
    public <T> void send(String targetNodeId, String senderId, T message) {
        validateTargetAndSender(targetNodeId, senderId);
        
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        
        try {
            byte[] serialized = objectMapper.writeValueAsBytes(message);
            long messageId = messageIdGenerator.incrementAndGet();
            
            // Check if we should process this message through the network simulator
            if (simulationEnabled && networkSimulator != null) {
                try {
                    boolean shouldDeliver = networkSimulator.processOutboundMessage(message, senderId, targetNodeId);
                    if (!shouldDeliver) {
                        // Message was dropped or delayed by the simulator
                        LOGGER.log(Level.FINE, "Message from {0} to {1} was dropped or delayed by network simulator", 
                                  new Object[]{senderId, targetNodeId});
                        return;
                    }
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Error in network simulator, delivering message anyway", e);
                    // Continue with delivery if simulator fails
                }
            }
            
            pendingMessages.add(new InternalMessage(
                senderId, 
                targetNodeId, 
                serialized, 
                message.getClass().getName(), 
                messageId
            ));
            
            LOGGER.log(Level.FINE, "Message queued from {0} to {1} with ID {2}", 
                      new Object[]{senderId, targetNodeId, messageId});
        } catch (JsonProcessingException e) {
            throw new MessageSerializationException(
                "Failed to serialize message of type " + message.getClass().getName() + 
                " from " + senderId + " to " + targetNodeId, 
                e
            );
        }
    }
    
    /**
     * Send a raw message to a target node.
     * 
     * @param targetNodeId The ID of the target node
     * @param senderId The ID of the sender node
     * @param payload The raw message payload
     * @param messageType The type of the message
     * @return The message ID
     * @throws UnregisteredNodeException if the target node is not registered
     */
    public long sendRaw(String targetNodeId, String senderId, byte[] payload, String messageType) {
        validateTargetAndSender(targetNodeId, senderId);
        
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }
        if (messageType == null || messageType.isEmpty()) {
            throw new IllegalArgumentException("Message type cannot be null or empty");
        }
        
        long messageId = messageIdGenerator.incrementAndGet();
        
        // Check if we should process this message through the network simulator
        if (simulationEnabled && networkSimulator != null) {
            try {
                // Deserialize for the simulator to inspect
                Class<?> messageClass = Class.forName(messageType);
                Object message = objectMapper.readValue(payload, messageClass);
                
                boolean shouldDeliver = networkSimulator.processOutboundMessage(message, senderId, targetNodeId);
                if (!shouldDeliver) {
                    // Message was dropped or delayed by the simulator
                    LOGGER.log(Level.FINE, "Raw message from {0} to {1} was dropped or delayed by network simulator", 
                              new Object[]{senderId, targetNodeId});
                    return messageId;
                }
            } catch (Exception e) {
                // If we can't deserialize for the simulator, just proceed with delivery
                LOGGER.log(Level.WARNING, "Warning: Could not process raw message through simulator: {0}", e.getMessage());
            }
        }
        
        pendingMessages.add(new InternalMessage(senderId, targetNodeId, payload, messageType, messageId));
        LOGGER.log(Level.FINE, "Raw message queued from {0} to {1} with ID {2}", 
                  new Object[]{senderId, targetNodeId, messageId});
        return messageId;
    }
    
    /**
     * Validate that target and sender nodes are valid.
     * 
     * @param targetNodeId The ID of the target node
     * @param senderId The ID of the sender node
     * @throws IllegalArgumentException if targetNodeId or senderId is null or empty
     * @throws UnregisteredNodeException if the target node is not registered
     */
    private void validateTargetAndSender(String targetNodeId, String senderId) {
        if (targetNodeId == null || targetNodeId.isEmpty()) {
            throw new IllegalArgumentException("Target node ID cannot be null or empty");
        }
        if (senderId == null || senderId.isEmpty()) {
            throw new IllegalArgumentException("Sender node ID cannot be null or empty");
        }
        if (!incomingMessages.containsKey(targetNodeId)) {
            throw new UnregisteredNodeException("Target node " + targetNodeId + " is not registered");
        }
    }
    
    /**
     * Process a simulation tick.
     * This advances the simulation time and processes any pending messages.
     */
    public void tick() {
        long tick = currentTick.incrementAndGet();
        
        // Process any delayed messages from the simulator that are now ready
        if (simulationEnabled && networkSimulator != null) {
            try {
                List<NetworkSimulator.DelayedMessage> readyMessages = 
                    networkSimulator.getMessagesForDelivery(tick);
                
                if (!readyMessages.isEmpty()) {
                    LOGGER.log(Level.FINE, "Processing {0} delayed messages at tick {1}", 
                              new Object[]{readyMessages.size(), tick});
                }
                
                for (NetworkSimulator.DelayedMessage delayedMessage : readyMessages) {
                    try {
                        // Re-serialize for consistency with normal message flow
                        byte[] serialized = objectMapper.writeValueAsBytes(delayedMessage.getMessage());
                        long messageId = messageIdGenerator.incrementAndGet();
                        
                        pendingMessages.add(new InternalMessage(
                            delayedMessage.getFrom(), 
                            delayedMessage.getTo(), 
                            serialized, 
                            delayedMessage.getMessage().getClass().getName(), 
                            messageId
                        ));
                        
                        LOGGER.log(Level.FINE, "Delayed message from {0} to {1} is now ready for delivery", 
                                  new Object[]{delayedMessage.getFrom(), delayedMessage.getTo()});
                    } catch (JsonProcessingException e) {
                        LOGGER.log(Level.WARNING, "Could not deliver delayed message: {0}", e.getMessage());
                    }
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error processing delayed messages", e);
                // Continue with normal message processing even if delayed messages fail
            }
        }
        
        // Process all pending messages and route them to target nodes
        int processedCount = 0;
        InternalMessage message;
        
        while ((message = pendingMessages.poll()) != null) {
            Queue<InternalMessage> targetQueue = incomingMessages.get(message.targetId);
            if (targetQueue != null) {
                // Check queue size before adding
                if (targetQueue.size() < maxQueueSize) {
                    targetQueue.add(message);
                    processedCount++;
                } else {
                    // Log that the message was dropped due to queue overflow
                    LOGGER.log(Level.WARNING, 
                              "Message dropped due to queue overflow for node: {0}", message.targetId);
                }
            } else {
                LOGGER.log(Level.WARNING, 
                          "Message dropped because target node {0} is not registered", message.targetId);
            }
        }
        
        if (processedCount > 0) {
            LOGGER.log(Level.FINE, "Processed {0} messages at tick {1}", 
                      new Object[]{processedCount, tick});
        }
    }
    
    /**
     * Receive messages for a node.
     * 
     * @param nodeId The ID of the node
     * @return A list of messages for the node
     */
    public List<Object> receiveMessages(String nodeId) {
        if (nodeId == null || nodeId.isEmpty()) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }
        
        Queue<InternalMessage> queue = incomingMessages.get(nodeId);
        if (queue == null) {
            LOGGER.log(Level.WARNING, "Attempted to receive messages for unregistered node: {0}", nodeId);
            return List.of();
        }
        
        List<Object> messages = new ArrayList<>();
        InternalMessage message;
        int messageCount = 0;
        
        while ((message = queue.poll()) != null) {
            try {
                // Deserialize the message based on its type
                Class<?> messageClass = Class.forName(message.messageType);
                Object deserializedMessage = objectMapper.readValue(message.payload, messageClass);
                messages.add(deserializedMessage);
                messageCount++;
            } catch (IOException e) {
                throw new MessageDeserializationException(
                    "Failed to deserialize message of type " + message.messageType + 
                    " from " + message.senderId + " to " + message.targetId,
                    e
                );
            } catch (ClassNotFoundException e) {
                throw new MessageDeserializationException(
                    "Unknown message type: " + message.messageType,
                    e
                );
            }
        }
        
        if (messageCount > 0) {
            LOGGER.log(Level.FINE, "Node {0} received {1} messages", 
                      new Object[]{nodeId, messageCount});
        }
        
        return messages;
    }
    
    /**
     * Receive raw messages for a node.
     * 
     * @param nodeId The ID of the node
     * @return A list of raw messages for the node
     */
    public List<Message> receiveRawMessages(String nodeId) {
        if (nodeId == null || nodeId.isEmpty()) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }
        
        Queue<InternalMessage> queue = incomingMessages.get(nodeId);
        if (queue == null) {
            LOGGER.log(Level.WARNING, "Attempted to receive raw messages for unregistered node: {0}", nodeId);
            return List.of();
        }
        
        List<Message> messages = new ArrayList<>();
        InternalMessage internalMessage;
        int messageCount = 0;
        
        while ((internalMessage = queue.poll()) != null) {
            messages.add(new Message(
                internalMessage.messageId,
                internalMessage.senderId,
                internalMessage.targetId,
                internalMessage.payload,
                internalMessage.messageType
            ));
            messageCount++;
        }
        
        if (messageCount > 0) {
            LOGGER.log(Level.FINE, "Node {0} received {1} raw messages", 
                      new Object[]{nodeId, messageCount});
        }
        
        return messages;
    }
    
    /**
     * Message container class for raw message passing.
     */
    public static class Message {
        private final long id;         // Unique message identifier
        private final String from;     // Sender node ID
        private final String to;       // Target node ID
        private final byte[] data;     // Raw message payload
        private final String type;     // Message type identifier
        
        public Message(long id, String from, String to, byte[] data, String type) {
            this.id = id;
            this.from = from;
            this.to = to;
            this.data = data;
            this.type = type;
        }
        
        public long getId() {
            return id;
        }
        
        public String getFrom() {
            return from;
        }
        
        public String getTo() {
            return to;
        }
        
        public byte[] getData() {
            return data;
        }
        
        public String getType() {
            return type;
        }
        
        @Override
        public String toString() {
            return "Message{" +
                "id=" + id +
                ", from='" + from + '\'' +
                ", to='" + to + '\'' +
                ", type='" + type + '\'' +
                ", dataSize=" + (data != null ? data.length : 0) +
                '}';
        }
    }
    
    /**
     * Get message statistics by type.
     * 
     * @return A map of message type to total size
     */
    public Map<String, Integer> getMessageStats() {
        Map<String, Integer> stats = new ConcurrentHashMap<>();
        
        for (Queue<InternalMessage> queue : incomingMessages.values()) {
            for (InternalMessage message : queue) {
                String type = message.messageType;
                int size = message.payload.length;
                stats.put(type, stats.getOrDefault(type, 0) + size);
            }
        }
        
        return stats;
    }
    
    /**
     * Get the current simulation tick.
     * 
     * @return The current tick
     */
    public long getCurrentTick() {
        return currentTick.get();
    }
} 