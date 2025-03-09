package com.dststore.network;

import com.dststore.message.Message;
import com.dststore.message.MessageType;
import com.dststore.simulation.SimulatedClock;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The MessageBus handles serialization, deserialization, and routing of messages between nodes
 * in the distributed system. It uses the PacketSimulator for network communication,
 * providing a higher-level messaging abstraction.
 */
public class MessageBus {
    private static final Logger logger = LoggerFactory.getLogger(MessageBus.class);
    
    private final String nodeId;
    private final PacketSimulator packetSimulator;
    private final ObjectMapper objectMapper;
    private final Map<String, MessageHandler> messageHandlers;
    
    // Track connected nodes for easier status reporting
    private final Map<String, Boolean> connectedNodes;
    
    // Message queue for deterministic delivery
    private final Queue<Message> messageQueue;
    
    /**
     * Creates a new MessageBus for the specified node.
     *
     * @param nodeId The ID of the node this MessageBus belongs to
     * @param packetSimulator The packet simulator to use for network communication
     */
    public MessageBus(String nodeId, PacketSimulator packetSimulator) {
        this.nodeId = nodeId;
        this.packetSimulator = packetSimulator;
        this.objectMapper = new ObjectMapper();
        this.messageHandlers = new HashMap<>();
        this.connectedNodes = new ConcurrentHashMap<>();
        this.messageQueue = new ConcurrentLinkedQueue<>();
        
        // Register this node to receive packets
        packetSimulator.registerListener(nodeId, this::handlePacket);
        
        logger.info("Created MessageBus for node {}", nodeId);
    }
    
    /**
     * Advances the message bus by one time unit, processing any queued messages.
     * This should be called by the main simulation loop to ensure deterministic execution.
     */
    public void tick() {
        // Process any queued messages
        int processedCount = 0;
        while (!messageQueue.isEmpty()) {
            Message message = messageQueue.poll();
            if (message != null) {
                boolean sent = sendMessageInternal(message);
                if (sent) {
                    processedCount++;
                }
            }
        }
        
        if (processedCount > 0) {
            logger.debug("Processed {} queued messages during tick", processedCount);
        }
    }
    
    /**
     * Connects this node to another node, enabling message exchange.
     *
     * @param targetNodeId The ID of the node to connect to
     */
    public void connect(String targetNodeId) {
        connectedNodes.put(targetNodeId, true);
        logger.info("Node {} connected to node {}", nodeId, targetNodeId);
    }
    
    /**
     * Disconnects this node from another node, stopping message exchange.
     *
     * @param targetNodeId The ID of the node to disconnect from
     */
    public void disconnect(String targetNodeId) {
        connectedNodes.remove(targetNodeId);
        logger.info("Node {} disconnected from node {}", nodeId, targetNodeId);
    }
    
    /**
     * Queues a message to be sent on the next tick.
     * This is the preferred method for sending messages in a deterministic manner.
     *
     * @param message The message to queue
     */
    public void queueMessage(Message message) {
        // Set the source ID if not already set
        if (message.getSourceId() == null) {
            message.setSourceId(nodeId);
        } else if (!message.getSourceId().equals(nodeId)) {
            logger.warn("Message source ID is not this node ({} vs {}), correcting", 
                       message.getSourceId(), nodeId);
            message.setSourceId(nodeId);
        }
        
        messageQueue.add(message);
        logger.debug("Message ID {} queued for delivery from {} to {}", message.getMessageId(), message.getSourceId(), message.getTargetId());
    }
    
    /**
     * Sends a message immediately.
     * This can be used for initialization or other cases where immediate delivery is required,
     * but generally queueMessage() should be preferred for deterministic simulation.
     *
     * @param message The message to send
     * @return True if the message was successfully enqueued for delivery, false otherwise
     */
    public boolean sendMessage(Message message) {
        // Set the source ID if not already set
        if (message.getSourceId() == null) {
            message.setSourceId(nodeId);
        } else if (!message.getSourceId().equals(nodeId)) {
            logger.warn("Message source ID is not this node ({} vs {}), correcting", 
                       message.getSourceId(), nodeId);
            message.setSourceId(nodeId);
        }
        
        return sendMessageInternal(message);
    }
    
    /**
     * Internal method to send a message after source ID validation.
     */
    private boolean sendMessageInternal(Message message) {
        String targetNodeId = message.getTargetId();
        if (targetNodeId == null) {
            logger.error("Cannot send message with null target ID: {}", message);
            return false;
        }
        
        if (!connectedNodes.containsKey(targetNodeId)) {
            logger.warn("Attempting to send message to disconnected node: {}", targetNodeId);
            return false;
        }
        
        try {
            byte[] serializedMessage = objectMapper.writeValueAsBytes(message);
            Path path = new Path(nodeId, targetNodeId);
            Packet packet = new Packet(path, serializedMessage, packetSimulator.getCurrentTick());
            
            boolean enqueued = packetSimulator.enqueuePacket(packet);
            if (enqueued) {
                logger.debug("Sent message of type {} from {} to {}", 
                           message.getType(), nodeId, targetNodeId);
            } else {
                logger.warn("Failed to enqueue message of type {} from {} to {}", 
                          message.getType(), nodeId, targetNodeId);
            }
            
            return enqueued;
        } catch (IOException e) {
            logger.error("Failed to serialize message: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * Registers a handler for messages of a specific type.
     *
     * @param messageHandler The handler to invoke when a message is received
     */
    public void registerHandler(MessageHandler messageHandler) {
        messageHandlers.put(messageHandler.getHandledType().name(), messageHandler);
        logger.debug("Registered handler for message type {} on node {}", 
                   messageHandler.getHandledType(), nodeId);
    }
    
    /**
     * Unregisters a handler for messages of a specific type.
     *
     * @param messageType The type of message to unregister the handler for
     */
    public void unregisterHandler(MessageType messageType) {
        messageHandlers.remove(messageType.name());
        logger.debug("Unregistered handler for message type {} on node {}", messageType, nodeId);
    }
    
    /**
     * Gets the ID of the node this message bus belongs to.
     *
     * @return Node ID
     */
    public String getNodeId() {
        return nodeId;
    }
    
    /**
     * Checks if this node is connected to another node.
     *
     * @param targetNodeId The ID of the node to check connection to
     * @return True if connected, false otherwise
     */
    public boolean isConnected(String targetNodeId) {
        return connectedNodes.containsKey(targetNodeId);
    }
    
    /**
     * Gets all nodes this node is connected to.
     *
     * @return Map of node IDs to connection status
     */
    public Map<String, Boolean> getConnectedNodes() {
        return new HashMap<>(connectedNodes);
    }
    
    /**
     * Gets the number of messages currently in the queue.
     *
     * @return The queue size
     */
    public int getQueueSize() {
        return messageQueue.size();
    }
    
    /**
     * Handles a received packet by deserializing it and passing it to the appropriate handler.
     *
     * @param packet The received packet
     * @param currentTick The current simulation tick
     */
    private void handlePacket(Packet packet, long currentTick) {
        try {
            byte[] payload = packet.getPayload();
            Message message = objectMapper.readValue(payload, Message.class);
            
            logger.debug("Handling packet with message ID {} at tick {}", message.getMessageId(), currentTick);
            logger.debug("Received message of type {} from {} to {}", 
                       message.getType(), message.getSourceId(), message.getTargetId());
            
            MessageHandler handler = messageHandlers.get(message.getType().name());
            if (handler != null) {
                handler.handleMessage(message);
            } else {
                logger.warn("No handler registered for message type {}", message.getType());
            }
        } catch (IOException e) {
            logger.error("Failed to deserialize packet payload: {}", e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error handling message: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Interface for components that handle specific types of messages.
     */
    public interface MessageHandler {
        /**
         * Gets the type of message this handler processes.
         *
         * @return The message type
         */
        MessageType getHandledType();
        
        /**
         * Handles a received message.
         *
         * @param message The message to handle
         */
        void handleMessage(Message message);
    }
} 