package com.dststore.network;

import com.dststore.message.Message;
import com.dststore.message.MessageType;
import com.dststore.simulation.SimulatedClock;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Set;

/**
 * The MessageBus handles serialization, deserialization, and routing of messages between nodes
 * in the distributed system. It uses the PacketSimulator for network communication,
 * providing a higher-level messaging abstraction.
 */
public class MessageBus implements IMessageBus {
    private static final Logger logger = LoggerFactory.getLogger(MessageBus.class);
    
    private final String nodeId;
    private final PacketSimulator packetSimulator;
    private final ObjectMapper objectMapper;
    private final Map<String, Boolean> connections = new HashMap<>();
    private final Map<MessageType, List<MessageHandler>> handlers = new HashMap<>();
    private final List<Message> messageQueue = new ArrayList<>();
    private final List<PacketListener> listeners = new ArrayList<>();
    
    // Track connected nodes for easier status reporting
    private final Map<String, Boolean> connectedNodes;
    
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
        this.connectedNodes = new ConcurrentHashMap<>();
        
        // Register this node to receive packets
        if (packetSimulator != null) {
            packetSimulator.registerListener(nodeId, this::handlePacket);
        }
        
        logger.info("Created MessageBus for node {}", nodeId);
    }

    @Override
    public void start() {
        // Nothing to do for simulated message bus
    }

    @Override
    public void stop() {
        // Nothing to do for simulated message bus
    }
    
    /**
     * Advances the message bus by one time unit, processing any queued messages.
     * This should be called by the main simulation loop to ensure deterministic execution.
     */
    public void tick() {
        // Process all queued messages
        List<Message> messages = new ArrayList<>(messageQueue);
        messageQueue.clear();
        
        for (Message message : messages) {
            // Ensure the source ID is set correctly
            if (message.getSourceId() == null || !message.getSourceId().equals(nodeId)) {
                message = message.withSourceId(nodeId);
            }
            
            // Send the message through the packet simulator
            sendMessageInternal(message);
        }
    }
    
    @Override
    public void connect(String targetNodeId, String host, int port) {
        connect(targetNodeId);
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
    
    @Override
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
        if (message.getSourceId() == null || !message.getSourceId().equals(nodeId)) {
            logger.warn("Message source ID is not this node ({} vs {}), correcting", 
                       message.getSourceId(), nodeId);
            message = message.withSourceId(nodeId);
        }
        
        messageQueue.add(message);
        logger.debug("Message ID {} queued for delivery from {} to {}", message.getMessageId(), message.getSourceId(), message.getTargetId());
    }
    
    @Override
    public void sendMessage(Message message) {
        // Ensure the source ID is set correctly
        if (message.getSourceId() == null || !message.getSourceId().equals(nodeId)) {
            message = message.withSourceId(nodeId);
        }
        
        // Check if we're connected to the target node
        if (message.getTargetId() == null) {
            logger.error("Cannot send message with null target ID: {}", message);
            return;
        }
        
        if (!isConnected(message.getTargetId())) {
            logger.warn("Attempting to send message to disconnected node: {}", message.getTargetId());
            return;
        }
        
        // Send the message immediately
        sendMessageInternal(message);
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
    
    @Override
    public void registerHandler(MessageHandler handler) {
        handlers.computeIfAbsent(handler.getHandledType(), k -> new ArrayList<>()).add(handler);
    }
    
    @Override
    public void unregisterHandler(MessageType messageType) {
        handlers.remove(messageType);
    }
    
    @Override
    public void registerListener(PacketListener listener) {
        listeners.add(listener);
    }
    
    /**
     * Handles a received packet by deserializing it and passing it to the appropriate handler.
     */
    private void handlePacket(Packet packet, long currentTick) {
        try {
            Message message = objectMapper.readValue(packet.getPayload(), Message.class);
            
            // First try specific message type handlers
            List<MessageHandler> messageHandlers = handlers.get(message.getType());
            if (messageHandlers != null) {
                for (MessageHandler handler : messageHandlers) {
                    handler.handleMessage(message);
                }
            }
        } catch (IOException e) {
            logger.error("Failed to deserialize message: {}", e.getMessage(), e);
        }
    }
    
    @Override
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

    public void send(Message message) {
        if (message.getTargetId() == null) {
            throw new IllegalArgumentException("Cannot send message with null target id");
        }
        if (!isConnected(message.getTargetId())) {
            logger.warn("Attempting to send message to disconnected node: {}", message.getTargetId());
            return;
        }
        Message messageWithSourceId = message.withSourceId(nodeId);
        sendMessage(messageWithSourceId);
    }

    public void broadcast(Message message) {
        connectedNodes.keySet().stream()
            .filter(targetId -> !targetId.equals(nodeId))
            .forEach(targetId -> {
                Message messageToSend = message.withSourceId(nodeId);
                // Since Message is immutable, we need to create a new instance with the target ID
                // This is handled by the concrete message classes
                messageToSend = messageToSend.withTargetId(targetId);
                sendMessage(messageToSend);
            });
    }
} 