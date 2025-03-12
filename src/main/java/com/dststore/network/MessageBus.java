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

/**
 * The MessageBus provides messaging capabilities between nodes in the distributed system.
 * It handles message serialization, routing, and delivery.
 */
public class MessageBus {
    
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
    
    public MessageBus() {
        this(DEFAULT_MAX_QUEUE_SIZE);
    }
    
    public MessageBus(int maxQueueSize) {
        // Configure ObjectMapper with type validation for security
        PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
            .allowIfBaseType(Object.class)
            .allowIfSubType("com.dststore.message")
            .build();
            
        this.objectMapper = new ObjectMapper();
        this.objectMapper.activateDefaultTyping(ptv);
        this.maxQueueSize = maxQueueSize;
    }
    
    public void registerNode(String nodeId) {
        if (nodeId == null || nodeId.isEmpty()) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }
        incomingMessages.put(nodeId, new ConcurrentLinkedQueue<>());
    }
    
    public <T> void send(String targetNodeId, String senderId, T message) {
        validateTargetAndSender(targetNodeId, senderId);
        
        try {
            byte[] serialized = objectMapper.writeValueAsBytes(message);
            long messageId = messageIdGenerator.incrementAndGet();
            pendingMessages.add(new InternalMessage(
                senderId, 
                targetNodeId, 
                serialized, 
                message.getClass().getName(), 
                messageId
            ));
        } catch (JsonProcessingException e) {
            throw new MessageSerializationException(
                "Failed to serialize message of type " + message.getClass().getName() + 
                " from " + senderId + " to " + targetNodeId, 
                e
            );
        }
    }
    
    public long sendRaw(String targetNodeId, String senderId, byte[] payload, String messageType) {
        validateTargetAndSender(targetNodeId, senderId);
        
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }
        if (messageType == null || messageType.isEmpty()) {
            throw new IllegalArgumentException("Message type cannot be null or empty");
        }
        
        long messageId = messageIdGenerator.incrementAndGet();
        pendingMessages.add(new InternalMessage(senderId, targetNodeId, payload, messageType, messageId));
        return messageId;
    }
    
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
    
    public void tick() {
        // Process all pending messages and route them to target nodes
        InternalMessage message;
        
        while ((message = pendingMessages.poll()) != null) {
            Queue<InternalMessage> targetQueue = incomingMessages.get(message.targetId);
            if (targetQueue != null) {
                // Check queue size before adding
                if (targetQueue.size() < maxQueueSize) {
                    targetQueue.add(message);
                } else {
                    // Log that the message was dropped due to queue overflow
                    System.out.println("WARNING: Message dropped due to queue overflow for node: " + message.targetId);
                }
            }
        }
    }
    
    public List<Object> receiveMessages(String nodeId) {
        Queue<InternalMessage> queue = incomingMessages.get(nodeId);
        if (queue == null) {
            return List.of();
        }
        
        List<Object> messages = new ArrayList<>();
        InternalMessage message;
        
        while ((message = queue.poll()) != null) {
            try {
                // Deserialize the message based on its type
                Class<?> messageClass = Class.forName(message.messageType);
                Object deserializedMessage = objectMapper.readValue(message.payload, messageClass);
                messages.add(deserializedMessage);
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
        
        return messages;
    }
    
    public List<Message> receiveRawMessages(String nodeId) {
        Queue<InternalMessage> queue = incomingMessages.get(nodeId);
        if (queue == null) {
            return List.of();
        }
        
        List<Message> messages = new ArrayList<>();
        InternalMessage internalMessage;
        
        while ((internalMessage = queue.poll()) != null) {
            messages.add(new Message(
                internalMessage.messageId,
                internalMessage.senderId,
                internalMessage.targetId,
                internalMessage.payload,
                internalMessage.messageType
            ));
        }
        
        return messages;
    }
    
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
    }
    
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
} 