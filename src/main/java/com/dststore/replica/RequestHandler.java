package com.dststore.replica;

import com.dststore.message.GetRequest;
import com.dststore.message.GetResponse;
import com.dststore.message.Message;
import com.dststore.message.MessageType;
import com.dststore.message.SetRequest;
import com.dststore.message.SetResponse;
import com.dststore.network.MessageBus;
import com.dststore.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles client requests by forwarding them to replica nodes and collecting responses.
 * This class manages the distribution of requests and coordination of responses.
 */
public class RequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);
    
    private final String clientId;
    private final MessageBus messageBus;
    private final List<String> replicaIds;
    private final Map<UUID, PendingRequest<?>> pendingRequests;
    private final Map<UUID, RequestTracker<GetResponse>> requestTrackers = new ConcurrentHashMap<>();
    private final Metrics metrics;
    
    /**
     * Creates a new RequestHandler with the specified client ID and message bus.
     *
     * @param clientId The ID of the client this handler belongs to
     * @param messageBus The message bus for communication
     * @param metrics The metrics for recording client requests
     */
    public RequestHandler(String clientId, MessageBus messageBus, Metrics metrics) {
        this.clientId = clientId;
        this.messageBus = messageBus;
        this.replicaIds = new ArrayList<>();
        this.pendingRequests = new ConcurrentHashMap<>();
        this.metrics = metrics;
        
        // Register message handlers for responses
        registerResponseHandlers();
        
        logger.info("Created RequestHandler for client {}", clientId);
    }
    
    /**
     * Registers a replica with this request handler.
     * Requests will be sent to all registered replicas.
     *
     * @param replicaId The ID of the replica to register
     */
    public void registerReplica(String replicaId) {
        if (!replicaIds.contains(replicaId)) {
            replicaIds.add(replicaId);
            messageBus.connect(replicaId);
            logger.info("Client {} registered replica {}", clientId, replicaId);
        }
    }
    
    /**
     * Unregisters a replica from this request handler.
     * Requests will no longer be sent to this replica.
     *
     * @param replicaId The ID of the replica to unregister
     */
    public void unregisterReplica(String replicaId) {
        if (replicaIds.remove(replicaId)) {
            messageBus.disconnect(replicaId);
            logger.info("Client {} unregistered replica {}", clientId, replicaId);
        }
    }
    
    /**
     * Gets the list of registered replica IDs.
     *
     * @return The list of replica IDs
     */
    public List<String> getReplicaIds() {
        return new ArrayList<>(replicaIds);
    }
    
    /**
     * Sends a SET request to all registered replicas.
     * Returns a future that will be completed when a response is received.
     *
     * @param key The key to set
     * @param value The value to set
     * @return A future that will be completed with the set response
     */
    public CompletableFuture<SetResponse> set(String key, String value) {
        metrics.recordRequest();
        // Create a future to be completed when a response is received
        CompletableFuture<SetResponse> future = new CompletableFuture<>();
        
        // Check if we have any replicas registered
        if (replicaIds.isEmpty()) {
            future.completeExceptionally(new IllegalStateException("No replicas registered"));
            return future;
        }
        
        // Create a message ID to track this request
        UUID messageId = UUID.randomUUID();
        
        // Create a pending request to track responses
        PendingRequest<SetResponse> pendingRequest = new PendingRequest<>(messageId, future);
        pendingRequests.put(messageId, pendingRequest);
        
        // Create a SET request
        long timestamp = System.currentTimeMillis();
        
        // Send the request to all replicas
        for (String replicaId : replicaIds) {
            SetRequest request = new SetRequest(clientId, replicaId, key, value, timestamp);
            request.setMessageId(messageId);
            
            // Queue the message for sending
            messageBus.queueMessage(request);
            
            logger.debug("Sending SET request with message ID {} for key {} to replicas", messageId, key);
            logger.debug("Client {} sent SET request for key {} to replica {}", 
                       clientId, key, replicaId);
        }
        
        logger.info("Client {} sent SET request for key {} to {} replicas", 
                  clientId, key, replicaIds.size());
        
        return future;
    }
    
    /**
     * Sends a GET request to all registered replicas.
     * Returns a future that will be completed when a response is received.
     *
     * @param key The key to get
     * @return A future that will be completed with the get response
     */
    public CompletableFuture<GetResponse> get(String key) {
        metrics.recordRequest();
        CompletableFuture<GetResponse> future = new CompletableFuture<>();

        if (replicaIds.isEmpty()) {
            future.completeExceptionally(new IllegalStateException("No replicas registered"));
            return future;
        }

        UUID messageId = UUID.randomUUID();
        RequestTracker<GetResponse> tracker = new RequestTracker<>(messageId.toString(), (replicaIds.size() / 2) + 1);
        tracker.setFuture(future);
        requestTrackers.put(messageId, tracker);

        for (String replicaId : replicaIds) {
            GetRequest request = new GetRequest(clientId, replicaId, key);
            request.setMessageId(messageId);
            messageBus.queueMessage(request);
        }

        // Timeout handling can be managed by the simulation framework

        return future;
    }
    
    /**
     * Advances the request handler by one time unit, processing any pending operations.
     * This method is called by the simulation loop to ensure deterministic execution.
     */
    public void tick() {
        // Tick the message bus to process any queued messages
        messageBus.tick();
        
        // Check for timed-out requests (not implemented for simplicity)
    }
    
    /**
     * Registers message handlers for response messages.
     */
    private void registerResponseHandlers() {
        // Handler for SET_RESPONSE messages
        messageBus.registerHandler(new MessageBus.MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.SET_RESPONSE;
            }
            
            @Override
            public void handleMessage(Message message) {
                SetResponse response = (SetResponse) message;
                UUID messageId = response.getMessageId();
                
                logger.debug("Received SET response with message ID {} for key {} from replica", messageId, response.getKey());
                
                // Find the pending request
                @SuppressWarnings("unchecked")
                PendingRequest<SetResponse> pendingRequest = 
                    (PendingRequest<SetResponse>) pendingRequests.get(messageId);
                
                if (pendingRequest != null) {
                    // For simplicity, we complete the future with the first response we receive
                    // In a real system, we'd want to wait for a quorum of responses
                    pendingRequest.getFuture().complete(response);
                    pendingRequests.remove(messageId);
                    
                    logger.info("Client {} completed SET request {} with response from {}", 
                              clientId, messageId, response.getSourceId());
                } else {
                    logger.warn("Client {} received SET response for unknown message {} from {}", 
                              clientId, messageId, response.getSourceId());
                }
            }
        });
        
        // Handler for GET_RESPONSE messages
        messageBus.registerHandler(new MessageBus.MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_RESPONSE;
            }
            
            @Override
            public void handleMessage(Message message) {
                GetResponse response = (GetResponse) message;
                UUID messageId = response.getMessageId();

                RequestTracker<GetResponse> tracker = requestTrackers.get(messageId);
                if (tracker != null && tracker.addResponse(response.getSourceId(), response)) {
                    tracker.getFuture().complete(tracker.getBestResponse());
                    requestTrackers.remove(messageId);
                }
            }
        });
    }
    
    /**
     * Gets the statistics for this request handler.
     *
     * @return A map of statistic names to values
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("pendingRequests", pendingRequests.size());
        stats.put("registeredReplicas", replicaIds.size());
        return stats;
    }
    
    /**
     * Represents a pending request waiting for a response.
     */
    private static class PendingRequest<T> {
        private final UUID messageId;
        private final CompletableFuture<T> future;
        private final long createdAt;
        
        /**
         * Creates a new pending request.
         *
         * @param messageId The message ID of the request
         * @param future The future to complete when a response is received
         */
        public PendingRequest(UUID messageId, CompletableFuture<T> future) {
            this.messageId = messageId;
            this.future = future;
            this.createdAt = System.currentTimeMillis();
        }
        
        /**
         * Gets the message ID of the request.
         *
         * @return The message ID
         */
        public UUID getMessageId() {
            return messageId;
        }
        
        /**
         * Gets the future to complete when a response is received.
         *
         * @return The future
         */
        public CompletableFuture<T> getFuture() {
            return future;
        }
        
        /**
         * Gets the time when this request was created.
         *
         * @return The creation time in milliseconds
         */
        public long getCreatedAt() {
            return createdAt;
        }
    }
} 