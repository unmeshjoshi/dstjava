package com.dststore.client;

import com.dststore.message.GetRequest;
import com.dststore.message.GetResponse;
import com.dststore.message.PutRequest;
import com.dststore.message.PutResponse;
import com.dststore.network.MessageBus;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Client for the distributed storage system.
 * Handles sending requests to replicas and processing responses.
 */
public class Client {
    private static final Logger LOGGER = Logger.getLogger(Client.class.getName());
    private final String clientId;
    private final MessageBus messageBus;
    private final Map<String, CompletableFuture<?>> pendingRequests = new ConcurrentHashMap<>();
    
    public static final String CLIENT_ID = "client-1";
    
    public Client(String clientId, MessageBus messageBus) {
        this.clientId = clientId;
        this.messageBus = messageBus;
        messageBus.registerNode(clientId, this::handleMessage, MessageBus.NodeType.CLIENT);
        System.out.println("Client " + clientId + " created and registered with MessageBus");
    }
    
    public CompletableFuture<GetResponse> getValue(String key, String replicaId) {
        String messageId = UUID.randomUUID().toString();
        GetRequest request = new GetRequest(messageId, key, clientId);
        
        CompletableFuture<GetResponse> future = new CompletableFuture<>();
        pendingRequests.put(messageId, future);
        
        messageBus.send(replicaId, clientId, request);
        System.out.println("Client " + clientId + " sent GetRequest for key '" + key + "' to replica " + replicaId + " with messageId " + messageId);
        
        return future;
    }
    
    /**
     * Puts a value for a key to the specified replica.
     * 
     * @param key The key to store
     * @param value The value to store
     * @param replicaId The ID of the replica to send the request to
     * @return A CompletableFuture that will be completed with the PutResponse
     */
    public CompletableFuture<PutResponse> put(String key, String value, String replicaId) {
        String messageId = UUID.randomUUID().toString();
        PutRequest request = new PutRequest(messageId, key, value, clientId);
        
        CompletableFuture<PutResponse> future = new CompletableFuture<>();
        pendingRequests.put(messageId, future);
        
        messageBus.send(replicaId, clientId, request);
        System.out.println("Client " + clientId + " sent PutRequest for key '" + key + "' with value '" + value + "' to replica " + replicaId + " with messageId " + messageId);
        
        return future;
    }
    
    private void handleMessage(Object message, String from) {
        if (message instanceof GetResponse) {
            GetResponse response = (GetResponse) message;
            System.out.println("Client " + clientId + " processing GetResponse: messageId=" + response.getMessageId() + 
                              ", key=" + response.getKey() + 
                              ", value='" + response.getValue() + "'" + 
                              ", success=" + response.isSuccess());
            processResponse(response.getMessageId(), response);
        } else if (message instanceof PutResponse) {
            PutResponse response = (PutResponse) message;
            System.out.println("Client " + clientId + " processing PutResponse: messageId=" + response.getMessageId() + 
                              ", key=" + response.getKey() + 
                              ", success=" + response.isSuccess());
            processResponse(response.getMessageId(), response);
        } else {
            System.out.println("Client " + clientId + " received unknown message type: " + message.getClass().getName());
        }
        
        // Debug: print pending requests
        if (!pendingRequests.isEmpty()) {
            System.out.println("Client " + clientId + " has " + pendingRequests.size() + " pending requests");
        }
    }
    
    @SuppressWarnings("unchecked")
    private <T> void processResponse(String messageId, T response) {
        CompletableFuture<T> future = (CompletableFuture<T>) pendingRequests.remove(messageId);
        if (future != null) {
            System.out.println("Client " + clientId + " completing future for messageId " + messageId);
            // Always complete the future, even if the operation failed
            // The success/failure is indicated in the response object
            future.complete(response);
        } else {
            System.out.println("Client " + clientId + " received response for unknown messageId: " + messageId);
        }
    }
    
    /**
     * Programmatically complete all pending requests with a failure response.
     * This is useful for tests that expect operations to fail.
     */
    public void completeAllPendingRequestsWithFailure() {
        for (Map.Entry<String, CompletableFuture<?>> entry : pendingRequests.entrySet()) {
            String messageId = entry.getKey();
            CompletableFuture<?> future = entry.getValue();
            
            System.out.println("Forcibly cancelling future for messageId " + messageId);
            
            // Simply cancel the future instead of trying to complete it with a specific response type
            future.cancel(true);
        }
        
        // Clear all pending requests
        pendingRequests.clear();
    }
    
    public String getClientId() {
        return clientId;
    }
    
    public Map<String, CompletableFuture<?>> getPendingRequests() {
        return pendingRequests;
    }
} 