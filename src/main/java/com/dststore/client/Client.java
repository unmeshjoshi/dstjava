package com.dststore.client;

import com.dststore.message.GetRequest;
import com.dststore.message.GetResponse;
import com.dststore.message.PutResponse;
import com.dststore.network.MessageBus;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Client for the distributed storage system.
 * Handles sending requests to replicas and processing responses.
 */
public class Client {
    private final String clientId;
    private final MessageBus messageBus;
    private final Map<String, CompletableFuture<?>> pendingRequests = new ConcurrentHashMap<>();
    
    public static final String CLIENT_ID = "client-1";
    
    public Client(String clientId, MessageBus messageBus) {
        this.clientId = clientId;
        this.messageBus = messageBus;
        messageBus.registerNode(clientId);
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
    
    public void tick() {
        // Process all incoming messages
        List<Object> messages = messageBus.receiveMessages(clientId);
        
        if (!messages.isEmpty()) {
            System.out.println("Client " + clientId + " received " + messages.size() + " messages");
        }
        
        for (Object message : messages) {
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
            future.complete(response);
        } else {
            System.out.println("Client " + clientId + " received response for unknown messageId: " + messageId);
        }
    }
    
    public String getClientId() {
        return clientId;
    }
    
    public Map<String, CompletableFuture<?>> getPendingRequests() {
        return pendingRequests;
    }
} 