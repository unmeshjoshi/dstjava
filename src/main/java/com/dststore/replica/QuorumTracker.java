package com.dststore.replica;

import com.dststore.message.GetResponse;
import com.dststore.message.ReplicaResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class QuorumTracker {
    private final String requestId;
    private final String clientId;
    private final String clientMessageId; // Original message ID from client request
    private final String key;
    private final Timeout timeout;
    private final Map<String, ReplicaResponse> responses = new HashMap<>();
    private boolean completed = false;
    private final int quorumSize;
    private final OperationType operationType; // GET or PUT
    
    public QuorumTracker(String requestId, String clientId, String clientMessageId, String key, 
                         int quorumSize, OperationType operationType, long currentTick, long timeoutTicks) {
        if (requestId == null || requestId.isEmpty()) {
            throw new IllegalArgumentException("Request ID cannot be null or empty");
        }
        if (clientId == null || clientId.isEmpty()) {
            throw new IllegalArgumentException("Client ID cannot be null or empty");
        }
        if (clientMessageId == null || clientMessageId.isEmpty()) {
            throw new IllegalArgumentException("Client message ID cannot be null or empty");
        }
        if (operationType == null) {
            throw new IllegalArgumentException("Operation type cannot be null");
        }
        
        this.requestId = requestId;
        this.clientId = clientId;
        this.clientMessageId = clientMessageId;
        this.key = key;
        this.timeout = new Timeout(currentTick, timeoutTicks);
        this.quorumSize = quorumSize;
        this.operationType = operationType;
        
        System.out.println("Created QuorumTracker with requestId=" + requestId + 
                          ", clientMessageId=" + clientMessageId + 
                          ", operation=" + operationType +
                          ", timeout=" + timeout);
    }
    
    public boolean addResponse(ReplicaResponse response) {
        if (response == null) {
            throw new IllegalArgumentException("Response cannot be null");
        }
        
        if (!completed) {
            responses.put(response.getResponderReplicaId(), response);
            return true;
        }
        return false;
    }
    
    public boolean hasQuorum() {
        return responses.size() >= quorumSize;
    }
    
    public ReplicaResponse getFirstResponse() {
        if (responses.isEmpty()) {
            return null;
        }
        return responses.values().iterator().next();
    }
    
    public GetResponse createClientResponse(String originReplicaId) {
        if (operationType != OperationType.GET) {
            throw new IllegalStateException("createClientResponse is only valid for GET operations");
        }
        
        if (!hasQuorum()) {
            return null;
        }
        
        // 1. First, try to find a successful response
        Optional<ReplicaResponse> successfulResponse = responses.values().stream()
            .filter(ReplicaResponse::isSuccess)
            .findFirst();
        
        if (successfulResponse.isPresent()) {
            // We found a successful response with a value
            ReplicaResponse response = successfulResponse.get();
            System.out.println("QuorumTracker using successful response from " + 
                              response.getResponderReplicaId() + 
                              " with value: '" + response.getValue() + "'");
            
            return new GetResponse(
                clientMessageId,
                response.getKey(),
                response.getValue(),
                true,
                originReplicaId
            );
        }
        
        // 2. If no successful response, use the first response
        ReplicaResponse firstResponse = responses.values().iterator().next();
        System.out.println("QuorumTracker using first response from " + 
                          firstResponse.getResponderReplicaId() + 
                          " (no successful responses)");
        
        return new GetResponse(
            clientMessageId,
            firstResponse.getKey(),
            firstResponse.getValue(),
            false,
            originReplicaId
        );
    }
    
    public void markCompleted() {
        this.completed = true;
    }
    
    public boolean hasTimedOut(long currentTick) {
        return timeout.isExpired(currentTick);
    }
    
    public String getRequestId() {
        return requestId;
    }
    
    public String getClientId() {
        return clientId;
    }
    
    public String getClientMessageId() {
        return clientMessageId;
    }
    
    public String getKey() {
        return key;
    }
    
    public Timeout getTimeout() {
        return timeout;
    }
    
    public boolean isCompleted() {
        return completed;
    }
    
    public OperationType getOperationType() {
        return operationType;
    }
} 
