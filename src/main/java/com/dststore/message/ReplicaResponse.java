package com.dststore.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ReplicaResponse {
    private final String requestId;
    private final String responderReplicaId;
    private final boolean success;
    private final String key;
    private final String value;
    
    @JsonCreator
    public ReplicaResponse(
            @JsonProperty("requestId") String requestId,
            @JsonProperty("responderReplicaId") String responderReplicaId, 
            @JsonProperty("success") boolean success,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value) {
        this.requestId = requestId;
        this.responderReplicaId = responderReplicaId;
        this.success = success;
        this.key = key;
        this.value = value;
    }
    
    // Getters
    public String getRequestId() { 
        return requestId; 
    }
    
    public String getResponderReplicaId() { 
        return responderReplicaId; 
    }
    
    public boolean isSuccess() { 
        return success; 
    }
    
    public String getKey() { 
        return key; 
    }
    
    public String getValue() { 
        return value; 
    }
} 