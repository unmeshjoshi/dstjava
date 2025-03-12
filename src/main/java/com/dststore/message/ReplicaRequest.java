package com.dststore.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ReplicaRequest {
    private final String requestId;
    private final String originReplicaId;
    private final String clientId;
    private final String key;
    private final String operation; // "GET" or "SET"
    private final String value;     // Used for SET operations
    
    @JsonCreator
    public ReplicaRequest(
            @JsonProperty("requestId") String requestId,
            @JsonProperty("originReplicaId") String originReplicaId,
            @JsonProperty("clientId") String clientId, 
            @JsonProperty("key") String key,
            @JsonProperty("operation") String operation,
            @JsonProperty("value") String value) {
        this.requestId = requestId;
        this.originReplicaId = originReplicaId;
        this.clientId = clientId;
        this.key = key;
        this.operation = operation;
        this.value = value;
    }
    
    // Getters
    public String getRequestId() { 
        return requestId; 
    }
    
    public String getOriginReplicaId() { 
        return originReplicaId; 
    }
    
    public String getClientId() { 
        return clientId; 
    }
    
    public String getKey() { 
        return key; 
    }
    
    public String getOperation() { 
        return operation; 
    }
    
    public String getValue() { 
        return value; 
    }
} 