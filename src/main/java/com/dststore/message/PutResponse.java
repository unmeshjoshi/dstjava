package com.dststore.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PutResponse {
    private final String messageId;
    private final String key;
    private final boolean success;
    private final String responderReplicaId;
    
    @JsonCreator
    public PutResponse(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("key") String key,
            @JsonProperty("success") boolean success,
            @JsonProperty("responderReplicaId") String responderReplicaId) {
        this.messageId = messageId;
        this.key = key;
        this.success = success;
        this.responderReplicaId = responderReplicaId;
    }
    
    public String getMessageId() {
        return messageId;
    }
    
    public String getKey() {
        return key;
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public String getResponderReplicaId() {
        return responderReplicaId;
    }
} 