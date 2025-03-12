package com.dststore.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PutRequest {
    private final String messageId;
    private final String key;
    private final String value;
    private final String clientId;
    
    @JsonCreator
    public PutRequest(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value,
            @JsonProperty("clientId") String clientId) {
        this.messageId = messageId;
        this.key = key;
        this.value = value;
        this.clientId = clientId;
    }
    
    public String getMessageId() {
        return messageId;
    }
    
    public String getKey() {
        return key;
    }
    
    public String getValue() {
        return value;
    }
    
    public String getClientId() {
        return clientId;
    }
} 