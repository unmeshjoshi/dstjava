package com.dststore.quorum.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request to get a value from the quorum-based key-value store.
 */
public class GetValueRequest {
    private final String messageId;
    private final String key;
    private final String clientId;

    @JsonCreator
    public GetValueRequest(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("key") String key,
            @JsonProperty("clientId") String clientId) {
        this.messageId = messageId;
        this.key = key;
        this.clientId = clientId;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getKey() {
        return key;
    }

    public String getClientId() {
        return clientId;
    }

    @Override
    public String toString() {
        return "GetValueRequest{" +
                "messageId='" + messageId + '\'' +
                ", key='" + key + '\'' +
                ", clientId='" + clientId + '\'' +
                '}';
    }
} 