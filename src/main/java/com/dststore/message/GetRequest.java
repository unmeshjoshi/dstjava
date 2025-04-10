package com.dststore.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A simple request to get a value for a key from the distributed store.
 */
public class GetRequest {
    private final String messageId;
    private final String key;
    private final String clientId;

    @JsonCreator
    public GetRequest(
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
        return "GetRequest{" +
                "messageId='" + messageId + '\'' +
                ", key='" + key + '\'' +
                ", clientId='" + clientId + '\'' +
                '}';
    }
} 