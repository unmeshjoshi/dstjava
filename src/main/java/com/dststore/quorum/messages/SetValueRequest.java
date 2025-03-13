package com.dststore.quorum.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request to set a value in the quorum-based key-value store.
 */
public class SetValueRequest {
    private final String messageId;
    private final String key;
    private final String value;
    private final String clientId;
    private final long version; // Optional version for conditional updates

    @JsonCreator
    public SetValueRequest(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value,
            @JsonProperty("clientId") String clientId,
            @JsonProperty("version") long version) {
        this.messageId = messageId;
        this.key = key;
        this.value = value;
        this.clientId = clientId;
        this.version = version;
    }

    // Constructor without version for unconditional updates
    public SetValueRequest(String messageId, String key, String value, String clientId) {
        this(messageId, key, value, clientId, 0);
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

    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "SetValueRequest{" +
                "messageId='" + messageId + '\'' +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", clientId='" + clientId + '\'' +
                ", version=" + version +
                '}';
    }
} 