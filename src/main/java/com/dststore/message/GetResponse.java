package com.dststore.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response from a GET operation containing the requested value.
 * Includes metadata about the operation's success and the replica that responded.
 */
public class GetResponse {
    private final String messageId;
    private final String key;
    private final String value;
    private final boolean success;
    private final String replicaId;

    @JsonCreator
    public GetResponse(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value,
            @JsonProperty("success") boolean success,
            @JsonProperty("replicaId") String replicaId) {
        this.messageId = messageId;
        this.key = key;
        this.value = value;
        this.success = success;
        this.replicaId = replicaId;
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

    public boolean isSuccess() {
        return success;
    }

    public String getReplicaId() {
        return replicaId;
    }

    @Override
    public String toString() {
        return "GetResponse{" +
                "messageId='" + messageId + '\'' +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", success=" + success +
                ", replicaId='" + replicaId + '\'' +
                '}';
    }
} 