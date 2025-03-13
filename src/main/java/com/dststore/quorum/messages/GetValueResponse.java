package com.dststore.quorum.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response to a GetValueRequest in the quorum-based key-value store.
 */
public class GetValueResponse {
    private final String messageId;
    private final String key;
    private final String value;
    private final boolean success;
    private final String replicaId;
    private final long version;

    @JsonCreator
    public GetValueResponse(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value,
            @JsonProperty("success") boolean success,
            @JsonProperty("replicaId") String replicaId,
            @JsonProperty("version") long version) {
        this.messageId = messageId;
        this.key = key;
        this.value = value;
        this.success = success;
        this.replicaId = replicaId;
        this.version = version;
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

    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "GetValueResponse{" +
                "messageId='" + messageId + '\'' +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", success=" + success +
                ", replicaId='" + replicaId + '\'' +
                ", version=" + version +
                '}';
    }
} 