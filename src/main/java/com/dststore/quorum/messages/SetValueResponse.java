package com.dststore.quorum.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response to a SetValueRequest in the quorum-based key-value store.
 */
public class SetValueResponse {
    private final String messageId;
    private final String key;
    private final boolean success;
    private final String replicaId;
    private final long version;

    @JsonCreator
    public SetValueResponse(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("key") String key,
            @JsonProperty("success") boolean success,
            @JsonProperty("replicaId") String replicaId,
            @JsonProperty("version") long version) {
        this.messageId = messageId;
        this.key = key;
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
        return "SetValueResponse{" +
                "messageId='" + messageId + '\'' +
                ", key='" + key + '\'' +
                ", success=" + success +
                ", replicaId='" + replicaId + '\'' +
                ", version=" + version +
                '}';
    }
} 