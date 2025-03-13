package com.dststore.quorum.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request to set a value with a specific version in the quorum-based key-value store.
 * Used for read-repair operations.
 */
public class VersionedSetValueRequest {
    private final String messageId;
    private final String key;
    private final String value;
    private final String originReplicaId;
    private final long version;

    @JsonCreator
    public VersionedSetValueRequest(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value,
            @JsonProperty("originReplicaId") String originReplicaId,
            @JsonProperty("version") long version) {
        this.messageId = messageId;
        this.key = key;
        this.value = value;
        this.originReplicaId = originReplicaId;
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

    public String getOriginReplicaId() {
        return originReplicaId;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "VersionedSetValueRequest{" +
                "messageId='" + messageId + '\'' +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", originReplicaId='" + originReplicaId + '\'' +
                ", version=" + version +
                '}';
    }
} 