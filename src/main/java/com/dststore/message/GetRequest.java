package com.dststore.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

/**
 * Message sent to request a value for a key.
 */
public class GetRequest extends Message {
    private final String key;

    /**
     * Default constructor for Jackson deserialization.
     */
    protected GetRequest() {
        super();
        this.key = null;
    }

    /**
     * Creates a new GetRequest.
     *
     * @param sourceId The source node ID
     * @param targetId The target node ID
     * @param key The key to get
     */
    public GetRequest(String sourceId, String targetId, String key) {
        super(sourceId, targetId, MessageType.GET_REQUEST);
        this.key = key;
    }

    /**
     * Creates a new GetRequest with all fields.
     *
     * @param messageId The message ID
     * @param timestamp The message timestamp
     * @param sourceId The source node ID
     * @param targetId The target node ID
     * @param key The key to get
     */
    @JsonCreator
    public GetRequest(
        @JsonProperty("messageId") UUID messageId,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("sourceId") String sourceId,
        @JsonProperty("targetId") String targetId,
        @JsonProperty("key") String key) {
        super(messageId, timestamp, sourceId, targetId, MessageType.GET_REQUEST);
        this.key = key;
    }

    @JsonProperty("key")
    public String getKey() {
        return key;
    }

    @Override
    public MessageType getType() {
        return MessageType.GET_REQUEST;
    }

    @Override
    public Message withSourceId(String newSourceId) {
        return new GetRequest(getMessageId(), getTimestamp(), newSourceId, getTargetId(), key);
    }

    @Override
    public Message withTargetId(String newTargetId) {
        return new GetRequest(getMessageId(), getTimestamp(), getSourceId(), newTargetId, key);
    }
} 