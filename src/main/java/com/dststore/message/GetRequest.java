package com.dststore.message;

import java.util.UUID;

/**
 * Message to request a value from the key-value store.
 */
public class GetRequest extends Message {
    private String key;

    /**
     * Default constructor for Jackson deserialization.
     */
    public GetRequest() {
        super();
    }

    /**
     * Create a new GetRequest with the specified key.
     */
    public GetRequest(String sourceId, String targetId, String key) {
        super(sourceId, targetId);
        this.key = key;
    }

    /**
     * Constructor with all fields.
     */
    public GetRequest(UUID messageId, long timestamp, String sourceId, String targetId, String key) {
        super(messageId, timestamp, sourceId, targetId);
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public MessageType getType() {
        return MessageType.GET_REQUEST;
    }
} 