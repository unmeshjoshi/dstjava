package com.dststore.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

/**
 * Message sent to set a value for a key.
 */
public class SetRequest extends Message {
    private final String key;
    private final String value;
    private final long valueTimestamp;

    /**
     * Default constructor for Jackson deserialization.
     */
    protected SetRequest() {
        super();
        this.key = null;
        this.value = null;
        this.valueTimestamp = 0;
    }

    /**
     * Creates a new SetRequest.
     *
     * @param sourceId The source node ID
     * @param targetId The target node ID
     * @param key The key to set
     * @param value The value to set
     */
    public SetRequest(String sourceId, String targetId, String key, String value) {
        this(sourceId, targetId, key, value, System.currentTimeMillis());
    }

    /**
     * Creates a new SetRequest with a specific value timestamp.
     *
     * @param sourceId The source node ID
     * @param targetId The target node ID
     * @param key The key to set
     * @param value The value to set
     * @param valueTimestamp The timestamp of the value
     */
    public SetRequest(String sourceId, String targetId, String key, String value, long valueTimestamp) {
        super(sourceId, targetId, MessageType.SET_REQUEST);
        this.key = key;
        this.value = value;
        this.valueTimestamp = valueTimestamp;
    }

    /**
     * Creates a new SetRequest with all fields.
     *
     * @param messageId The message ID
     * @param timestamp The message timestamp
     * @param sourceId The source node ID
     * @param targetId The target node ID
     * @param key The key to set
     * @param value The value to set
     * @param valueTimestamp The timestamp of the value
     */
    @JsonCreator
    public SetRequest(
        @JsonProperty("messageId") UUID messageId,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("sourceId") String sourceId,
        @JsonProperty("targetId") String targetId,
        @JsonProperty("key") String key,
        @JsonProperty("value") String value,
        @JsonProperty("valueTimestamp") long valueTimestamp) {
        super(messageId, timestamp, sourceId, targetId, MessageType.SET_REQUEST);
        this.key = key;
        this.value = value;
        this.valueTimestamp = valueTimestamp;
    }

    @JsonProperty("key")
    public String getKey() {
        return key;
    }

    @JsonProperty("value")
    public String getValue() {
        return value;
    }

    @JsonProperty("valueTimestamp")
    public long getValueTimestamp() {
        return valueTimestamp;
    }

    @Override
    public MessageType getType() {
        return MessageType.SET_REQUEST;
    }

    @Override
    public Message withSourceId(String newSourceId) {
        return new SetRequest(getMessageId(), getTimestamp(), newSourceId, getTargetId(), key, value, valueTimestamp);
    }

    @Override
    public Message withTargetId(String newTargetId) {
        return new SetRequest(getMessageId(), getTimestamp(), getSourceId(), newTargetId, key, value, valueTimestamp);
    }
} 