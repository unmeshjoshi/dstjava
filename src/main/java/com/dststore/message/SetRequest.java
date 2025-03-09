package com.dststore.message;

import java.util.UUID;

/**
 * Message to set a value in the key-value store.
 */
public class SetRequest extends Message {
    private String key;
    private String value;
    private long valueTimestamp;

    /**
     * Default constructor for Jackson deserialization.
     */
    public SetRequest() {
        super();
    }

    /**
     * Create a new SetRequest with the specified key and value.
     * The value timestamp will be set to the current time.
     */
    public SetRequest(String sourceId, String targetId, String key, String value) {
        super(sourceId, targetId);
        this.key = key;
        this.value = value;
        this.valueTimestamp = System.currentTimeMillis();
    }

    /**
     * Create a new SetRequest with the specified key, value, and value timestamp.
     */
    public SetRequest(String sourceId, String targetId, String key, String value, long valueTimestamp) {
        super(sourceId, targetId);
        this.key = key;
        this.value = value;
        this.valueTimestamp = valueTimestamp;
    }

    /**
     * Constructor with all fields.
     */
    public SetRequest(UUID messageId, long timestamp, String sourceId, String targetId, 
                     String key, String value, long valueTimestamp) {
        super(messageId, timestamp, sourceId, targetId);
        this.key = key;
        this.value = value;
        this.valueTimestamp = valueTimestamp;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getValueTimestamp() {
        return valueTimestamp;
    }

    public void setValueTimestamp(long valueTimestamp) {
        this.valueTimestamp = valueTimestamp;
    }

    @Override
    public MessageType getType() {
        return MessageType.SET_REQUEST;
    }
} 