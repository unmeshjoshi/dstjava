package com.dststore.message;

import java.util.UUID;

/**
 * Response to a SetRequest confirming the operation in the key-value store.
 */
public class SetResponse extends Message {
    private String key;
    private boolean successful;
    private String errorMessage;
    private long valueTimestamp;

    /**
     * Default constructor for Jackson deserialization.
     */
    public SetResponse() {
        super();
    }

    /**
     * Create a new successful SetResponse.
     */
    public SetResponse(String sourceId, String targetId, String key, long valueTimestamp) {
        super(sourceId, targetId);
        this.key = key;
        this.valueTimestamp = valueTimestamp;
        this.successful = true;
    }

    /**
     * Create a new failed SetResponse with an error message.
     */
    public SetResponse(String sourceId, String targetId, String key, String errorMessage) {
        super(sourceId, targetId);
        this.key = key;
        this.errorMessage = errorMessage;
        this.successful = false;
    }

    /**
     * Constructor with all fields.
     */
    public SetResponse(UUID messageId, long timestamp, String sourceId, String targetId, 
                      String key, boolean successful, String errorMessage, long valueTimestamp) {
        super(messageId, timestamp, sourceId, targetId);
        this.key = key;
        this.successful = successful;
        this.errorMessage = errorMessage;
        this.valueTimestamp = valueTimestamp;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public void setSuccessful(boolean successful) {
        this.successful = successful;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public long getValueTimestamp() {
        return valueTimestamp;
    }

    public void setValueTimestamp(long valueTimestamp) {
        this.valueTimestamp = valueTimestamp;
    }

    @Override
    public MessageType getType() {
        return MessageType.SET_RESPONSE;
    }
} 