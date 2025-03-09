package com.dststore.message;

import java.util.UUID;

/**
 * Response to a GetRequest containing the requested value from the key-value store.
 */
public class GetResponse extends Message {
    private String key;
    private String value;
    private boolean successful;
    private String errorMessage;
    private long valueTimestamp;

    /**
     * Default constructor for Jackson deserialization.
     */
    public GetResponse() {
        super();
    }

    /**
     * Create a new successful GetResponse with a value.
     */
    public GetResponse(String sourceId, String targetId, String key, String value, long valueTimestamp) {
        super(sourceId, targetId);
        this.key = key;
        this.value = value;
        this.valueTimestamp = valueTimestamp;
        this.successful = true;
    }

    /**
     * Create a new failed GetResponse with an error message.
     */
    public GetResponse(String sourceId, String targetId, String key, String errorMessage) {
        super(sourceId, targetId);
        this.key = key;
        this.errorMessage = errorMessage;
        this.successful = false;
    }

    /**
     * Constructor with all fields.
     */
    public GetResponse(UUID messageId, long timestamp, String sourceId, String targetId, 
                      String key, String value, boolean successful, String errorMessage, long valueTimestamp) {
        super(messageId, timestamp, sourceId, targetId);
        this.key = key;
        this.value = value;
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

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
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
        return MessageType.GET_RESPONSE;
    }
} 