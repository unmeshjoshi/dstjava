package com.dststore.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

/**
 * Response to a SetRequest confirming the operation in the key-value store.
 */
public class SetResponse extends Message {
    private final String key;
    private final boolean successful;
    private final String errorMessage;
    private final long valueTimestamp;

    /**
     * Default constructor for Jackson deserialization.
     */
    protected SetResponse() {
        super();
        this.key = null;
        this.successful = false;
        this.errorMessage = null;
        this.valueTimestamp = 0;
    }

    /**
     * Creates a new SetResponse.
     *
     * @param sourceId The source node ID
     * @param targetId The target node ID
     * @param key The key that was set
     * @param successful Whether the request was successful
     * @param errorMessage Error message if not successful
     */
    public SetResponse(String sourceId, String targetId, String key, boolean successful, String errorMessage) {
        this(sourceId, targetId, key, successful, errorMessage, System.currentTimeMillis());
    }

    /**
     * Creates a new SetResponse with a specific value timestamp.
     *
     * @param sourceId The source node ID
     * @param targetId The target node ID
     * @param key The key that was set
     * @param successful Whether the request was successful
     * @param errorMessage Error message if not successful
     * @param valueTimestamp The timestamp of the value
     */
    public SetResponse(String sourceId, String targetId, String key, boolean successful, String errorMessage, long valueTimestamp) {
        super(sourceId, targetId, MessageType.SET_RESPONSE);
        this.key = key;
        this.successful = successful;
        this.errorMessage = errorMessage;
        this.valueTimestamp = valueTimestamp;
    }

    /**
     * Creates a new SetResponse with all fields.
     *
     * @param messageId The message ID
     * @param timestamp The message timestamp
     * @param sourceId The source node ID
     * @param targetId The target node ID
     * @param key The key that was set
     * @param successful Whether the request was successful
     * @param errorMessage Error message if not successful
     * @param valueTimestamp The timestamp of the value
     */
    @JsonCreator
    public SetResponse(
        @JsonProperty("messageId") UUID messageId,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("sourceId") String sourceId,
        @JsonProperty("targetId") String targetId,
        @JsonProperty("key") String key,
        @JsonProperty("successful") boolean successful,
        @JsonProperty("errorMessage") String errorMessage,
        @JsonProperty("valueTimestamp") long valueTimestamp) {
        super(messageId, timestamp, sourceId, targetId, MessageType.SET_RESPONSE);
        this.key = key;
        this.successful = successful;
        this.errorMessage = errorMessage;
        this.valueTimestamp = valueTimestamp;
    }

    @JsonProperty("key")
    public String getKey() {
        return key;
    }

    @JsonProperty("successful")
    public boolean isSuccessful() {
        return successful;
    }

    @JsonProperty("errorMessage")
    public String getErrorMessage() {
        return errorMessage;
    }

    @JsonProperty("valueTimestamp")
    public long getValueTimestamp() {
        return valueTimestamp;
    }

    @Override
    public MessageType getType() {
        return MessageType.SET_RESPONSE;
    }

    @Override
    public Message withSourceId(String newSourceId) {
        return new SetResponse(getMessageId(), getTimestamp(), newSourceId, getTargetId(), 
                             key, successful, errorMessage, valueTimestamp);
    }

    @Override
    public Message withTargetId(String newTargetId) {
        return new SetResponse(getMessageId(), getTimestamp(), getSourceId(), newTargetId, 
                             key, successful, errorMessage, valueTimestamp);
    }
} 