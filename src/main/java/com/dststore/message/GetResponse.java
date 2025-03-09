package com.dststore.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

/**
 * Response to a GetRequest containing the requested value from the key-value store.
 */
public class GetResponse extends Message {
    private final String key;
    private final String value;
    private final boolean successful;
    private final String errorMessage;
    private final long valueTimestamp;

    /**
     * Default constructor for Jackson deserialization.
     */
    protected GetResponse() {
        super();
        this.key = null;
        this.value = null;
        this.successful = false;
        this.errorMessage = null;
        this.valueTimestamp = 0;
    }

    /**
     * Creates a new GetResponse.
     *
     * @param sourceId The source node ID
     * @param targetId The target node ID
     * @param key The requested key
     * @param value The value found (may be null)
     * @param successful Whether the request was successful
     * @param errorMessage Error message if not successful
     */
    public GetResponse(String sourceId, String targetId, String key, String value, boolean successful, String errorMessage) {
        this(sourceId, targetId, key, value, successful, errorMessage, System.currentTimeMillis());
    }

    /**
     * Creates a new GetResponse with value timestamp.
     *
     * @param sourceId The source node ID
     * @param targetId The target node ID
     * @param key The requested key
     * @param value The value found (may be null)
     * @param successful Whether the request was successful
     * @param errorMessage Error message if not successful
     * @param valueTimestamp The timestamp of the value
     */
    public GetResponse(String sourceId, String targetId, String key, String value, 
                      boolean successful, String errorMessage, long valueTimestamp) {
        super(sourceId, targetId, MessageType.GET_RESPONSE);
        this.key = key;
        this.value = value;
        this.successful = successful;
        this.errorMessage = errorMessage;
        this.valueTimestamp = valueTimestamp;
    }

    /**
     * Creates a new GetResponse with all fields.
     *
     * @param messageId The message ID
     * @param timestamp The message timestamp
     * @param sourceId The source node ID
     * @param targetId The target node ID
     * @param key The requested key
     * @param value The value found (may be null)
     * @param successful Whether the request was successful
     * @param errorMessage Error message if not successful
     * @param valueTimestamp The timestamp of the value
     */
    @JsonCreator
    public GetResponse(
        @JsonProperty("messageId") UUID messageId,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("sourceId") String sourceId,
        @JsonProperty("targetId") String targetId,
        @JsonProperty("key") String key,
        @JsonProperty("value") String value,
        @JsonProperty("successful") boolean successful,
        @JsonProperty("errorMessage") String errorMessage,
        @JsonProperty("valueTimestamp") long valueTimestamp) {
        super(messageId, timestamp, sourceId, targetId, MessageType.GET_RESPONSE);
        this.key = key;
        this.value = value;
        this.successful = successful;
        this.errorMessage = errorMessage;
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
        return MessageType.GET_RESPONSE;
    }

    @Override
    public Message withSourceId(String newSourceId) {
        return new GetResponse(getMessageId(), getTimestamp(), newSourceId, getTargetId(), 
                             key, value, successful, errorMessage, valueTimestamp);
    }

    @Override
    public Message withTargetId(String newTargetId) {
        return new GetResponse(getMessageId(), getTimestamp(), getSourceId(), newTargetId, 
                             key, value, successful, errorMessage, valueTimestamp);
    }
} 