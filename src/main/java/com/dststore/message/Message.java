package com.dststore.message;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.UUID;

/**
 * Base class for all messages in the DST Store system.
 * Uses Jackson annotations for proper JSON serialization/deserialization.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME, 
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = SetRequest.class, name = "SetRequest"),
    @JsonSubTypes.Type(value = SetResponse.class, name = "SetResponse"),
    @JsonSubTypes.Type(value = GetRequest.class, name = "GetRequest"),
    @JsonSubTypes.Type(value = GetResponse.class, name = "GetResponse")
})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class Message {
    private UUID messageId;
    private long timestamp;
    private String sourceId;
    private String targetId;

    /**
     * Default constructor for Jackson deserialization.
     */
    protected Message() {
        this.messageId = UUID.randomUUID();
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Constructor with all fields.
     */
    protected Message(UUID messageId, long timestamp, String sourceId, String targetId) {
        this.messageId = messageId;
        this.timestamp = timestamp;
        this.sourceId = sourceId;
        this.targetId = targetId;
    }

    /**
     * Create a new message with a random UUID and current timestamp.
     */
    protected Message(String sourceId, String targetId) {
        this.messageId = UUID.randomUUID();
        this.timestamp = System.currentTimeMillis();
        this.sourceId = sourceId;
        this.targetId = targetId;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public void setMessageId(UUID messageId) {
        this.messageId = messageId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    /**
     * Get the type of message for type discrimination.
     */
    public abstract MessageType getType();
} 