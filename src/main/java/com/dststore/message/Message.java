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
    private final UUID messageId;
    private final long timestamp;
    private final String sourceId;
    private final String targetId;
    private final MessageType type;

    /**
     * Default constructor for Jackson deserialization.
     */
    protected Message() {
        this.messageId = UUID.randomUUID();
        this.timestamp = System.currentTimeMillis();
        this.sourceId = null;
        this.targetId = null;
        this.type = null;
    }

    /**
     * Constructor with all fields.
     */
    protected Message(UUID messageId, long timestamp, String sourceId, String targetId, MessageType type) {
        this.messageId = messageId;
        this.timestamp = timestamp;
        this.sourceId = sourceId;
        this.targetId = targetId;
        this.type = type;
    }

    /**
     * Create a new message with a random UUID and current timestamp.
     */
    protected Message(String sourceId, String targetId, MessageType type) {
        this.messageId = UUID.randomUUID();
        this.timestamp = System.currentTimeMillis();
        this.sourceId = sourceId;
        this.targetId = targetId;
        this.type = type;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getSourceId() {
        return sourceId;
    }

    public String getTargetId() {
        return targetId;
    }

    public MessageType getType() {
        return type;
    }

    /**
     * Creates a new message with the specified source ID.
     *
     * @param newSourceId The new source ID
     * @return A new message with the updated source ID
     */
    public abstract Message withSourceId(String newSourceId);

    /**
     * Creates a new message with the specified target ID.
     *
     * @param newTargetId The new target ID
     * @return A new message with the updated target ID
     */
    public abstract Message withTargetId(String newTargetId);
} 