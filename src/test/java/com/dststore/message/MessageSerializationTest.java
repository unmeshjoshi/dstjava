package com.dststore.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for message serialization and deserialization.
 */
public class MessageSerializationTest {
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    void testSetRequestSerialization() throws Exception {
        // Arrange
        UUID messageId = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        String sourceId = "client-1";
        String targetId = "replica-1";
        String key = "test-key";
        String value = "test-value";
        long valueTimestamp = timestamp;

        SetRequest request = new SetRequest(messageId, timestamp, sourceId, targetId, key, value, valueTimestamp);

        // Act
        String json = objectMapper.writeValueAsString(request);
        Message deserialized = objectMapper.readValue(json, Message.class);

        // Assert
        assertThat(deserialized).isInstanceOf(SetRequest.class);
        SetRequest deserializedRequest = (SetRequest) deserialized;
        assertThat(deserializedRequest.getMessageId()).isEqualTo(messageId);
        assertThat(deserializedRequest.getTimestamp()).isEqualTo(timestamp);
        assertThat(deserializedRequest.getSourceId()).isEqualTo(sourceId);
        assertThat(deserializedRequest.getTargetId()).isEqualTo(targetId);
        assertThat(deserializedRequest.getKey()).isEqualTo(key);
        assertThat(deserializedRequest.getValue()).isEqualTo(value);
        assertThat(deserializedRequest.getValueTimestamp()).isEqualTo(valueTimestamp);
        assertThat(deserializedRequest.getType()).isEqualTo(MessageType.SET_REQUEST);
    }

    @Test
    void testSetResponseSerialization() throws Exception {
        // Arrange
        UUID messageId = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        String sourceId = "replica-1";
        String targetId = "client-1";
        String key = "test-key";
        boolean successful = true;
        String errorMessage = null;
        long valueTimestamp = timestamp;

        SetResponse response = new SetResponse(messageId, timestamp, sourceId, targetId, key, successful, errorMessage, valueTimestamp);

        // Act
        String json = objectMapper.writeValueAsString(response);
        Message deserialized = objectMapper.readValue(json, Message.class);

        // Assert
        assertThat(deserialized).isInstanceOf(SetResponse.class);
        SetResponse deserializedResponse = (SetResponse) deserialized;
        assertThat(deserializedResponse.getMessageId()).isEqualTo(messageId);
        assertThat(deserializedResponse.getTimestamp()).isEqualTo(timestamp);
        assertThat(deserializedResponse.getSourceId()).isEqualTo(sourceId);
        assertThat(deserializedResponse.getTargetId()).isEqualTo(targetId);
        assertThat(deserializedResponse.getKey()).isEqualTo(key);
        assertThat(deserializedResponse.isSuccessful()).isEqualTo(successful);
        assertThat(deserializedResponse.getErrorMessage()).isEqualTo(errorMessage);
        assertThat(deserializedResponse.getValueTimestamp()).isEqualTo(valueTimestamp);
        assertThat(deserializedResponse.getType()).isEqualTo(MessageType.SET_RESPONSE);
    }

    @Test
    void testGetRequestSerialization() throws Exception {
        // Arrange
        UUID messageId = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        String sourceId = "client-1";
        String targetId = "replica-1";
        String key = "test-key";

        GetRequest request = new GetRequest(messageId, timestamp, sourceId, targetId, key);

        // Act
        String json = objectMapper.writeValueAsString(request);
        Message deserialized = objectMapper.readValue(json, Message.class);

        // Assert
        assertThat(deserialized).isInstanceOf(GetRequest.class);
        GetRequest deserializedRequest = (GetRequest) deserialized;
        assertThat(deserializedRequest.getMessageId()).isEqualTo(messageId);
        assertThat(deserializedRequest.getTimestamp()).isEqualTo(timestamp);
        assertThat(deserializedRequest.getSourceId()).isEqualTo(sourceId);
        assertThat(deserializedRequest.getTargetId()).isEqualTo(targetId);
        assertThat(deserializedRequest.getKey()).isEqualTo(key);
        assertThat(deserializedRequest.getType()).isEqualTo(MessageType.GET_REQUEST);
    }

    @Test
    void testGetResponseSerialization() throws Exception {
        // Arrange
        UUID messageId = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        String sourceId = "replica-1";
        String targetId = "client-1";
        String key = "test-key";
        String value = "test-value";
        boolean successful = true;
        String errorMessage = null;
        long valueTimestamp = timestamp;

        GetResponse response = new GetResponse(messageId, timestamp, sourceId, targetId, key, value, successful, errorMessage, valueTimestamp);

        // Act
        String json = objectMapper.writeValueAsString(response);
        Message deserialized = objectMapper.readValue(json, Message.class);

        // Assert
        assertThat(deserialized).isInstanceOf(GetResponse.class);
        GetResponse deserializedResponse = (GetResponse) deserialized;
        assertThat(deserializedResponse.getMessageId()).isEqualTo(messageId);
        assertThat(deserializedResponse.getTimestamp()).isEqualTo(timestamp);
        assertThat(deserializedResponse.getSourceId()).isEqualTo(sourceId);
        assertThat(deserializedResponse.getTargetId()).isEqualTo(targetId);
        assertThat(deserializedResponse.getKey()).isEqualTo(key);
        assertThat(deserializedResponse.getValue()).isEqualTo(value);
        assertThat(deserializedResponse.isSuccessful()).isEqualTo(successful);
        assertThat(deserializedResponse.getErrorMessage()).isEqualTo(errorMessage);
        assertThat(deserializedResponse.getValueTimestamp()).isEqualTo(valueTimestamp);
        assertThat(deserializedResponse.getType()).isEqualTo(MessageType.GET_RESPONSE);
    }

    @Test
    void testErrorResponseSerialization() throws Exception {
        // Arrange
        UUID messageId = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        String sourceId = "replica-1";
        String targetId = "client-1";
        String key = "test-key";
        String errorMessage = "Key not found";

        GetResponse response = new GetResponse(sourceId, targetId, key, errorMessage);
        response.setMessageId(messageId);
        response.setTimestamp(timestamp);

        // Act
        String json = objectMapper.writeValueAsString(response);
        Message deserialized = objectMapper.readValue(json, Message.class);

        // Assert
        assertThat(deserialized).isInstanceOf(GetResponse.class);
        GetResponse deserializedResponse = (GetResponse) deserialized;
        assertThat(deserializedResponse.getMessageId()).isEqualTo(messageId);
        assertThat(deserializedResponse.getTimestamp()).isEqualTo(timestamp);
        assertThat(deserializedResponse.getSourceId()).isEqualTo(sourceId);
        assertThat(deserializedResponse.getTargetId()).isEqualTo(targetId);
        assertThat(deserializedResponse.getKey()).isEqualTo(key);
        assertThat(deserializedResponse.isSuccessful()).isFalse();
        assertThat(deserializedResponse.getErrorMessage()).isEqualTo(errorMessage);
        assertThat(deserializedResponse.getType()).isEqualTo(MessageType.GET_RESPONSE);
    }
} 