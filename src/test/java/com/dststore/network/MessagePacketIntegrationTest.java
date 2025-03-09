package com.dststore.network;

import com.dststore.message.GetRequest;
import com.dststore.message.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests between message serialization and packet transport.
 */
public class MessagePacketIntegrationTest {
    
    private ObjectMapper objectMapper;
    
    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }
    
    @Test
    void testMessageSerializationIntoPacket() throws Exception {
        // Arrange
        String sourceId = "client-1";
        String targetId = "replica-1";
        String key = "test-key";
        
        GetRequest originalRequest = new GetRequest(sourceId, targetId, key);
        Path path = new Path(sourceId, targetId);
        
        // Act - Serialize message into a packet
        byte[] serializedMessage = objectMapper.writeValueAsBytes(originalRequest);
        Packet packet = new Packet(path, serializedMessage, 1);
        
        // Act - Extract and deserialize message from packet
        byte[] extractedPayload = packet.getPayload();
        Message deserializedMessage = objectMapper.readValue(extractedPayload, Message.class);
        
        // Assert
        assertThat(deserializedMessage).isInstanceOf(GetRequest.class);
        GetRequest deserializedRequest = (GetRequest) deserializedMessage;
        assertThat(deserializedRequest.getSourceId()).isEqualTo(sourceId);
        assertThat(deserializedRequest.getTargetId()).isEqualTo(targetId);
        assertThat(deserializedRequest.getKey()).isEqualTo(key);
    }
    
    @Test
    void testPacketRoundTripWithSerialization() throws Exception {
        // Arrange
        String clientId = "client-1";
        String replicaId = "replica-1";
        String key = "test-key";
        
        // Create request from client to replica
        GetRequest request = new GetRequest(clientId, replicaId, key);
        Path requestPath = new Path(clientId, replicaId);
        
        // Act - Client serializes request and creates packet
        byte[] serializedRequest = objectMapper.writeValueAsBytes(request);
        Packet requestPacket = new Packet(requestPath, serializedRequest, 1);
        
        // Act - Replica receives packet and deserializes the message
        byte[] receivedPayload = requestPacket.getPayload();
        Message receivedMessage = objectMapper.readValue(receivedPayload, Message.class);
        
        // Simulate replica creating a response
        byte[] responsePayload = objectMapper.writeValueAsBytes(receivedMessage);
        Packet responsePacket = requestPacket.createResponse(responsePayload, 2);
        
        // Act - Client receives response packet and deserializes
        byte[] clientReceivedPayload = responsePacket.getPayload();
        Message clientReceivedMessage = objectMapper.readValue(clientReceivedPayload, Message.class);
        
        // Assert
        assertThat(clientReceivedMessage).isInstanceOf(GetRequest.class);
        GetRequest finalRequest = (GetRequest) clientReceivedMessage;
        assertThat(finalRequest.getSourceId()).isEqualTo(clientId);
        assertThat(finalRequest.getTargetId()).isEqualTo(replicaId);
        assertThat(finalRequest.getKey()).isEqualTo(key);
        
        // Verify path is correctly reversed in response
        assertThat(responsePacket.getPath().getSourceId()).isEqualTo(replicaId);
        assertThat(responsePacket.getPath().getTargetId()).isEqualTo(clientId);
    }
} 