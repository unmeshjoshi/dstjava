package com.dststore.network;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the Packet class.
 */
public class PacketTest {
    
    @Test
    void testPacketCreation() {
        // Arrange
        Path path = new Path("replica-1", "replica-2");
        byte[] payload = "Test payload".getBytes(StandardCharsets.UTF_8);
        long createdAtTick = 42;
        
        // Act
        Packet packet = new Packet(path, payload, createdAtTick);
        
        // Assert
        assertThat(packet.getPacketId()).isNotNull();
        assertThat(packet.getPath()).isEqualTo(path);
        assertThat(packet.getCreatedAtTick()).isEqualTo(createdAtTick);
        
        // Verify payload is a copy (defensive)
        assertThat(packet.getPayload()).isEqualTo(payload);
        assertThat(packet.getPayload()).isNotSameAs(payload);
    }
    
    @Test
    void testPacketCreationWithId() {
        // Arrange
        UUID packetId = UUID.randomUUID();
        Path path = new Path("replica-1", "replica-2");
        byte[] payload = "Test payload".getBytes(StandardCharsets.UTF_8);
        long createdAtTick = 42;
        
        // Act
        Packet packet = new Packet(packetId, path, payload, createdAtTick);
        
        // Assert
        assertThat(packet.getPacketId()).isEqualTo(packetId);
        assertThat(packet.getPath()).isEqualTo(path);
        assertThat(packet.getPayload()).isEqualTo(payload);
        assertThat(packet.getCreatedAtTick()).isEqualTo(createdAtTick);
    }
    
    @Test
    void testPayloadDefensiveCopy() {
        // Arrange
        Path path = new Path("replica-1", "replica-2");
        byte[] payload = "Original payload".getBytes(StandardCharsets.UTF_8);
        Packet packet = new Packet(path, payload, 42);
        
        // Act - modify the original payload
        Arrays.fill(payload, (byte) 0);
        
        // Assert - packet's payload should be unchanged
        assertThat(new String(packet.getPayload(), StandardCharsets.UTF_8))
            .isEqualTo("Original payload");
        
        // Act - try to modify the returned payload
        byte[] returnedPayload = packet.getPayload();
        Arrays.fill(returnedPayload, (byte) 0);
        
        // Assert - packet's internal payload should still be unchanged
        assertThat(new String(packet.getPayload(), StandardCharsets.UTF_8))
            .isEqualTo("Original payload");
    }
    
    @Test
    void testCreateResponse() {
        // Arrange
        Path originalPath = new Path("replica-1", "replica-2");
        byte[] originalPayload = "Request payload".getBytes(StandardCharsets.UTF_8);
        long originalTick = 42;
        Packet original = new Packet(originalPath, originalPayload, originalTick);
        
        byte[] responsePayload = "Response payload".getBytes(StandardCharsets.UTF_8);
        long currentTick = 43;
        
        // Act
        Packet response = original.createResponse(responsePayload, currentTick);
        
        // Assert
        assertThat(response.getPath()).isEqualTo(originalPath.reverse());
        assertThat(response.getPayload()).isEqualTo(responsePayload);
        assertThat(response.getCreatedAtTick()).isEqualTo(currentTick);
    }
    
    @Test
    void testToString() {
        // Arrange
        UUID packetId = UUID.randomUUID();
        Path path = new Path("replica-1", "replica-2");
        byte[] payload = "Test payload".getBytes(StandardCharsets.UTF_8);
        long createdAtTick = 42;
        Packet packet = new Packet(packetId, path, payload, createdAtTick);
        
        // Act
        String toString = packet.toString();
        
        // Assert
        assertThat(toString).contains(packetId.toString());
        assertThat(toString).contains(path.toString());
        assertThat(toString).contains(String.valueOf(payload.length));
        assertThat(toString).contains(String.valueOf(createdAtTick));
    }
} 