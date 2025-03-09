package com.dststore.network;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the PacketSimulatorStats class.
 */
public class PacketSimulatorStatsTest {
    
    private PacketSimulatorStats stats;
    private Path path1;
    private Path path2;
    
    @BeforeEach
    void setUp() {
        stats = new PacketSimulatorStats();
        path1 = new Path("node-1", "node-2");
        path2 = new Path("node-2", "node-3");
    }
    
    @Test
    void testRecordPacketEnqueued() {
        // Arrange
        Packet packet1 = new Packet(path1, "payload1".getBytes(StandardCharsets.UTF_8), 10);
        Packet packet2 = new Packet(path1, "payload2".getBytes(StandardCharsets.UTF_8), 11);
        Packet packet3 = new Packet(path2, "payload3".getBytes(StandardCharsets.UTF_8), 12);
        
        // Act
        stats.recordPacketEnqueued(packet1);
        stats.recordPacketEnqueued(packet2);
        stats.recordPacketEnqueued(packet3);
        
        // Assert
        assertThat(stats.getPacketsEnqueued()).isEqualTo(3);
        assertThat(stats.getPacketsForPath(path1)).isEqualTo(2);
        assertThat(stats.getPacketsForPath(path2)).isEqualTo(1);
    }
    
    @Test
    void testRecordPacketDelivered() {
        // Arrange
        Packet packet1 = new Packet(path1, "payload1".getBytes(StandardCharsets.UTF_8), 10);
        Packet packet2 = new Packet(path1, "payload2".getBytes(StandardCharsets.UTF_8), 10);
        
        // Act
        stats.recordPacketDelivered(packet1, 15); // 5 tick delivery time
        stats.recordPacketDelivered(packet2, 17); // 7 tick delivery time
        
        // Assert
        assertThat(stats.getPacketsDelivered()).isEqualTo(2);
        assertThat(stats.getAverageDeliveryTime()).isEqualTo(6.0); // (5 + 7) / 2
    }
    
    @Test
    void testRecordPacketDropped() {
        // Arrange
        Packet packet1 = new Packet(path1, "payload1".getBytes(StandardCharsets.UTF_8), 10);
        Packet packet2 = new Packet(path2, "payload2".getBytes(StandardCharsets.UTF_8), 11);
        
        // Act
        stats.recordPacketEnqueued(packet1);
        stats.recordPacketEnqueued(packet2);
        stats.recordPacketDropped(packet1);
        
        // Assert
        assertThat(stats.getPacketsDropped()).isEqualTo(1);
        assertThat(stats.getDropRate()).isEqualTo(0.5); // 1 out of 2 dropped
    }
    
    @Test
    void testReset() {
        // Arrange
        Packet packet = new Packet(path1, "payload".getBytes(StandardCharsets.UTF_8), 10);
        stats.recordPacketEnqueued(packet);
        stats.recordPacketDelivered(packet, 15);
        
        // Act
        stats.reset();
        
        // Assert
        assertThat(stats.getPacketsEnqueued()).isZero();
        assertThat(stats.getPacketsDelivered()).isZero();
        assertThat(stats.getPacketsDropped()).isZero();
        assertThat(stats.getPacketsForPath(path1)).isZero();
        assertThat(stats.getAverageDeliveryTime()).isZero();
        assertThat(stats.getDropRate()).isZero();
    }
    
    @Test
    void testGetStatsForUnknownPath() {
        // Arrange
        Path unknownPath = new Path("unknown-1", "unknown-2");
        
        // Act & Assert
        assertThat(stats.getPacketsForPath(unknownPath)).isZero();
    }
    
    @Test
    void testAverageDeliveryTimeWhenNoPacketsDelivered() {
        // Act & Assert
        assertThat(stats.getAverageDeliveryTime()).isZero();
    }
    
    @Test
    void testDropRateWhenNoPacketsEnqueued() {
        // Act & Assert
        assertThat(stats.getDropRate()).isZero();
    }
    
    @Test
    void testToString() {
        // Arrange
        Packet packet1 = new Packet(path1, "payload1".getBytes(StandardCharsets.UTF_8), 10);
        Packet packet2 = new Packet(path1, "payload2".getBytes(StandardCharsets.UTF_8), 10);
        stats.recordPacketEnqueued(packet1);
        stats.recordPacketEnqueued(packet2);
        stats.recordPacketDelivered(packet1, 15);
        stats.recordPacketDropped(packet2);
        
        // Act
        String result = stats.toString();
        
        // Assert
        assertThat(result).contains("enqueued=2");
        assertThat(result).contains("delivered=1");
        assertThat(result).contains("dropped=1");
        assertThat(result).contains("dropRate=50.00%");
    }
} 