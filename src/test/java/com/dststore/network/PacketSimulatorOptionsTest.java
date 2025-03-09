package com.dststore.network;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the PacketSimulatorOptions class.
 */
public class PacketSimulatorOptionsTest {
    
    @Test
    void testDefaultConstructor() {
        // Act
        PacketSimulatorOptions options = new PacketSimulatorOptions();
        
        // Assert
        assertThat(options.getPacketLossProbability()).isEqualTo(0.0);
        assertThat(options.getMinDelayTicks()).isEqualTo(1);
        assertThat(options.getMaxDelayTicks()).isEqualTo(1);
        assertThat(options.getRandomSeed()).isEqualTo(42);
        assertThat(options.isCollectStatistics()).isTrue();
    }
    
    @Test
    void testConstructorWithFixedDelay() {
        // Arrange
        double packetLossProbability = 0.1;
        int delayTicks = 5;
        long randomSeed = 123;
        
        // Act
        PacketSimulatorOptions options = new PacketSimulatorOptions(packetLossProbability, delayTicks, randomSeed);
        
        // Assert
        assertThat(options.getPacketLossProbability()).isEqualTo(packetLossProbability);
        assertThat(options.getMinDelayTicks()).isEqualTo(delayTicks);
        assertThat(options.getMaxDelayTicks()).isEqualTo(delayTicks);
        assertThat(options.getRandomSeed()).isEqualTo(randomSeed);
        assertThat(options.isCollectStatistics()).isTrue();
    }
    
    @Test
    void testFullConstructor() {
        // Arrange
        double packetLossProbability = 0.1;
        int minDelayTicks = 2;
        int maxDelayTicks = 5;
        long randomSeed = 123;
        boolean collectStatistics = false;
        
        // Act
        PacketSimulatorOptions options = new PacketSimulatorOptions(
            packetLossProbability, minDelayTicks, maxDelayTicks, randomSeed, collectStatistics);
        
        // Assert
        assertThat(options.getPacketLossProbability()).isEqualTo(packetLossProbability);
        assertThat(options.getMinDelayTicks()).isEqualTo(minDelayTicks);
        assertThat(options.getMaxDelayTicks()).isEqualTo(maxDelayTicks);
        assertThat(options.getRandomSeed()).isEqualTo(randomSeed);
        assertThat(options.isCollectStatistics()).isEqualTo(collectStatistics);
    }
    
    @Test
    void testBuilder() {
        // Act
        PacketSimulatorOptions options = PacketSimulatorOptions.builder()
            .packetLossProbability(0.2)
            .delayRange(3, 8)
            .randomSeed(456)
            .collectStatistics(false)
            .build();
        
        // Assert
        assertThat(options.getPacketLossProbability()).isEqualTo(0.2);
        assertThat(options.getMinDelayTicks()).isEqualTo(3);
        assertThat(options.getMaxDelayTicks()).isEqualTo(8);
        assertThat(options.getRandomSeed()).isEqualTo(456);
        assertThat(options.isCollectStatistics()).isFalse();
    }
    
    @Test
    void testBuilderWithFixedDelay() {
        // Act
        PacketSimulatorOptions options = PacketSimulatorOptions.builder()
            .packetLossProbability(0.15)
            .fixedDelay(5)
            .build();
        
        // Assert
        assertThat(options.getPacketLossProbability()).isEqualTo(0.15);
        assertThat(options.getMinDelayTicks()).isEqualTo(5);
        assertThat(options.getMaxDelayTicks()).isEqualTo(5);
        // Default values for unspecified fields
        assertThat(options.getRandomSeed()).isEqualTo(42);
        assertThat(options.isCollectStatistics()).isTrue();
    }
    
    @Test
    void testInvalidPacketLossProbability() {
        // Negative probability
        assertThatThrownBy(() -> 
            new PacketSimulatorOptions(-0.1, 1, 1, 42, true)
        ).isInstanceOf(IllegalArgumentException.class);
        
        // Probability > 1.0
        assertThatThrownBy(() -> 
            new PacketSimulatorOptions(1.1, 1, 1, 42, true)
        ).isInstanceOf(IllegalArgumentException.class);
    }
    
    @Test
    void testInvalidDelays() {
        // Negative min delay
        assertThatThrownBy(() -> 
            new PacketSimulatorOptions(0.1, -1, 5, 42, true)
        ).isInstanceOf(IllegalArgumentException.class);
        
        // Max delay < min delay
        assertThatThrownBy(() -> 
            new PacketSimulatorOptions(0.1, 5, 3, 42, true)
        ).isInstanceOf(IllegalArgumentException.class);
    }
} 