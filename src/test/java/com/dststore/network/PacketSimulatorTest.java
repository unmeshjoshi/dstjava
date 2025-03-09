package com.dststore.network;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the PacketSimulator class.
 */
public class PacketSimulatorTest {
    
    private PacketSimulator simulator;
    private Path path;
    private List<Packet> deliveredPackets;
    
    @BeforeEach
    void setUp() {
        PacketSimulatorOptions options = new PacketSimulatorOptions();
        simulator = new PacketSimulator(options);
        path = new Path("node-1", "node-2");
        deliveredPackets = new ArrayList<>();
        
        // Register a listener that collects delivered packets
        simulator.registerListener("node-2", (packet, tick) -> deliveredPackets.add(packet));
    }
    
    @Test
    void testTickIncrementsCurrentTick() {
        // Arrange
        long initialTick = simulator.getCurrentTick();
        
        // Act
        simulator.tick();
        
        // Assert
        assertThat(simulator.getCurrentTick()).isEqualTo(initialTick + 1);
    }
    
    @Test
    void testTickMultipleIncrementsCurrentTick() {
        // Arrange
        long initialTick = simulator.getCurrentTick();
        
        // Act
        simulator.tick(5);
        
        // Assert
        assertThat(simulator.getCurrentTick()).isEqualTo(initialTick + 5);
    }
    
    @Test
    void testPacketDeliveryWithFixedDelay() {
        // Arrange
        // Create a simulator with fixed 3-tick delay
        PacketSimulatorOptions options = PacketSimulatorOptions.builder()
            .fixedDelay(3)
            .packetLossProbability(0.0) // No packet loss
            .build();
        simulator = new PacketSimulator(options);
        simulator.registerListener("node-2", (packet, tick) -> deliveredPackets.add(packet));
        
        byte[] payload = "test payload".getBytes(StandardCharsets.UTF_8);
        Packet packet = new Packet(path, payload, 0);
        
        // Act
        simulator.enqueuePacket(packet);
        
        // Assert - packet shouldn't be delivered yet
        assertThat(deliveredPackets).isEmpty();
        
        // Act - advance 2 ticks (total: 2)
        simulator.tick(2);
        
        // Assert - packet still shouldn't be delivered
        assertThat(deliveredPackets).isEmpty();
        
        // Act - advance 1 more tick (total: 3)
        simulator.tick();
        
        // Assert - packet should be delivered at tick 3
        assertThat(deliveredPackets).hasSize(1);
        assertThat(deliveredPackets.get(0)).isEqualTo(packet);
        assertThat(simulator.getCurrentTick()).isEqualTo(3);
    }
    
    @Test
    void testPacketDropBasedOnProbability() {
        // Arrange - create a simulator that drops all packets
        PacketSimulatorOptions options = PacketSimulatorOptions.builder()
            .packetLossProbability(1.0) // 100% packet loss
            .build();
        simulator = new PacketSimulator(options);
        
        byte[] payload = "test payload".getBytes(StandardCharsets.UTF_8);
        Packet packet = new Packet(path, payload, 0);
        
        // Act
        boolean enqueued = simulator.enqueuePacket(packet);
        simulator.tick(10); // Tick enough to deliver any packet
        
        // Assert
        assertThat(enqueued).isFalse(); // Packet should be dropped
        assertThat(simulator.getStats().getPacketsDropped()).isEqualTo(1);
        assertThat(simulator.getStats().getDropRate()).isEqualTo(1.0);
    }
    
    @Test
    void testPacketDeliveryWithListenerRegistration() {
        // Arrange
        PacketSimulatorOptions options = new PacketSimulatorOptions();
        simulator = new PacketSimulator(options);
        
        AtomicInteger deliveryCount = new AtomicInteger(0);
        simulator.registerListener("node-2", (packet, tick) -> deliveryCount.incrementAndGet());
        
        byte[] payload = "test payload".getBytes(StandardCharsets.UTF_8);
        Packet packet = new Packet(path, payload, 0);
        
        // Act
        simulator.enqueuePacket(packet);
        simulator.tick(2); // Enough ticks to deliver the packet
        
        // Assert
        assertThat(deliveryCount.get()).isEqualTo(1);
        
        // Act - unregister the listener and send another packet
        simulator.unregisterListener("node-2");
        Packet packet2 = new Packet(path, "another payload".getBytes(StandardCharsets.UTF_8), 2);
        simulator.enqueuePacket(packet2);
        simulator.tick(2);
        
        // Assert - count should still be 1 since listener was unregistered
        assertThat(deliveryCount.get()).isEqualTo(1);
    }
    
    @Test
    void testDeterministicBehaviorWithSameSeeds() {
        // Arrange - create two simulators with the same seed
        long seed = 12345;
        List<Packet> deliveriesSimulator1 = new ArrayList<>();
        List<Packet> deliveriesSimulator2 = new ArrayList<>();
        
        PacketSimulatorOptions options1 = PacketSimulatorOptions.builder()
            .packetLossProbability(0.5) // 50% packet loss
            .delayRange(1, 5) // Variable delay
            .randomSeed(seed)
            .build();
        
        PacketSimulatorOptions options2 = PacketSimulatorOptions.builder()
            .packetLossProbability(0.5) // 50% packet loss
            .delayRange(1, 5) // Variable delay
            .randomSeed(seed)
            .build();
        
        PacketSimulator simulator1 = new PacketSimulator(options1);
        PacketSimulator simulator2 = new PacketSimulator(options2);
        
        simulator1.registerListener("node-2", (packet, tick) -> deliveriesSimulator1.add(packet));
        simulator2.registerListener("node-2", (packet, tick) -> deliveriesSimulator2.add(packet));
        
        // Create 10 packets with fixed IDs for reproducibility
        List<Packet> packets = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID id = new UUID(0, i); // Deterministic ID
            byte[] payload = String.format("payload-%d", i).getBytes(StandardCharsets.UTF_8);
            packets.add(new Packet(id, path, payload, 0));
        }
        
        // Act - enqueue all packets in both simulators
        for (Packet packet : packets) {
            simulator1.enqueuePacket(packet);
            simulator2.enqueuePacket(packet);
        }
        
        // Run both simulators for enough ticks to deliver all non-dropped packets
        simulator1.tick(10);
        simulator2.tick(10);
        
        // Assert - both simulators should drop the same packets and deliver in the same order
        assertThat(deliveriesSimulator1.size()).isEqualTo(deliveriesSimulator2.size());
        for (int i = 0; i < deliveriesSimulator1.size(); i++) {
            assertThat(deliveriesSimulator1.get(i).getPacketId())
                .isEqualTo(deliveriesSimulator2.get(i).getPacketId());
        }
        
        // Stats should also be identical
        assertThat(simulator1.getStats().getPacketsDropped())
            .isEqualTo(simulator2.getStats().getPacketsDropped());
        assertThat(simulator1.getStats().getPacketsDelivered())
            .isEqualTo(simulator2.getStats().getPacketsDelivered());
    }
    
    @Test
    void testDifferentBehaviorWithDifferentSeeds() {
        // Arrange - create two simulators with different seeds
        List<UUID> deliveriesSimulator1 = new ArrayList<>();
        List<UUID> deliveriesSimulator2 = new ArrayList<>();
        
        PacketSimulatorOptions options1 = PacketSimulatorOptions.builder()
            .packetLossProbability(0.5) // 50% packet loss
            .delayRange(1, 5) // Variable delay
            .randomSeed(12345)
            .build();
        
        PacketSimulatorOptions options2 = PacketSimulatorOptions.builder()
            .packetLossProbability(0.5) // 50% packet loss
            .delayRange(1, 5) // Variable delay
            .randomSeed(67890) // Different seed
            .build();
        
        PacketSimulator simulator1 = new PacketSimulator(options1);
        PacketSimulator simulator2 = new PacketSimulator(options2);
        
        simulator1.registerListener("node-2", (packet, tick) -> deliveriesSimulator1.add(packet.getPacketId()));
        simulator2.registerListener("node-2", (packet, tick) -> deliveriesSimulator2.add(packet.getPacketId()));
        
        // Create identical packets for both simulators
        for (int i = 0; i < 20; i++) {
            byte[] payload = String.format("payload-%d", i).getBytes(StandardCharsets.UTF_8);
            UUID id = new UUID(0, i); // Deterministic ID
            
            Packet packet = new Packet(id, path, payload, 0);
            simulator1.enqueuePacket(packet);
            simulator2.enqueuePacket(packet);
        }
        
        // Act - run both simulators for enough ticks
        simulator1.tick(10);
        simulator2.tick(10);
        
        // Assert - with different seeds, simulators should behave differently
        // There's a small chance they could randomly behave the same, but it's extremely unlikely
        // with 20 packets at 50% drop probability
        boolean differentBehavior = 
            simulator1.getStats().getPacketsDropped() != simulator2.getStats().getPacketsDropped() ||
            !deliveriesSimulator1.equals(deliveriesSimulator2);
        
        assertThat(differentBehavior).isTrue();
    }
    
    @Test
    void testSimulatorReset() {
        // Arrange
        byte[] payload = "test payload".getBytes(StandardCharsets.UTF_8);
        Packet packet = new Packet(path, payload, 0);
        
        simulator.enqueuePacket(packet);
        simulator.tick(2); // Deliver the packet
        
        assertThat(simulator.getCurrentTick()).isEqualTo(2);
        assertThat(deliveredPackets).hasSize(1);
        assertThat(simulator.getStats().getPacketsEnqueued()).isEqualTo(1);
        
        // Act
        simulator.reset();
        
        // Assert
        assertThat(simulator.getCurrentTick()).isZero();
        assertThat(simulator.getStats().getPacketsEnqueued()).isZero();
        assertThat(simulator.getStats().getPacketsDelivered()).isZero();
    }
} 