package com.dststore.simulation;

import com.dststore.network.Packet;
import com.dststore.network.PacketListener;
import com.dststore.network.PacketSimulator;
import com.dststore.network.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the ClockDrivenPacketSimulator class.
 */
public class ClockDrivenPacketSimulatorTest {
    
    private SimulatedClock clock;
    private PacketSimulator packetSimulator;
    private ClockDrivenPacketSimulator clockDrivenSimulator;
    private List<Packet> deliveredPackets;
    
    @BeforeEach
    void setUp() {
        clock = new SimulatedClock();
        packetSimulator = new PacketSimulator();
        deliveredPackets = new ArrayList<>();
        
        // Register a listener to track packet delivery
        PacketListener listener = (packet, tick) -> deliveredPackets.add(packet);
        packetSimulator.registerListener("node-2", listener);
        
        // Create the integrated simulator
        clockDrivenSimulator = new ClockDrivenPacketSimulator(clock, packetSimulator);
    }
    
    @Test
    void testPacketDeliveryDrivenByClock() {
        // Arrange
        Path path = new Path("node-1", "node-2");
        byte[] payload = "test-payload".getBytes(StandardCharsets.UTF_8);
        Packet packet = new Packet(path, payload, 0);
        
        // Enqueue packet with default 1-tick delay in packet simulator
        packetSimulator.enqueuePacket(packet);
        
        // Act - advance the clock, which should advance the packet simulator
        clock.tick();
        
        // Assert - packet should be delivered after 1 tick
        assertThat(deliveredPackets).hasSize(1);
        assertThat(deliveredPackets.get(0)).isEqualTo(packet);
    }
    
    @Test
    void testMultiplePacketsDeliveryWithClockTicks() {
        // Arrange
        Path path = new Path("node-1", "node-2");
        List<Packet> packets = new ArrayList<>();
        
        // Create and enqueue 3 packets
        for (int i = 0; i < 3; i++) {
            byte[] payload = String.format("payload-%d", i).getBytes(StandardCharsets.UTF_8);
            Packet packet = new Packet(path, payload, 0);
            packets.add(packet);
            packetSimulator.enqueuePacket(packet);
        }
        
        // Act - advance the clock, which should advance the packet simulator
        clock.tick();
        
        // Assert - all packets should be delivered
        assertThat(deliveredPackets).hasSize(3);
        // Use a more relaxed assertion that checks the size but doesn't depend on order
        assertThat(deliveredPackets).hasSameElementsAs(packets);
    }
    
    @Test
    void testDisconnectStopsAdvancingPacketSimulator() {
        // Arrange
        Path path = new Path("node-1", "node-2");
        byte[] payload = "test-payload".getBytes(StandardCharsets.UTF_8);
        
        // Act - disconnect before any packets are sent
        clockDrivenSimulator.disconnect();
        
        // Enqueue a packet and tick the clock
        Packet packet = new Packet(path, payload, 0);
        packetSimulator.enqueuePacket(packet);
        clock.tick();
        
        // Assert - packet should not be delivered because simulator was disconnected
        assertThat(deliveredPackets).isEmpty();
        
        // The packet simulator itself should not have advanced
        assertThat(packetSimulator.getCurrentTick()).isZero();
    }
    
    @Test
    void testSynchronizationAtCreation() {
        // Arrange - create a clock with an initial tick value
        SimulatedClock clock = new SimulatedClock(5);
        PacketSimulator packetSimulator = new PacketSimulator();
        
        // Act - create the integrated simulator, which should synchronize the tick values
        ClockDrivenPacketSimulator clockDrivenSimulator = 
            new ClockDrivenPacketSimulator(clock, packetSimulator);
        
        // Assert - packet simulator should be synchronized to clock's tick value
        assertThat(packetSimulator.getCurrentTick()).isEqualTo(clock.getCurrentTick());
    }
} 