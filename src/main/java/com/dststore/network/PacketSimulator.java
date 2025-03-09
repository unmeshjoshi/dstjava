package com.dststore.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Core packet simulator for deterministic network simulation.
 * Uses a priority queue to schedule packet delivery at specific ticks and
 * supports deterministic packet loss and delay based on a seeded random number generator.
 */
public class PacketSimulator {
    private static final Logger logger = LoggerFactory.getLogger(PacketSimulator.class);
    
    // Simulation configuration
    private final PacketSimulatorOptions options;
    private final Random random;
    
    // Delivery schedule
    private final PriorityQueue<ScheduledPacket> deliveryQueue;
    private long currentTick;
    
    // Statistics
    private final PacketSimulatorStats stats;
    
    // Packet delivery listeners
    private final Map<String, PacketListener> listeners;
    
    private final Set<String> partitionedNodes = new HashSet<>();
    
    /**
     * Creates a new packet simulator with the specified options.
     *
     * @param options Configuration options for the simulator
     */
    public PacketSimulator(PacketSimulatorOptions options) {
        this.options = options;
        this.random = new Random(options.getRandomSeed());
        this.deliveryQueue = new PriorityQueue<>();
        this.currentTick = 0;
        this.stats = new PacketSimulatorStats();
        this.listeners = new HashMap<>();
    }
    
    /**
     * Creates a new packet simulator with default options.
     */
    public PacketSimulator() {
        this(new PacketSimulatorOptions());
    }
    
    /**
     * Advances the simulation by one tick and processes any packets
     * scheduled for delivery at this tick.
     */
    public void tick() {
        currentTick++;
        processScheduledPackets();
    }
    
    /**
     * Advances the simulation by multiple ticks.
     *
     * @param ticks Number of ticks to advance
     */
    public void tick(int ticks) {
        for (int i = 0; i < ticks; i++) {
            tick();
        }
    }
    
    /**
     * Gets the current simulation tick.
     *
     * @return Current tick count
     */
    public long getCurrentTick() {
        return currentTick;
    }
    
    /**
     * Enqueues a packet for delivery with simulated network conditions.
     * May drop the packet based on loss probability.
     *
     * @param packet The packet to send
     * @return True if the packet was enqueued, false if it was dropped
     */
    public boolean enqueuePacket(Packet packet) {
        if (options.isCollectStatistics()) {
            stats.recordPacketEnqueued(packet);
        }
        
        // Check if packet should be dropped based on loss probability
        if (random.nextDouble() < options.getPacketLossProbability()) {
            if (options.isCollectStatistics()) {
                stats.recordPacketDropped(packet);
            }
            logger.debug("Dropped packet: {}", packet);
            return false;
        }
        
        // Calculate delivery delay based on min/max settings
        int delay;
        if (options.getMinDelayTicks() == options.getMaxDelayTicks()) {
            delay = options.getMinDelayTicks();
        } else {
            delay = options.getMinDelayTicks() + 
                   random.nextInt(options.getMaxDelayTicks() - options.getMinDelayTicks() + 1);
        }
        
        // Schedule the packet for delivery
        long deliveryTick = currentTick + delay;
        ScheduledPacket scheduledPacket = new ScheduledPacket(packet, deliveryTick);
        deliveryQueue.add(scheduledPacket);
        
        logger.debug("Scheduled packet: {} for delivery at tick {}", packet, deliveryTick);
        return true;
    }
    
    /**
     * Registers a listener for packets delivered to a specific node.
     *
     * @param nodeId The node ID to listen for packets
     * @param listener The listener to notify on packet delivery
     */
    public void registerListener(String nodeId, PacketListener listener) {
        listeners.put(nodeId, listener);
    }
    
    /**
     * Unregisters a listener for a specific node.
     *
     * @param nodeId The node ID to stop listening for
     */
    public void unregisterListener(String nodeId) {
        listeners.remove(nodeId);
    }
    
    /**
     * Gets the statistics for this simulator.
     *
     * @return Statistics object
     */
    public PacketSimulatorStats getStats() {
        return stats;
    }
    
    /**
     * Resets the simulator to its initial state.
     * Clears the delivery queue and resets statistics.
     */
    public void reset() {
        currentTick = 0;
        deliveryQueue.clear();
        stats.reset();
    }
    
    /**
     * Process all packets scheduled for delivery at the current tick.
     */
    private void processScheduledPackets() {
        while (!deliveryQueue.isEmpty() && deliveryQueue.peek().deliveryTick <= currentTick) {
            ScheduledPacket packet = deliveryQueue.poll();
            String key = packet.packet.getPath().getSourceId() + "-" + packet.packet.getPath().getTargetId();
            if (!partitionedNodes.contains(key)) {
                PacketListener listener = listeners.get(packet.packet.getPath().getTargetId());
                if (listener != null) {
                    listener.onPacketDelivered(packet.packet, currentTick);
                    stats.recordPacketDelivered(packet.packet, currentTick);
                }
            } else {
                logger.info("Packet from {} to {} dropped due to partition", packet.packet.getPath().getSourceId(), packet.packet.getPath().getTargetId());
                stats.recordPacketDropped(packet.packet);
            }
        }
    }
    
    /**
     * Class to hold a packet along with its scheduled delivery tick.
     * Used within the priority queue for scheduling.
     */
    private static class ScheduledPacket implements Comparable<ScheduledPacket> {
        private final Packet packet;
        private final long deliveryTick;
        
        public ScheduledPacket(Packet packet, long deliveryTick) {
            this.packet = packet;
            this.deliveryTick = deliveryTick;
        }
        
        @Override
        public int compareTo(ScheduledPacket other) {
            return Long.compare(this.deliveryTick, other.deliveryTick);
        }
    }
    
    /**
     * Disconnects two nodes, simulating a network partition.
     *
     * @param nodeId1 The ID of the first node
     * @param nodeId2 The ID of the second node
     */
    public void disconnect(String nodeId1, String nodeId2) {
        // Implement logic to prevent packets from being delivered between these nodes
        logger.info("Disconnecting nodes {} and {}", nodeId1, nodeId2);
        // Example: Remove listeners or mark nodes as disconnected
    }

    /**
     * Connects two nodes, healing a network partition.
     *
     * @param nodeId1 The ID of the first node
     * @param nodeId2 The ID of the second node
     */
    public void connect(String nodeId1, String nodeId2) {
        // Implement logic to allow packets to be delivered between these nodes
        logger.info("Connecting nodes {} and {}", nodeId1, nodeId2);
        // Example: Add listeners or mark nodes as connected
    }

    // Method to partition nodes
    public void partitionNodes(String node1, String node2) {
        partitionedNodes.add(node1 + "-" + node2);
        partitionedNodes.add(node2 + "-" + node1);
        logger.info("Partitioned nodes: {} and {}", node1, node2);
    }

    // Method to heal partition between nodes
    public void healPartition(String node1, String node2) {
        partitionedNodes.remove(node1 + "-" + node2);
        partitionedNodes.remove(node2 + "-" + node1);
        logger.info("Healed partition between nodes: {} and {}", node1, node2);
    }
} 