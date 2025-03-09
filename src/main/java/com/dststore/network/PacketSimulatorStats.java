package com.dststore.network;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks statistics for the packet simulator.
 * Thread-safe for concurrent packet delivery operations.
 */
public class PacketSimulatorStats {
    private final AtomicLong packetsEnqueued = new AtomicLong(0);
    private final AtomicLong packetsDelivered = new AtomicLong(0);
    private final AtomicLong packetsDropped = new AtomicLong(0);
    private final Map<Path, AtomicLong> packetsByPath = new HashMap<>();
    private final AtomicLong totalDeliveryTime = new AtomicLong(0);
    
    /**
     * Records a packet being enqueued for delivery.
     *
     * @param packet The packet being enqueued
     */
    public void recordPacketEnqueued(Packet packet) {
        packetsEnqueued.incrementAndGet();
        packetsByPath.computeIfAbsent(packet.getPath(), path -> new AtomicLong(0))
                    .incrementAndGet();
    }
    
    /**
     * Records a packet being successfully delivered.
     *
     * @param packet The packet being delivered
     * @param deliveryTick The tick when the packet was delivered
     */
    public void recordPacketDelivered(Packet packet, long deliveryTick) {
        packetsDelivered.incrementAndGet();
        long deliveryTime = deliveryTick - packet.getCreatedAtTick();
        totalDeliveryTime.addAndGet(deliveryTime);
    }
    
    /**
     * Records a packet being dropped.
     *
     * @param packet The packet being dropped
     */
    public void recordPacketDropped(Packet packet) {
        packetsDropped.incrementAndGet();
    }
    
    /**
     * Gets the total number of packets that have been enqueued.
     *
     * @return Count of enqueued packets
     */
    public long getPacketsEnqueued() {
        return packetsEnqueued.get();
    }
    
    /**
     * Gets the total number of packets that have been delivered.
     *
     * @return Count of delivered packets
     */
    public long getPacketsDelivered() {
        return packetsDelivered.get();
    }
    
    /**
     * Gets the total number of packets that have been dropped.
     *
     * @return Count of dropped packets
     */
    public long getPacketsDropped() {
        return packetsDropped.get();
    }
    
    /**
     * Gets the number of packets sent along a specific path.
     *
     * @param path The path to check
     * @return Count of packets sent on this path
     */
    public long getPacketsForPath(Path path) {
        AtomicLong count = packetsByPath.get(path);
        return count != null ? count.get() : 0;
    }
    
    /**
     * Gets the average delivery time in ticks for successfully delivered packets.
     *
     * @return Average delivery time, or 0 if no packets have been delivered
     */
    public double getAverageDeliveryTime() {
        long delivered = packetsDelivered.get();
        return delivered > 0 ? (double) totalDeliveryTime.get() / delivered : 0;
    }
    
    /**
     * Gets the packet drop rate (ratio of dropped to enqueued).
     *
     * @return Drop rate between 0.0 and 1.0, or 0 if no packets have been enqueued
     */
    public double getDropRate() {
        long enqueued = packetsEnqueued.get();
        return enqueued > 0 ? (double) packetsDropped.get() / enqueued : 0;
    }
    
    /**
     * Resets all statistics counters to zero.
     */
    public void reset() {
        packetsEnqueued.set(0);
        packetsDelivered.set(0);
        packetsDropped.set(0);
        packetsByPath.clear();
        totalDeliveryTime.set(0);
    }
    
    @Override
    public String toString() {
        return String.format(
            "PacketSimulatorStats{enqueued=%d, delivered=%d, dropped=%d, avgDeliveryTime=%.2f, dropRate=%.2f%%}",
            packetsEnqueued.get(),
            packetsDelivered.get(),
            packetsDropped.get(),
            getAverageDeliveryTime(),
            getDropRate() * 100
        );
    }
} 