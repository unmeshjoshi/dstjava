package com.dststore.network;

/**
 * Interface for components that need to be notified when packets are delivered.
 */
@FunctionalInterface
public interface PacketDeliveryListener {
    /**
     * Called when a packet is delivered to its destination.
     *
     * @param packet The packet that was delivered
     * @param currentTick The current simulation tick when delivery occurred
     */
    void onPacketDelivered(Packet packet, long currentTick);
} 