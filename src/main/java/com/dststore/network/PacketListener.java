package com.dststore.network;

import com.dststore.message.Message;

/**
 * Interface for components that need to receive network messages.
 */
public interface PacketListener {
    /**
     * Called when a message is received.
     *
     * @param message The received message
     */
    void onPacketReceived(Message message);
} 