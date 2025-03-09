package com.dststore.network;

import com.dststore.message.Message;
import com.dststore.message.MessageType;

/**
 * Interface for handling specific types of messages in the message bus.
 */
public interface MessageHandler {
    /**
     * Gets the type of message this handler can process.
     *
     * @return The message type this handler is responsible for
     */
    MessageType getHandledType();

    /**
     * Handles a received message of the specified type.
     *
     * @param message The message to handle
     */
    void handleMessage(Message message);
} 