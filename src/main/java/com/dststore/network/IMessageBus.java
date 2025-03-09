package com.dststore.network;

import com.dststore.message.Message;
import com.dststore.message.MessageType;

/**
 * Interface defining the contract for message bus implementations.
 * This can be implemented by both simulated and real network communication classes.
 */
public interface IMessageBus {
    /**
     * Starts the message bus.
     */
    void start();

    /**
     * Stops the message bus.
     */
    void stop();

    /**
     * Connects to a remote node.
     *
     * @param targetNodeId The ID of the node to connect to
     * @param host The host address of the target node
     * @param port The port number of the target node
     */
    void connect(String targetNodeId, String host, int port);

    /**
     * Disconnects from a remote node.
     *
     * @param targetNodeId The ID of the node to disconnect from
     */
    void disconnect(String targetNodeId);

    /**
     * Sends a message to a remote node.
     *
     * @param message The message to send
     */
    void sendMessage(Message message);

    /**
     * Registers a packet listener.
     *
     * @param listener The listener to register
     */
    void registerListener(PacketListener listener);

    /**
     * Registers a handler for messages of a specific type.
     *
     * @param messageHandler The handler to invoke when a message is received
     */
    void registerHandler(MessageHandler messageHandler);

    /**
     * Unregisters a handler for messages of a specific type.
     *
     * @param messageType The type of message to unregister the handler for
     */
    void unregisterHandler(MessageType messageType);

    /**
     * Gets the ID of this node.
     *
     * @return The node ID
     */
    String getNodeId();

    /**
     * Advances the message bus by one time unit, processing any pending operations.
     * This method is called by the simulation loop to ensure deterministic execution.
     */
    void tick();
} 