package com.dststore.simulation;

import com.dststore.message.Message;
import com.dststore.network.MessageBus;
import com.dststore.network.PacketSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Integrates MessageBus with SimulatedClock to provide a time-synchronized message bus.
 * This class adds the capability to buffer outgoing messages and release them on clock ticks,
 * allowing for deterministic, tick-based message delivery.
 */
public class ClockDrivenMessageBus {
    private static final Logger logger = LoggerFactory.getLogger(ClockDrivenMessageBus.class);
    
    private final MessageBus messageBus;
    private final SimulatedClock clock;
    private final SimulatedClock.TickListener tickHandler;
    private final Queue<Message> messageQueue;
    private final List<MessageBusListener> listeners;
    
    /**
     * Creates a new ClockDrivenMessageBus.
     *
     * @param nodeId The ID of the node this message bus belongs to
     * @param packetSimulator The packet simulator to use for network communication
     * @param clock The simulated clock to drive the message bus
     */
    public ClockDrivenMessageBus(String nodeId, PacketSimulator packetSimulator, SimulatedClock clock) {
        this.messageBus = new MessageBus(nodeId, packetSimulator);
        this.clock = clock;
        this.messageQueue = new ConcurrentLinkedQueue<>();
        this.listeners = new ArrayList<>();
        
        // Create a tick handler that processes queued messages when the clock ticks
        this.tickHandler = this::onTick;
        
        // Register with the clock
        clock.registerListener(tickHandler);
        
        logger.info("Created ClockDrivenMessageBus for node {} at tick {}", 
                  nodeId, clock.getCurrentTick());
    }
    
    /**
     * Queues a message to be sent on the next clock tick.
     * This allows for deterministic message delivery timing.
     *
     * @param message The message to queue
     */
    public void queueMessage(Message message) {
        messageQueue.add(message);
        logger.debug("Queued message of type {} from {} to {} for delivery on next tick", 
                   message.getType(), message.getSourceId(), message.getTargetId());
    }
    
    /**
     * Sends a message immediately, without waiting for a clock tick.
     * Use this for urgent messages where timing is not critical for determinism.
     *
     * @param message The message to send immediately
     * @return True if the message was successfully enqueued for delivery, false otherwise
     */
    public boolean sendMessageImmediately(Message message) {
        return messageBus.sendMessage(message);
    }
    
    /**
     * Connects this node to another node, enabling message exchange.
     *
     * @param targetNodeId The ID of the node to connect to
     */
    public void connect(String targetNodeId) {
        messageBus.connect(targetNodeId);
        
        // Notify listeners
        for (MessageBusListener listener : listeners) {
            listener.onConnect(messageBus.getNodeId(), targetNodeId);
        }
    }
    
    /**
     * Disconnects this node from another node, stopping message exchange.
     *
     * @param targetNodeId The ID of the node to disconnect from
     */
    public void disconnect(String targetNodeId) {
        messageBus.disconnect(targetNodeId);
        
        // Notify listeners
        for (MessageBusListener listener : listeners) {
            listener.onDisconnect(messageBus.getNodeId(), targetNodeId);
        }
    }
    
    /**
     * Registers a handler for messages of a specific type.
     *
     * @param messageHandler The handler to invoke when a message is received
     */
    public void registerHandler(MessageBus.MessageHandler messageHandler) {
        messageBus.registerHandler(messageHandler);
    }
    
    /**
     * Adds a listener for message bus events.
     *
     * @param listener The listener to add
     */
    public void addListener(MessageBusListener listener) {
        listeners.add(listener);
    }
    
    /**
     * Removes a listener for message bus events.
     *
     * @param listener The listener to remove
     * @return True if the listener was found and removed, false otherwise
     */
    public boolean removeListener(MessageBusListener listener) {
        return listeners.remove(listener);
    }
    
    /**
     * Gets the underlying message bus.
     *
     * @return The message bus
     */
    public MessageBus getMessageBus() {
        return messageBus;
    }
    
    /**
     * Gets the node ID of this message bus.
     *
     * @return The node ID
     */
    public String getNodeId() {
        return messageBus.getNodeId();
    }
    
    /**
     * Disconnects from the clock and stops processing messages.
     */
    public void disconnect() {
        clock.unregisterListener(tickHandler);
        logger.info("Disconnected message bus for node {} from clock", messageBus.getNodeId());
    }
    
    /**
     * Handles a clock tick by processing queued messages.
     *
     * @param tick The current tick value
     */
    private void onTick(long tick) {
        logger.debug("Processing queued messages for node {} at tick {}", 
                   messageBus.getNodeId(), tick);
        
        int messageCount = 0;
        while (!messageQueue.isEmpty()) {
            Message message = messageQueue.poll();
            if (message != null) {
                boolean sent = messageBus.sendMessage(message);
                if (sent) {
                    messageCount++;
                    
                    // Notify listeners
                    for (MessageBusListener listener : listeners) {
                        listener.onMessageSent(message, tick);
                    }
                }
            }
        }
        
        if (messageCount > 0) {
            logger.debug("Sent {} queued messages at tick {}", messageCount, tick);
        }
    }
    
    /**
     * Interface for listening to message bus events.
     */
    public interface MessageBusListener {
        /**
         * Called when a node connects to another node.
         *
         * @param sourceNodeId The node initiating the connection
         * @param targetNodeId The node being connected to
         */
        void onConnect(String sourceNodeId, String targetNodeId);
        
        /**
         * Called when a node disconnects from another node.
         *
         * @param sourceNodeId The node initiating the disconnection
         * @param targetNodeId The node being disconnected from
         */
        void onDisconnect(String sourceNodeId, String targetNodeId);
        
        /**
         * Called when a message is sent.
         *
         * @param message The message being sent
         * @param tick The tick at which the message was sent
         */
        void onMessageSent(Message message, long tick);
    }
} 