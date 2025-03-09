package com.dststore.simulation;

import com.dststore.message.Message;
import com.dststore.message.MessageType;
import com.dststore.network.IMessageBus;
import com.dststore.network.MessageBus;
import com.dststore.network.MessageHandler;
import com.dststore.network.PacketListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A message bus implementation that is driven by a simulated clock.
 * Messages are queued and delivered only when the clock ticks.
 */
public class ClockDrivenMessageBus implements IMessageBus {
    private static final Logger logger = LoggerFactory.getLogger(ClockDrivenMessageBus.class);
    
    private final String nodeId;
    private final IMessageBus messageBus;
    private final SimulatedClock clock;
    private final Queue<Message> messageQueue;
    private volatile boolean running;
    
    public ClockDrivenMessageBus(String nodeId, SimulatedClock clock) {
        this.nodeId = nodeId;
        this.clock = clock;
        this.messageBus = new MessageBus(nodeId, null);
        this.messageQueue = new ConcurrentLinkedQueue<>();
        this.running = false;
        
        // Register for clock ticks
        clock.registerListener(this::onTick);
    }
    
    @Override
    public void start() {
        running = true;
        messageBus.start();
    }
    
    @Override
    public void stop() {
        running = false;
        messageBus.stop();
    }
    
    @Override
    public void connect(String targetNodeId, String host, int port) {
        messageBus.connect(targetNodeId, host, port);
    }
    
    @Override
    public void disconnect(String targetNodeId) {
        messageBus.disconnect(targetNodeId);
    }
    
    @Override
    public void sendMessage(Message message) {
        if (!running) {
            logger.warn("Attempted to send message while not running");
            return;
        }
        
        // Queue the message for delivery on next tick
        messageQueue.add(message);
    }
    
    @Override
    public void registerListener(PacketListener listener) {
        messageBus.registerListener(listener);
    }
    
    @Override
    public String getNodeId() {
        return nodeId;
    }
    
    @Override
    public void tick() {
        onTick(clock.getCurrentTick());
    }
    
    @Override
    public void registerHandler(MessageHandler messageHandler) {
        // Forward to the underlying message bus
        messageBus.registerHandler(messageHandler);
    }
    
    @Override
    public void unregisterHandler(MessageType messageType) {
        // Forward to the underlying message bus
        messageBus.unregisterHandler(messageType);
    }
    
    /**
     * Called when the simulated clock ticks.
     * Processes any queued messages.
     *
     * @param tick The current tick count
     */
    private void onTick(long tick) {
        if (!running) {
            return;
        }
        
        // Process all queued messages
        int processedCount = 0;
        while (!messageQueue.isEmpty()) {
            Message message = messageQueue.poll();
            if (message != null) {
                messageBus.sendMessage(message);
                processedCount++;
            }
        }
        
        if (processedCount > 0) {
            logger.debug("Processed {} messages at tick {}", processedCount, tick);
        }
    }
} 