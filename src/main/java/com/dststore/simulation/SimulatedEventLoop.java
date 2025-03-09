package com.dststore.simulation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

/**
 * A simulated event loop that integrates with our clock-driven simulation.
 * This allows us to test network I/O in a deterministic way.
 */
public class SimulatedEventLoop {
    private static final Logger logger = LoggerFactory.getLogger(SimulatedEventLoop.class);
    
    private final SimulatedClock clock;
    private final Queue<Runnable> taskQueue;
    private final Queue<SimulatedIOEvent> ioEvents;
    private volatile boolean running;
    
    public SimulatedEventLoop(SimulatedClock clock) {
        this.clock = clock;
        this.taskQueue = new ConcurrentLinkedQueue<>();
        this.ioEvents = new ConcurrentLinkedQueue<>();
        this.running = false;
        
        // Register for clock ticks to process events
        clock.registerListener(this::onTick);
    }
    
    public void start() {
        running = true;
    }
    
    public void stop() {
        running = false;
    }
    
    /**
     * Called when the simulated clock ticks.
     * Processes any pending tasks and I/O events.
     */
    private void onTick(long tick) {
        if (!running) {
            return;
        }
        
        // Process tasks first
        processTasks();
        
        // Then process I/O events
        processIOEvents();
    }
    
    private void processTasks() {
        Runnable task;
        while ((task = taskQueue.poll()) != null) {
            try {
                task.run();
            } catch (Exception e) {
                logger.error("Error executing task", e);
            }
        }
    }
    
    private void processIOEvents() {
        SimulatedIOEvent event;
        while ((event = ioEvents.poll()) != null) {
            try {
                event.handler.accept(event.key);
            } catch (Exception e) {
                logger.error("Error handling I/O event", e);
            }
        }
    }
    
    public void execute(Runnable task) {
        taskQueue.offer(task);
    }
    
    public void simulateIOEvent(SelectionKey key, Consumer<SelectionKey> handler) {
        ioEvents.offer(new SimulatedIOEvent(key, handler));
    }
    
    /**
     * Represents a simulated I/O event.
     */
    private static class SimulatedIOEvent {
        private final SelectionKey key;
        private final Consumer<SelectionKey> handler;
        
        public SimulatedIOEvent(SelectionKey key, Consumer<SelectionKey> handler) {
            this.key = key;
            this.handler = handler;
        }
    }
} 