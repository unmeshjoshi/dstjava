package com.dststore.simulation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simulated clock that advances in discrete ticks rather than using wall-clock time.
 * This provides a deterministic time source for all components in the simulation.
 */
public class SimulatedClock {
    private static final Logger logger = LoggerFactory.getLogger(SimulatedClock.class);
    
    private final AtomicLong currentTick;
    private final List<TickListener> listeners;
    
    /**
     * Creates a new simulated clock starting at tick 0.
     */
    public SimulatedClock() {
        this(0);
    }
    
    /**
     * Creates a new simulated clock starting at the specified tick.
     *
     * @param initialTick The initial tick value
     */
    public SimulatedClock(long initialTick) {
        this.currentTick = new AtomicLong(initialTick);
        this.listeners = new ArrayList<>();
    }
    
    /**
     * Gets the current tick value.
     *
     * @return The current tick
     */
    public long getCurrentTick() {
        return currentTick.get();
    }
    
    /**
     * Advances the clock by one tick and notifies all registered listeners.
     *
     * @return The new current tick value
     */
    public long tick() {
        long newTick = currentTick.incrementAndGet();
        notifyListeners(newTick);
        return newTick;
    }
    
    /**
     * Advances the clock by the specified number of ticks and notifies all registered listeners.
     *
     * @param ticks Number of ticks to advance
     * @return The new current tick value
     */
    public long tick(int ticks) {
        if (ticks <= 0) {
            throw new IllegalArgumentException("Number of ticks must be positive");
        }
        
        long newTick = 0;
        for (int i = 0; i < ticks; i++) {
            newTick = tick();
        }
        return newTick;
    }
    
    /**
     * Sets the clock to a specific tick value.
     * This is primarily used for testing or initialization.
     * If the new tick value is less than the current tick, registered components may behave unexpectedly.
     *
     * @param tick The tick value to set
     */
    public void setCurrentTick(long tick) {
        if (tick < 0) {
            throw new IllegalArgumentException("Tick value cannot be negative");
        }
        currentTick.set(tick);
    }
    
    /**
     * Registers a listener to be notified when the clock ticks.
     *
     * @param listener The listener to register
     */
    public void registerListener(TickListener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }
    
    /**
     * Unregisters a listener so it will no longer be notified when the clock ticks.
     *
     * @param listener The listener to unregister
     * @return True if the listener was found and removed, false otherwise
     */
    public boolean unregisterListener(TickListener listener) {
        synchronized (listeners) {
            return listeners.remove(listener);
        }
    }
    
    /**
     * Notifies all registered listeners that the clock has ticked.
     * Exceptions thrown by listeners are caught and logged to avoid disrupting other listeners.
     *
     * @param newTick The new tick value
     */
    private void notifyListeners(long newTick) {
        List<TickListener> listenersCopy;
        synchronized (listeners) {
            listenersCopy = new ArrayList<>(listeners);
        }
        
        for (TickListener listener : listenersCopy) {
            try {
                listener.onTick(newTick);
            } catch (Exception e) {
                logger.error("Exception in tick listener: {}", e.getMessage(), e);
                // Continue notifying other listeners despite this exception
            }
        }
    }
    
    /**
     * Interface for components that need to be notified when the clock ticks.
     */
    @FunctionalInterface
    public interface TickListener {
        /**
         * Called when the clock ticks.
         *
         * @param tick The new tick value
         */
        void onTick(long tick);
    }
} 