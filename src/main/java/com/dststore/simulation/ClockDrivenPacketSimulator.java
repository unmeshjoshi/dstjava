package com.dststore.simulation;

import com.dststore.network.PacketSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A demonstration class that connects a PacketSimulator to a SimulatedClock,
 * allowing the packet simulator to advance automatically when the clock ticks.
 * This is an example of how to integrate simulation components.
 */
public class ClockDrivenPacketSimulator {
    private static final Logger logger = LoggerFactory.getLogger(ClockDrivenPacketSimulator.class);
    
    private final SimulatedClock clock;
    private final PacketSimulator packetSimulator;
    private final SimulatedClock.TickListener tickHandler;
    
    /**
     * Creates a new ClockDrivenPacketSimulator that connects the given simulators.
     *
     * @param clock The simulated clock that will drive the timeline
     * @param packetSimulator The packet simulator that will be advanced with the clock
     */
    public ClockDrivenPacketSimulator(SimulatedClock clock, PacketSimulator packetSimulator) {
        this.clock = clock;
        this.packetSimulator = packetSimulator;
        
        // Create a tick listener that will advance the packet simulator when the clock ticks
        this.tickHandler = this::handleTick;
        
        // Register the tick handler with the clock
        clock.registerListener(tickHandler);
        
        // Synchronize the initial tick values
        if (clock.getCurrentTick() > 0) {
            packetSimulator.tick((int)clock.getCurrentTick());
        }
        
        logger.info("Created ClockDrivenPacketSimulator with clock at tick {} and packet simulator", 
                   clock.getCurrentTick());
    }
    
    /**
     * Disconnects the packet simulator from the clock.
     */
    public void disconnect() {
        clock.unregisterListener(tickHandler);
        logger.info("Disconnected packet simulator from clock");
    }
    
    /**
     * Handles a tick event from the clock by advancing the packet simulator.
     *
     * @param tick The new tick value
     */
    private void handleTick(long tick) {
        // Log at debug level because this will happen frequently
        logger.debug("Clock ticked to {}, advancing packet simulator", tick);
        
        // Since our packet simulator is also tick-based, we only need to advance it by 1
        packetSimulator.tick();
        
        // In a real system, we might also do other work here, like checking for completion conditions
    }
} 