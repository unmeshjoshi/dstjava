package com.dststore.network;

import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Calculates message delivery delays for the simulated network.
 * This class encapsulates the logic for determining when messages should be delivered
 * based on configured latency settings.
 */
public class MessageDelayCalculator {
    private static final Logger LOGGER = Logger.getLogger(MessageDelayCalculator.class.getName());

    // Latency configuration
    private int minLatencyTicks;
    private int maxLatencyTicks;

    /**
     * Creates a new MessageDelayCalculator with the specified latency range.
     *
     * @param minLatencyTicks Minimum latency in ticks
     * @param maxLatencyTicks Maximum latency in ticks
     */
    public MessageDelayCalculator(int minLatencyTicks, int maxLatencyTicks) {
        setLatency(minLatencyTicks, maxLatencyTicks);
    }

    /**
     * Configures the latency range for message delivery in ticks.
     *
     * @param minTicks Minimum latency in ticks
     * @param maxTicks Maximum latency in ticks
     * @throws IllegalArgumentException if the latency values are invalid
     */
    public void setLatency(int minTicks, int maxTicks) {
        if (minTicks < 0 || maxTicks < 0) {
            throw new IllegalArgumentException("Latency ticks cannot be negative");
        }
        if (minTicks > maxTicks) {
            throw new IllegalArgumentException("Minimum latency cannot be greater than maximum latency");
        }

        this.minLatencyTicks = minTicks;
        this.maxLatencyTicks = maxTicks;
    }

    public int getMinLatencyTicks() {
        return minLatencyTicks;
    }
    public int getMaxLatencyTicks() {
        return maxLatencyTicks;
    }

    /**
     * Calculates the delivery tick for a message based on the current tick and latency settings.
     *
     * @param currentTick The current simulation tick
     * @return The tick at which the message should be delivered
     */
    public long calculateDeliveryTick(long currentTick) {
        int delay = calculateMessageDelay();
        return currentTick + (delay > 0 ? delay : 1); // At minimum, deliver on next tick
    }

    /**
     * Calculates a random delay based on the configured latency range.
     * Uses exponential distribution for jitter to better model real network conditions.
     *
     * @return The delay in ticks
     */
    public int calculateMessageDelay() {
        if (minLatencyTicks == maxLatencyTicks) {
            LOGGER.log(Level.INFO, "Using fixed delay of {0} ticks", minLatencyTicks);
            return minLatencyTicks;
        }

        // Base delay is always the minimum latency
        var baseDelay = minLatencyTicks;
        var maxJitter = maxLatencyTicks - minLatencyTicks;

        // Calculate jitter using exponential distribution
        var jitter = 0;
        if (maxJitter > 0) {
            var random = ThreadLocalRandom.current().nextDouble();
            // Use exponential distribution for jitter (more realistic)
            // Scale -ln(random) to the range [0, maxJitter]
            var expRandom = -Math.log(random);
            // Normalize to [0, 1] range by dividing by theoretical max (which is technically infinity, but we use 5.0)
            var normalizedRandom = Math.min(expRandom / 5.0, 1.0);
            // Scale to maxJitter
            jitter = (int) Math.floor(normalizedRandom * maxJitter);

            LOGGER.log(Level.INFO, "Exponential delay calculation: random={0}, expRandom={1}, normalizedRandom={2}, jitter={3}",
                    new Object[]{random, expRandom, normalizedRandom, jitter});
        }

        var totalDelay = baseDelay + jitter;
        LOGGER.log(Level.INFO, "Calculated delay: {0} ticks (baseDelay={1}, jitter={2})",
                new Object[]{totalDelay, baseDelay, jitter});

        return totalDelay;
    }
} 