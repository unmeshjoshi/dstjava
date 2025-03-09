package com.dststore.network;

/**
 * Configuration options for the packet simulator.
 * These options control network conditions like packet loss, delay, and jitter.
 */
public class PacketSimulatorOptions {
    // Network reliability
    private final double packetLossProbability;
    
    // Network latency
    private final int minDelayTicks;
    private final int maxDelayTicks;
    
    // For deterministic simulation
    private final long randomSeed;
    
    // Statistics tracking
    private final boolean collectStatistics;
    
    /**
     * Creates simulator options with default reliable network settings:
     * - No packet loss
     * - Consistent 1-tick delay
     * - Default random seed (42)
     * - Statistics collection enabled
     */
    public PacketSimulatorOptions() {
        this(0.0, 1, 1, 42, true);
    }
    
    /**
     * Creates simulator options with specified reliability but fixed delay.
     *
     * @param packetLossProbability Probability (0.0-1.0) that a packet will be dropped
     * @param delayTicks Fixed delay in ticks for all packets
     * @param randomSeed Seed for the random number generator
     */
    public PacketSimulatorOptions(double packetLossProbability, int delayTicks, long randomSeed) {
        this(packetLossProbability, delayTicks, delayTicks, randomSeed, true);
    }
    
    /**
     * Creates fully customized simulator options.
     *
     * @param packetLossProbability Probability (0.0-1.0) that a packet will be dropped
     * @param minDelayTicks Minimum delay in ticks for packet delivery
     * @param maxDelayTicks Maximum delay in ticks for packet delivery (for jitter simulation)
     * @param randomSeed Seed for the random number generator
     * @param collectStatistics Whether to collect and report statistics
     */
    public PacketSimulatorOptions(double packetLossProbability, int minDelayTicks, 
                                int maxDelayTicks, long randomSeed, boolean collectStatistics) {
        
        if (packetLossProbability < 0.0 || packetLossProbability > 1.0) {
            throw new IllegalArgumentException("Packet loss probability must be between 0.0 and 1.0");
        }
        
        if (minDelayTicks < 0) {
            throw new IllegalArgumentException("Minimum delay cannot be negative");
        }
        
        if (maxDelayTicks < minDelayTicks) {
            throw new IllegalArgumentException("Maximum delay cannot be less than minimum delay");
        }
        
        this.packetLossProbability = packetLossProbability;
        this.minDelayTicks = minDelayTicks;
        this.maxDelayTicks = maxDelayTicks;
        this.randomSeed = randomSeed;
        this.collectStatistics = collectStatistics;
    }
    
    /**
     * Gets the probability of packet loss.
     *
     * @return Probability value between 0.0 (no loss) and 1.0 (all packets lost)
     */
    public double getPacketLossProbability() {
        return packetLossProbability;
    }
    
    /**
     * Gets the minimum delay in ticks for packet delivery.
     *
     * @return Minimum delay ticks
     */
    public int getMinDelayTicks() {
        return minDelayTicks;
    }
    
    /**
     * Gets the maximum delay in ticks for packet delivery.
     *
     * @return Maximum delay ticks
     */
    public int getMaxDelayTicks() {
        return maxDelayTicks;
    }
    
    /**
     * Gets the random seed for deterministic simulation.
     *
     * @return Random seed value
     */
    public long getRandomSeed() {
        return randomSeed;
    }
    
    /**
     * Whether to collect and report simulation statistics.
     *
     * @return True if statistics collection is enabled
     */
    public boolean isCollectStatistics() {
        return collectStatistics;
    }
    
    /**
     * Create a builder for PacketSimulatorOptions.
     * 
     * @return A new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Builder for creating PacketSimulatorOptions with a fluent interface.
     */
    public static class Builder {
        private double packetLossProbability = 0.0;
        private int minDelayTicks = 1;
        private int maxDelayTicks = 1;
        private long randomSeed = 42;
        private boolean collectStatistics = true;
        
        /**
         * Sets the packet loss probability.
         * 
         * @param packetLossProbability Probability (0.0-1.0) that a packet will be dropped
         * @return This builder instance
         */
        public Builder packetLossProbability(double packetLossProbability) {
            this.packetLossProbability = packetLossProbability;
            return this;
        }
        
        /**
         * Sets a fixed delay for all packets.
         * 
         * @param delayTicks Fixed delay in ticks for all packets
         * @return This builder instance
         */
        public Builder fixedDelay(int delayTicks) {
            this.minDelayTicks = delayTicks;
            this.maxDelayTicks = delayTicks;
            return this;
        }
        
        /**
         * Sets the delay range for packets (to simulate jitter).
         * 
         * @param minDelayTicks Minimum delay in ticks
         * @param maxDelayTicks Maximum delay in ticks
         * @return This builder instance
         */
        public Builder delayRange(int minDelayTicks, int maxDelayTicks) {
            this.minDelayTicks = minDelayTicks;
            this.maxDelayTicks = maxDelayTicks;
            return this;
        }
        
        /**
         * Sets the random seed for deterministic simulation.
         * 
         * @param randomSeed Random seed value
         * @return This builder instance
         */
        public Builder randomSeed(long randomSeed) {
            this.randomSeed = randomSeed;
            return this;
        }
        
        /**
         * Sets whether to collect and report statistics.
         * 
         * @param collectStatistics True to enable statistics collection
         * @return This builder instance
         */
        public Builder collectStatistics(boolean collectStatistics) {
            this.collectStatistics = collectStatistics;
            return this;
        }
        
        /**
         * Builds the PacketSimulatorOptions instance.
         * 
         * @return A new PacketSimulatorOptions with the configured values
         */
        public PacketSimulatorOptions build() {
            return new PacketSimulatorOptions(
                packetLossProbability,
                minDelayTicks,
                maxDelayTicks,
                randomSeed,
                collectStatistics
            );
        }
    }
} 