package com.dststore.network;

import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Random;
import java.util.logging.Logger;

public class NetworkConditions {
    private final Random random;
    private volatile double messageLossRate = 0.0;
    private volatile int maxMessagesPerTick = Integer.MAX_VALUE;

    NetworkConditions(Random random) {
        this.random = Objects.requireNonNull(random, "Random source cannot be null");
    }

    boolean isDeliverable() {
        if (messageLossRate <= 0) {
            return true;  // Fast path - no message loss configured
        }
        return !shouldDropMessage();
    }

    private boolean shouldDropMessage() {
        double randomValue = getNextRandomValue();
        boolean shouldDrop = randomValue < messageLossRate;
        
        if (shouldDrop) {
            Logger.getLogger(NetworkConditions.class.getName())
                  .fine(() -> String.format(
                      "Dropping message: random value %.3f < loss rate %.3f",
                      randomValue, messageLossRate));
        }
        return shouldDrop;
    }

    // Package-private for testing
    double getNextRandomValue() {
        double value = random.nextDouble();
        // Add assertion to catch any implementation changes in Random
        assert value >= 0.0 && value < 1.0 : 
            String.format("Random value %.3f outside expected range [0.0, 1.0)", value);
        return value;
    }

    double getMessageLossRate() {
        return messageLossRate;
    }

    int getMaxMessagesPerTick() {
        return maxMessagesPerTick;
    }

    void setMessageLossRate(double rate) {
        if (rate < 0.0 || rate > 1.0) {
            throw new IllegalArgumentException(
                String.format("Message loss rate must be between 0.0 and 1.0, got %.3f", rate));
        }
        this.messageLossRate = rate;
    }

    void setMaxMessagesPerTick(int max) {
        if (max <= 0) {
            throw new IllegalArgumentException(
                "Maximum messages per tick must be positive, got " + max);
        }
        this.maxMessagesPerTick = max;
    }

    <T> List<T> applyBandwidthLimit(List<T> messages) {
        if (messages.size() <= maxMessagesPerTick) {
            return new ArrayList<>(messages);  // Return a copy to avoid modifying the original list
        }
        return new ArrayList<>(messages.subList(0, maxMessagesPerTick));
    }
} 