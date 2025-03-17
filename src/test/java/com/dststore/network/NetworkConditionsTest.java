package com.dststore.network;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Random;

class NetworkConditionsTest {
    @Test
    void testMessageLossDistribution() {
        Random deterministicRandom = new Random(42); // Fixed seed for reproducibility
        NetworkConditions conditions = new NetworkConditions(deterministicRandom);
        
        // Test with 30% loss rate
        conditions.setMessageLossRate(0.3);
        int totalMessages = 10000;
        int droppedMessages = 0;
        
        for (int i = 0; i < totalMessages; i++) {
            if (!conditions.isDeliverable()) {
                droppedMessages++;
            }
        }
        
        double actualLossRate = (double) droppedMessages / totalMessages;
        assertEquals(0.3, actualLossRate, 0.02, // Allow 2% deviation
            String.format("Expected 30%% loss rate, got %.1f%%", actualLossRate * 100));
    }

    @Test
    void testRandomValueRange() {
        NetworkConditions conditions = new NetworkConditions(new Random());
        int iterations = 1000;
        
        for (int i = 0; i < iterations; i++) {
            double value = conditions.getNextRandomValue();
            assertTrue(value >= 0.0, "Random value must be >= 0.0");
            assertTrue(value < 1.0, "Random value must be < 1.0");
        }
    }
} 