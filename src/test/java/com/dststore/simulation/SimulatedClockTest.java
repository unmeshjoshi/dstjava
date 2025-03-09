package com.dststore.simulation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the SimulatedClock class.
 */
public class SimulatedClockTest {
    
    private SimulatedClock clock;
    
    @BeforeEach
    void setUp() {
        clock = new SimulatedClock();
    }
    
    @Test
    void testDefaultConstructor() {
        // Assert
        assertThat(clock.getCurrentTick()).isZero();
    }
    
    @Test
    void testConstructorWithInitialTick() {
        // Arrange
        long initialTick = 42;
        
        // Act
        SimulatedClock clock = new SimulatedClock(initialTick);
        
        // Assert
        assertThat(clock.getCurrentTick()).isEqualTo(initialTick);
    }
    
    @Test
    void testTickIncrementsCurrentTick() {
        // Arrange
        long initialTick = clock.getCurrentTick();
        
        // Act
        long newTick = clock.tick();
        
        // Assert
        assertThat(newTick).isEqualTo(initialTick + 1);
        assertThat(clock.getCurrentTick()).isEqualTo(initialTick + 1);
    }
    
    @Test
    void testTickMultipleIncrementsCurrentTick() {
        // Arrange
        long initialTick = clock.getCurrentTick();
        int ticks = 5;
        
        // Act
        long newTick = clock.tick(ticks);
        
        // Assert
        assertThat(newTick).isEqualTo(initialTick + ticks);
        assertThat(clock.getCurrentTick()).isEqualTo(initialTick + ticks);
    }
    
    @Test
    void testTickMultipleWithInvalidValue() {
        // Assert
        assertThatThrownBy(() -> clock.tick(0))
            .isInstanceOf(IllegalArgumentException.class);
            
        assertThatThrownBy(() -> clock.tick(-1))
            .isInstanceOf(IllegalArgumentException.class);
    }
    
    @Test
    void testSetCurrentTick() {
        // Arrange
        long newTick = 100;
        
        // Act
        clock.setCurrentTick(newTick);
        
        // Assert
        assertThat(clock.getCurrentTick()).isEqualTo(newTick);
    }
    
    @Test
    void testSetCurrentTickWithInvalidValue() {
        // Assert
        assertThatThrownBy(() -> clock.setCurrentTick(-1))
            .isInstanceOf(IllegalArgumentException.class);
    }
    
    @Test
    void testTickNotifiesRegisteredListeners() {
        // Arrange
        AtomicInteger listenerCallCount = new AtomicInteger(0);
        List<Long> receivedTicks = new ArrayList<>();
        
        SimulatedClock.TickListener listener = tick -> {
            listenerCallCount.incrementAndGet();
            receivedTicks.add(tick);
        };
        
        clock.registerListener(listener);
        
        // Act
        clock.tick();
        clock.tick();
        
        // Assert
        assertThat(listenerCallCount.get()).isEqualTo(2);
        assertThat(receivedTicks).containsExactly(1L, 2L);
    }
    
    @Test
    void testTickMultipleNotifiesListenersForEachTick() {
        // Arrange
        List<Long> receivedTicks = new ArrayList<>();
        
        SimulatedClock.TickListener listener = receivedTicks::add;
        clock.registerListener(listener);
        
        // Act
        clock.tick(3);
        
        // Assert
        assertThat(receivedTicks).containsExactly(1L, 2L, 3L);
    }
    
    @Test
    void testUnregisterListener() {
        // Arrange
        AtomicInteger listenerCallCount = new AtomicInteger(0);
        
        SimulatedClock.TickListener listener = tick -> listenerCallCount.incrementAndGet();
        clock.registerListener(listener);
        
        // Verify listener is registered
        clock.tick();
        assertThat(listenerCallCount.get()).isEqualTo(1);
        
        // Act
        boolean removed = clock.unregisterListener(listener);
        
        // Assert
        assertThat(removed).isTrue();
        
        // Tick again and verify listener wasn't called
        clock.tick();
        assertThat(listenerCallCount.get()).isEqualTo(1); // Still 1, not incremented
    }
    
    @Test
    void testUnregisterNonexistentListener() {
        // Arrange
        SimulatedClock.TickListener listener = tick -> { };
        
        // Act - try to unregister a listener that was never registered
        boolean removed = clock.unregisterListener(listener);
        
        // Assert
        assertThat(removed).isFalse();
    }
    
    @Test
    void testListenerExceptionDoesNotAffectOtherListeners() {
        // Arrange
        AtomicInteger goodListenerCallCount = new AtomicInteger(0);
        
        SimulatedClock.TickListener throwingListener = tick -> {
            throw new RuntimeException("Test exception");
        };
        
        SimulatedClock.TickListener goodListener = tick -> {
            goodListenerCallCount.incrementAndGet();
        };
        
        // This test might log an exception, but it should still pass
        clock.registerListener(throwingListener);
        clock.registerListener(goodListener);
        
        // Act - listener with exception shouldn't prevent good listener from being called
        try {
            clock.tick();
        } catch (Exception e) {
            // In case the exception bubbles up (which it shouldn't), catch it so the test continues
        }
        
        // Assert - good listener should still be called even if other listener throws
        assertThat(goodListenerCallCount.get()).isEqualTo(1);
    }
} 