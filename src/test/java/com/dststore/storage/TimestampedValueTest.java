package com.dststore.storage;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the TimestampedValue class.
 */
public class TimestampedValueTest {
    
    @Test
    void testRegularValue() {
        // Arrange & Act
        TimestampedValue value = new TimestampedValue("test-value", 123);
        
        // Assert
        assertThat(value.getValue()).isEqualTo("test-value");
        assertThat(value.getTimestamp()).isEqualTo(123);
        assertThat(value.isTombstone()).isFalse();
    }
    
    @Test
    void testTombstone() {
        // Arrange & Act
        TimestampedValue tombstone = new TimestampedValue(null, 456);
        
        // Assert
        assertThat(tombstone.getValue()).isNull();
        assertThat(tombstone.getTimestamp()).isEqualTo(456);
        assertThat(tombstone.isTombstone()).isTrue();
    }
    
    @Test
    void testTimestampComparison() {
        // Arrange
        TimestampedValue older = new TimestampedValue("old-value", 100);
        TimestampedValue newer = new TimestampedValue("new-value", 200);
        
        // Act & Assert
        assertThat(newer.isNewerThan(older)).isTrue();
        assertThat(older.isNewerThan(newer)).isFalse();
        
        // Compare with null
        assertThat(older.isNewerThan(null)).isTrue();
        
        // Same timestamp
        TimestampedValue sameTime = new TimestampedValue("same-time", 100);
        assertThat(older.isNewerThan(sameTime)).isFalse();
        assertThat(sameTime.isNewerThan(older)).isFalse();
    }
    
    @Test
    void testEquality() {
        // Arrange
        TimestampedValue value1 = new TimestampedValue("test", 100);
        TimestampedValue value2 = new TimestampedValue("test", 100);
        TimestampedValue value3 = new TimestampedValue("test", 200);
        TimestampedValue value4 = new TimestampedValue("different", 100);
        
        // Act & Assert
        assertThat(value1).isEqualTo(value2);
        assertThat(value1).isNotEqualTo(value3);
        assertThat(value1).isNotEqualTo(value4);
        
        // Verify hashCode is consistent with equals
        assertThat(value1.hashCode()).isEqualTo(value2.hashCode());
        assertThat(value1.hashCode()).isNotEqualTo(value3.hashCode());
        assertThat(value1.hashCode()).isNotEqualTo(value4.hashCode());
    }
    
    @Test
    void testToString() {
        // Arrange
        TimestampedValue value = new TimestampedValue("test", 100);
        TimestampedValue tombstone = new TimestampedValue(null, 200);
        
        // Act
        String valueString = value.toString();
        String tombstoneString = tombstone.toString();
        
        // Assert
        assertThat(valueString).contains("test");
        assertThat(valueString).contains("100");
        
        assertThat(tombstoneString).contains("TOMBSTONE");
        assertThat(tombstoneString).contains("200");
    }
} 