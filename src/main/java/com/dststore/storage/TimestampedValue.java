package com.dststore.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Objects;

/**
 * A value with an associated timestamp for conflict resolution.
 * This class is immutable to ensure consistency.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TimestampedValue {
    private final String value;
    private final long timestamp;
    
    /**
     * Creates a new TimestampedValue.
     *
     * @param value The value (can be null for tombstones)
     * @param timestamp The timestamp associated with this value
     */
    @JsonCreator
    public TimestampedValue(@JsonProperty("value") String value, 
                          @JsonProperty("timestamp") long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }
    
    /**
     * Gets the value.
     *
     * @return The value, or null if this is a tombstone
     */
    public String getValue() {
        return value;
    }
    
    /**
     * Gets the timestamp.
     *
     * @return The timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }
    
    /**
     * Checks if this value is newer than another value.
     *
     * @param other The other timestamped value
     * @return True if this value has a higher timestamp
     */
    public boolean isNewerThan(TimestampedValue other) {
        return other == null || this.timestamp > other.timestamp;
    }
    
    /**
     * Checks if this value is a tombstone (deleted marker).
     *
     * @return True if this value represents a deletion
     */
    public boolean isTombstone() {
        return value == null;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimestampedValue that = (TimestampedValue) o;
        return timestamp == that.timestamp && Objects.equals(value, that.value);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(value, timestamp);
    }
    
    @Override
    public String toString() {
        return isTombstone() 
            ? String.format("TOMBSTONE(t=%d)", timestamp) 
            : String.format("\"%s\"(t=%d)", value, timestamp);
    }
} 