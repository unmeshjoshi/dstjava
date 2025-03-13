package com.dststore.quorum;

/**
 * Represents a stored value with version information in the quorum-based key-value store.
 */
public class StoredValue {
    private final String value;
    private final long version;

    public StoredValue(String value, long version) {
        this.value = value;
        this.version = version;
    }

    public String getValue() {
        return value;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "StoredValue{" +
                "value='" + value + '\'' +
                ", version=" + version +
                '}';
    }
} 