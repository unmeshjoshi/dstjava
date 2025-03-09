package com.dststore.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the KeyValueStore class.
 */
public class KeyValueStoreTest {
    
    @TempDir
    Path tempDir;
    
    private KeyValueStore store;
    private String testDir;
    
    @BeforeEach
    void setUp() {
        testDir = tempDir.toString();
        store = new KeyValueStore("test-store", testDir);
    }
    
    @AfterEach
    void tearDown() {
        // Clean up any created files
        if (store != null) {
            store.clear();
        }
    }
    
    @Test
    void testBasicSetAndGet() {
        // Act
        boolean result = store.set("key1", "value1");
        
        // Assert
        assertThat(result).isTrue();
        assertThat(store.get("key1")).isEqualTo("value1");
        assertThat(store.get("non-existent")).isNull();
    }
    
    @Test
    void testGetWithTimestamp() {
        // Arrange
        long timestamp = 12345L;
        store.set("key1", "value1", timestamp);
        
        // Act
        TimestampedValue value = store.getWithTimestamp("key1");
        
        // Assert
        assertThat(value).isNotNull();
        assertThat(value.getValue()).isEqualTo("value1");
        assertThat(value.getTimestamp()).isEqualTo(timestamp);
    }
    
    @Test
    void testTimestampBasedConflictResolution() {
        // Arrange - store a value with timestamp 200
        store.set("key1", "value-newer", 200);
        
        // Act - try to update with an older timestamp
        boolean result = store.set("key1", "value-older", 100);
        
        // Assert - update should be rejected
        assertThat(result).isFalse();
        assertThat(store.get("key1")).isEqualTo("value-newer");
        
        // Act - update with a newer timestamp
        boolean result2 = store.set("key1", "value-newest", 300);
        
        // Assert - update should be accepted
        assertThat(result2).isTrue();
        assertThat(store.get("key1")).isEqualTo("value-newest");
    }
    
    @Test
    void testDelete() {
        // Arrange
        store.set("key1", "value1");
        store.set("key2", "value2");
        
        // Act
        boolean result = store.delete("key1");
        
        // Assert
        assertThat(result).isTrue();
        assertThat(store.get("key1")).isNull();
        assertThat(store.containsKey("key1")).isFalse();
        assertThat(store.get("key2")).isEqualTo("value2"); // Other keys unaffected
    }
    
    @Test
    void testDeleteWithTimestamp() {
        // Arrange - store a value with timestamp 200
        store.set("key1", "value1", 200);
        
        // Act - try to delete with an older timestamp
        boolean result = store.delete("key1", 100);
        
        // Assert - delete should be rejected
        assertThat(result).isFalse();
        assertThat(store.get("key1")).isEqualTo("value1");
        
        // Act - delete with a newer timestamp
        boolean result2 = store.delete("key1", 300);
        
        // Assert - delete should succeed
        assertThat(result2).isTrue();
        assertThat(store.get("key1")).isNull();
    }
    
    @Test
    void testContainsKey() {
        // Arrange
        store.set("key1", "value1");
        store.set("key2", null); // Tombstone
        
        // Act & Assert
        assertThat(store.containsKey("key1")).isTrue();
        assertThat(store.containsKey("key2")).isFalse(); // Tombstones return false
        assertThat(store.containsKey("non-existent")).isFalse();
    }
    
    @Test
    void testSize() {
        // Arrange
        store.set("key1", "value1");
        store.set("key2", "value2");
        store.set("key3", null); // Tombstone
        
        // Act & Assert
        assertThat(store.size()).isEqualTo(2); // Should not count tombstone
    }
    
    @Test
    void testGetAll() {
        // Arrange
        store.set("key1", "value1");
        store.set("key2", "value2");
        store.set("key3", null); // Tombstone
        
        // Act
        Map<String, String> allValues = store.getAll();
        
        // Assert
        assertThat(allValues).hasSize(2);
        assertThat(allValues).containsEntry("key1", "value1");
        assertThat(allValues).containsEntry("key2", "value2");
        assertThat(allValues).doesNotContainKey("key3"); // Tombstone not included
    }
    
    @Test
    void testGetAllWithTimestamps() {
        // Arrange
        store.set("key1", "value1", 100);
        store.set("key2", "value2", 200);
        store.set("key3", null, 300); // Tombstone
        
        // Act
        Map<String, TimestampedValue> allValues = store.getAllWithTimestamps();
        
        // Assert
        assertThat(allValues).hasSize(3); // Includes tombstone
        
        TimestampedValue value1 = allValues.get("key1");
        assertThat(value1.getValue()).isEqualTo("value1");
        assertThat(value1.getTimestamp()).isEqualTo(100);
        
        TimestampedValue value3 = allValues.get("key3");
        assertThat(value3.getValue()).isNull();
        assertThat(value3.getTimestamp()).isEqualTo(300);
    }
    
    @Test
    void testClear() {
        // Arrange
        store.set("key1", "value1");
        store.set("key2", "value2");
        
        // Act
        store.clear();
        
        // Assert
        assertThat(store.size()).isZero();
        assertThat(store.get("key1")).isNull();
    }
    
    @Test
    void testGetStoreId() {
        // Act & Assert
        assertThat(store.getStoreId()).isEqualTo("test-store");
    }
    
    @Test
    void testDataFileIsCreated() {
        // Arrange
        store.set("key1", "value1");
        
        // Act & Assert
        File expectedFile = new File(testDir, "test-store-store.json");
        assertThat(expectedFile.exists()).isTrue();
    }
} 