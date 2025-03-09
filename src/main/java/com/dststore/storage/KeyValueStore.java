package com.dststore.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A local key-value store with timestamp-based conflict resolution and disk persistence.
 * This store is designed to be used by a single replica in the distributed system.
 */
public class KeyValueStore {
    private static final Logger logger = LoggerFactory.getLogger(KeyValueStore.class);
    
    private final String storeId;
    private final String dataFilePath;
    private final ObjectMapper objectMapper;
    private final Map<String, TimestampedValue> storage;
    
    /**
     * Creates a new KeyValueStore with the specified ID.
     *
     * @param storeId The ID of this store (typically matches the replica ID)
     */
    public KeyValueStore(String storeId) {
        this(storeId, "data");
    }
    
    /**
     * Creates a new KeyValueStore with the specified ID and data directory.
     *
     * @param storeId The ID of this store (typically matches the replica ID)
     * @param dataDir The directory to store persistence files
     */
    public KeyValueStore(String storeId, String dataDir) {
        this.storeId = storeId;
        this.objectMapper = new ObjectMapper();
        this.storage = new ConcurrentHashMap<>();
        
        // Setup data directory and file path
        File directory = new File(dataDir);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        
        this.dataFilePath = Paths.get(dataDir, storeId + "-store.json").toString();
        
        // Load data from disk
        loadFromDisk();
        
        logger.info("Initialized KeyValueStore '{}' with {} keys", storeId, storage.size());
    }
    
    /**
     * Gets a value for the specified key, or null if not found.
     *
     * @param key The key to look up
     * @return The value, or null if no mapping exists
     */
    public String get(String key) {
        TimestampedValue value = storage.get(key);
        return value != null ? value.getValue() : null;
    }
    
    /**
     * Gets a value with its timestamp for the specified key, or null if not found.
     *
     * @param key The key to look up
     * @return The timestamped value, or null if no mapping exists
     */
    public TimestampedValue getWithTimestamp(String key) {
        return storage.get(key);
    }
    
    /**
     * Stores a value for the specified key with the current timestamp.
     *
     * @param key The key to store
     * @param value The value to store
     * @return True if the value was stored, false if it was rejected due to conflict resolution
     */
    public boolean set(String key, String value) {
        return set(key, value, System.currentTimeMillis());
    }
    
    /**
     * Stores a value for the specified key with the provided timestamp.
     * Uses timestamp-based conflict resolution - a value is only updated if the new timestamp
     * is greater than or equal to the existing timestamp.
     *
     * @param key The key to store
     * @param value The value to store
     * @param timestamp The timestamp of this value
     * @return True if the value was stored, false if it was rejected due to conflict resolution
     */
    public boolean set(String key, String value, long timestamp) {
        // Check if we already have a value for this key
        TimestampedValue existingValue = storage.get(key);
        
        // If we have a previous value, apply timestamp-based conflict resolution
        if (existingValue != null && existingValue.getTimestamp() > timestamp) {
            logger.debug("Rejecting write for key '{}' due to older timestamp ({} vs {})",
                      key, timestamp, existingValue.getTimestamp());
            return false;
        }
        
        // Store the new value
        TimestampedValue newValue = new TimestampedValue(value, timestamp);
        storage.put(key, newValue);
        
        logger.debug("Set key '{}' to '{}' with timestamp {}", key, value, timestamp);
        
        // Persist to disk
        saveToDisk();
        
        return true;
    }
    
    /**
     * Deletes a key-value pair.
     * Note: This is a soft delete using a tombstone value with the current timestamp.
     *
     * @param key The key to delete
     * @return True if the key was deleted (or already deleted), false otherwise
     */
    public boolean delete(String key) {
        return delete(key, System.currentTimeMillis());
    }
    
    /**
     * Deletes a key-value pair with the specified timestamp.
     * Note: This is a soft delete using a tombstone value.
     *
     * @param key The key to delete
     * @param timestamp The timestamp of this delete operation
     * @return True if the key was deleted, false if the deletion was rejected due to conflict resolution
     */
    public boolean delete(String key, long timestamp) {
        return set(key, null, timestamp);
    }
    
    /**
     * Checks if a key exists in the store and has a non-null value.
     *
     * @param key The key to check
     * @return True if the key exists and has a value, false otherwise
     */
    public boolean containsKey(String key) {
        TimestampedValue value = storage.get(key);
        return value != null && value.getValue() != null;
    }
    
    /**
     * Gets the number of key-value pairs in the store.
     *
     * @return The number of keys
     */
    public int size() {
        return (int) storage.values().stream()
            .filter(v -> v.getValue() != null)
            .count();
    }
    
    /**
     * Gets a snapshot of all key-value pairs.
     *
     * @return A copy of the key-value mappings
     */
    public Map<String, String> getAll() {
        Map<String, String> result = new HashMap<>();
        
        storage.forEach((key, timestampedValue) -> {
            if (timestampedValue.getValue() != null) {
                result.put(key, timestampedValue.getValue());
            }
        });
        
        return result;
    }
    
    /**
     * Gets a snapshot of all key-value pairs with their timestamps.
     *
     * @return A copy of the key-value mappings with timestamps
     */
    public Map<String, TimestampedValue> getAllWithTimestamps() {
        return new HashMap<>(storage);
    }
    
    /**
     * Clears all key-value pairs from the store.
     * This is primarily used for testing.
     */
    public void clear() {
        storage.clear();
        saveToDisk();
        logger.info("Cleared all data in store '{}'", storeId);
    }
    
    /**
     * Loads the store data from disk.
     */
    private void loadFromDisk() {
        File file = new File(dataFilePath);
        if (!file.exists()) {
            logger.info("No data file found at {}, starting with empty store", dataFilePath);
            return;
        }
        
        try {
            byte[] jsonData = Files.readAllBytes(Path.of(dataFilePath));
            Map<String, TimestampedValue> loadedData = objectMapper.readValue(
                jsonData, new TypeReference<Map<String, TimestampedValue>>() {});
            
            storage.putAll(loadedData);
            logger.info("Loaded {} keys from {}", loadedData.size(), dataFilePath);
        } catch (IOException e) {
            logger.error("Failed to load data from {}: {}", dataFilePath, e.getMessage(), e);
        }
    }
    
    /**
     * Saves the store data to disk.
     */
    private void saveToDisk() {
        try {
            objectMapper.writeValue(new File(dataFilePath), storage);
            logger.debug("Saved {} keys to {}", storage.size(), dataFilePath);
        } catch (IOException e) {
            logger.error("Failed to save data to {}: {}", dataFilePath, e.getMessage(), e);
        }
    }
    
    /**
     * Gets the ID of this store.
     *
     * @return The store ID
     */
    public String getStoreId() {
        return storeId;
    }
} 