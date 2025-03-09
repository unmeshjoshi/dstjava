package com.dststore.replica;

import com.dststore.message.GetRequest;
import com.dststore.message.GetResponse;
import com.dststore.message.Message;
import com.dststore.message.MessageType;
import com.dststore.message.SetRequest;
import com.dststore.message.SetResponse;
import com.dststore.network.IMessageBus;
import com.dststore.network.MessageHandler;
import com.dststore.storage.KeyValueStore;
import com.dststore.storage.TimestampedValue;
import com.dststore.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A replica in the distributed key-value store.
 * Each replica maintains a local key-value store and processes incoming messages.
 */
public class Replica {
    private static final Logger logger = LoggerFactory.getLogger(Replica.class);
    
    private final String replicaId;
    private final KeyValueStore store;
    private final IMessageBus messageBus;
    private final Metrics metrics;
    
    // Statistics
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong successfulRequests = new AtomicLong(0);
    private final AtomicLong failedRequests = new AtomicLong(0);
    private final Map<MessageType, AtomicLong> messageTypeCounts = new HashMap<>();
    
    // State
    private boolean running = false;
    
    /**
     * Creates a new replica with the specified ID and message bus.
     *
     * @param replicaId The unique identifier for this replica
     * @param messageBus The message bus for communication
     * @param metrics The metrics for recording request handling
     */
    public Replica(String replicaId, IMessageBus messageBus, Metrics metrics) {
        this.replicaId = replicaId;
        this.messageBus = messageBus;
        this.store = new KeyValueStore(replicaId);
        this.metrics = metrics;
        
        // Initialize message type counters
        for (MessageType type : MessageType.values()) {
            messageTypeCounts.put(type, new AtomicLong(0));
        }
        
        // Register message handlers
        registerMessageHandlers();
        
        logger.info("Initialized replica {}", replicaId);
    }
    
    /**
     * Advances the replica by one time unit, processing any pending operations.
     * This method is called by the simulation loop to ensure deterministic execution.
     */
    public void tick() {
        // The actual message processing is done by the message handlers
        // This method is here for future extensions like background tasks,
        // maintenance operations, or state reconciliation
    }
    
    /**
     * Starts the replica, allowing it to process messages.
     */
    public void start() {
        running = true;
        logger.info("Started replica {}", replicaId);
    }
    
    /**
     * Stops the replica, preventing it from processing messages.
     */
    public void stop() {
        running = false;
        logger.info("Stopped replica {}", replicaId);
    }
    
    /**
     * Connects this replica to another replica.
     *
     * @param targetReplicaId The ID of the replica to connect to
     * @param host The host address of the target replica
     * @param port The port number of the target replica
     */
    public void connectTo(String targetReplicaId, String host, int port) {
        messageBus.connect(targetReplicaId, host, port);
        logger.info("Replica {} connected to replica {}", replicaId, targetReplicaId);
    }
    
    /**
     * Disconnects this replica from another replica.
     *
     * @param targetReplicaId The ID of the replica to disconnect from
     */
    public void disconnectFrom(String targetReplicaId) {
        messageBus.disconnect(targetReplicaId);
        logger.info("Replica {} disconnected from replica {}", replicaId, targetReplicaId);
    }
    
    /**
     * Gets the ID of this replica.
     *
     * @return The replica ID
     */
    public String getReplicaId() {
        return replicaId;
    }
    
    /**
     * Gets the underlying key-value store.
     *
     * @return The key-value store
     */
    public KeyValueStore getStore() {
        return store;
    }
    
    /**
     * Gets statistics about the replica's operations.
     *
     * @return A map of statistic names to values
     */
    public Map<String, Long> getStats() {
        Map<String, Long> stats = new HashMap<>();
        stats.put("totalRequests", totalRequests.get());
        stats.put("successfulRequests", successfulRequests.get());
        stats.put("failedRequests", failedRequests.get());
        
        // Add message type counts
        for (Map.Entry<MessageType, AtomicLong> entry : messageTypeCounts.entrySet()) {
            stats.put("messages." + entry.getKey().name(), entry.getValue().get());
        }
        
        return stats;
    }
    
    /**
     * Checks if the replica is currently running.
     *
     * @return true if the replica is running, false otherwise
     */
    public boolean isRunning() {
        return running;
    }
    
    /**
     * Registers message handlers for different message types.
     */
    private void registerMessageHandlers() {
        messageBus.registerHandler(new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }

            @Override
            public void handleMessage(Message message) {
                messageTypeCounts.get(MessageType.GET_REQUEST).incrementAndGet();
                if (!running) {
                    logger.warn("Ignoring {} message, replica {} is not running", message.getType(), replicaId);
                    return;
                }
                if (message instanceof GetRequest getRequest) {
                    handleGetRequest(getRequest);
                }
            }
        });

        messageBus.registerHandler(new MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.SET_REQUEST;
            }

            @Override
            public void handleMessage(Message message) {
                messageTypeCounts.get(MessageType.SET_REQUEST).incrementAndGet();
                if (!running) {
                    logger.warn("Ignoring {} message, replica {} is not running", message.getType(), replicaId);
                    return;
                }
                if (message instanceof SetRequest setRequest) {
                    handleSetRequest(setRequest);
                }
            }
        });
    }

    private void handleGetRequest(GetRequest request) {
        if (!running) {
            logger.warn("Ignoring GET_REQUEST message, replica {} is not running", replicaId);
            return;
        }
        
        totalRequests.incrementAndGet();
        
        logger.debug("Replica {} handling GET request with message ID {} for key {}", replicaId, request.getMessageId(), request.getKey());
        
        // Process the GET request with consistency handling
        String key = request.getKey();
        TimestampedValue value = store.getWithTimestamp(key);
        
        GetResponse response;
        if (value != null) {
            // Success - key found
            response = new GetResponse(
                request.getMessageId(),
                System.currentTimeMillis(),
                replicaId,
                request.getSourceId(),
                key,
                value.getValue(),
                true,
                null,
                value.getTimestamp()
            );
            successfulRequests.incrementAndGet();
            metrics.recordSuccessfulRequest();
        } else {
            // Failure - key not found
            response = new GetResponse(
                request.getMessageId(),
                System.currentTimeMillis(),
                replicaId,
                request.getSourceId(),
                key,
                null,
                false,
                "Key not found",
                0
            );
            failedRequests.incrementAndGet();
            metrics.recordFailedRequest();
        }
        
        // Send the response
        messageBus.sendMessage(response);
        
        logger.debug("Replica {} sending GET response with message ID {} for key {}", replicaId, response.getMessageId(), response.getKey());
    }

    private void handleSetRequest(SetRequest request) {
        if (!running) {
            logger.warn("Ignoring SET_REQUEST message, replica {} is not running", replicaId);
            return;
        }
        
        totalRequests.incrementAndGet();
        
        logger.debug("Replica {} handling SET request with message ID {} for key {}", replicaId, request.getMessageId(), request.getKey());
        
        // Process the SET request with consistency handling
        String key = request.getKey();
        String value = request.getValue();
        long timestamp = request.getValueTimestamp();
        
        // Check for timestamp conflict before setting
        TimestampedValue existingValue = store.getWithTimestamp(key);
        String errorMessage = null;
        boolean success;
        
        if (existingValue != null && existingValue.getTimestamp() > timestamp) {
            success = false;
            errorMessage = String.format("Timestamp conflict: existing value has timestamp %d which is newer than request timestamp %d",
                existingValue.getTimestamp(), timestamp);
        } else {
            success = store.set(key, value, timestamp);
            if (!success) {
                errorMessage = "Failed to set value";
            }
        }
        
        SetResponse response = new SetResponse(
            request.getMessageId(),
            System.currentTimeMillis(),
            replicaId,
            request.getSourceId(),
            key,
            success,
            errorMessage,
            timestamp
        );
        
        if (success) {
            successfulRequests.incrementAndGet();
            metrics.recordSuccessfulRequest();
        } else {
            failedRequests.incrementAndGet();
            metrics.recordFailedRequest();
        }
        
        // Send the response
        messageBus.sendMessage(response);
        
        logger.debug("Replica {} sending SET response with message ID {} for key {}", replicaId, response.getMessageId(), response.getKey());
    }
} 