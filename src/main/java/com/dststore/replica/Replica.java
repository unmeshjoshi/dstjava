package com.dststore.replica;

import com.dststore.message.GetRequest;
import com.dststore.message.GetResponse;
import com.dststore.message.Message;
import com.dststore.message.MessageType;
import com.dststore.message.SetRequest;
import com.dststore.message.SetResponse;
import com.dststore.network.MessageBus;
import com.dststore.storage.KeyValueStore;
import com.dststore.storage.TimestampedValue;
import com.dststore.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A replica in the distributed key-value store.
 * Each replica maintains a local key-value store and processes incoming messages.
 */
public class Replica {
    private static final Logger logger = LoggerFactory.getLogger(Replica.class);
    
    private final String replicaId;
    private final KeyValueStore store;
    private final MessageBus messageBus;
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
    public Replica(String replicaId, MessageBus messageBus, Metrics metrics) {
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
        if (!running) {
            return;
        }
        
        // The actual message processing is done by the message handlers
        // This method is here for future extensions like background tasks,
        // maintenance operations, or state reconciliation
        
        // Tick the message bus to process any queued messages
        messageBus.tick();
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
     */
    public void connectTo(String targetReplicaId) {
        messageBus.connect(targetReplicaId);
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
     * Registers message handlers for different message types.
     */
    private void registerMessageHandlers() {
        // Handler for GET requests
        messageBus.registerHandler(new MessageBus.MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_REQUEST;
            }
            
            @Override
            public void handleMessage(Message message) {
                if (!running) {
                    logger.warn("Ignoring GET_REQUEST message, replica {} is not running", replicaId);
                    return;
                }
                
                totalRequests.incrementAndGet();
                messageTypeCounts.get(MessageType.GET_REQUEST).incrementAndGet();
                
                GetRequest request = (GetRequest) message;
                logger.debug("Replica {} handling GET request with message ID {} for key {}", replicaId, request.getMessageId(), request.getKey());
                
                // Process the GET request with consistency handling
                String key = request.getKey();
                TimestampedValue value = store.getWithTimestamp(key);
                
                GetResponse response;
                if (value != null) {
                    // Success - key found
                    response = new GetResponse(
                        replicaId,
                        request.getSourceId(),
                        key,
                        value.getValue(),
                        value.getTimestamp()
                    );
                    response.setMessageId(request.getMessageId());
                    successfulRequests.incrementAndGet();
                    metrics.recordSuccessfulRequest();
                } else {
                    // Failure - key not found
                    response = new GetResponse(
                        replicaId,
                        request.getSourceId(),
                        key,
                        "Key not found"
                    );
                    response.setMessageId(request.getMessageId());
                    failedRequests.incrementAndGet();
                    metrics.recordFailedRequest();
                }
                
                // Implement conflict resolution if needed
                // Example: Compare timestamps and resolve conflicts

                // Send the response
                messageBus.queueMessage(response);
                
                logger.debug("Replica {} sending GET response with message ID {} for key {}", replicaId, response.getMessageId(), response.getKey());
            }
        });
        
        // Handler for SET requests
        messageBus.registerHandler(new MessageBus.MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.SET_REQUEST;
            }
            
            @Override
            public void handleMessage(Message message) {
                if (!running) {
                    logger.warn("Ignoring SET_REQUEST message, replica {} is not running", replicaId);
                    return;
                }
                
                totalRequests.incrementAndGet();
                messageTypeCounts.get(MessageType.SET_REQUEST).incrementAndGet();
                
                SetRequest request = (SetRequest) message;
                logger.debug("Replica {} handling SET request with message ID {} for key {}", replicaId, request.getMessageId(), request.getKey());
                
                // Process the SET request
                String key = request.getKey();
                String value = request.getValue();
                long timestamp = request.getValueTimestamp();
                
                boolean success = store.set(key, value, timestamp);
                
                // Create and send response
                SetResponse response = new SetResponse(
                    replicaId,
                    request.getSourceId(),
                    key,
                    success ? timestamp : 0L
                );
                response.setMessageId(request.getMessageId());
                
                if (success) {
                    successfulRequests.incrementAndGet();
                    metrics.recordSuccessfulRequest();
                } else {
                    failedRequests.incrementAndGet();
                    metrics.recordFailedRequest();
                    // Make it an error response
                    response = new SetResponse(
                        replicaId,
                        request.getSourceId(),
                        key,
                        "Write rejected due to timestamp conflict"
                    );
                    response.setMessageId(request.getMessageId());
                }
                
                // Send the response
                messageBus.queueMessage(response);
                
                logger.debug("Replica {} sending SET response with message ID {} for key {}", replicaId, response.getMessageId(), response.getKey());
            }
        });
    }
} 