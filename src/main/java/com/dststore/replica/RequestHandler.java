package com.dststore.replica;

import com.dststore.message.*;
import com.dststore.network.IMessageBus;
import com.dststore.network.PacketListener;
import com.dststore.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Handles client requests by forwarding them to replica nodes and collecting responses.
 * This class manages the distribution of requests and coordination of responses.
 */
public class RequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);
    
    private final String replicaId;
    private final IMessageBus messageBus;
    private final List<String> replicaIds;
    private final Map<UUID, RequestTracker<?>> requestTrackers = new HashMap<>();
    private final Metrics metrics;
    private int currentReplicaIndex = 0;
    private final Map<String, Long> replicaFailures = new HashMap<>();
    private static final long REPLICA_RETRY_INTERVAL = 5000; // 5 seconds
    private static final long RETRY_INTERVAL_MS = 5000; // 5 seconds

    /**
     * Creates a new RequestHandler with the specified client ID and message bus.
     *
     * @param replicaId The ID of the replica this handler belongs to
     * @param messageBus The message bus for communication
     * @param metrics The metrics for recording client requests
     */
    public RequestHandler(String replicaId, IMessageBus messageBus, Metrics metrics) {
        this.replicaId = replicaId;
        this.messageBus = messageBus;
        this.replicaIds = new ArrayList<>();
        this.metrics = metrics;
        
        setupMessageHandling();
        logger.info("Created RequestHandler for replica {}", replicaId);
    }
    
    private void setupMessageHandling() {
        messageBus.registerListener(new PacketListener() {
            @Override
            public void onPacketReceived(Message message) {
                handleMessage(message);
            }
        });
    }

    private void handleMessage(Message message) {
        if (message instanceof SetResponse) {
            handleSetResponse((SetResponse) message);
        } else if (message instanceof GetResponse) {
            handleGetResponse((GetResponse) message);
        } else if (message instanceof SetRequest) {
            handleSetRequest((SetRequest) message);
        } else if (message instanceof GetRequest) {
            handleGetRequest((GetRequest) message);
        }
    }

    public void start() {
        messageBus.start();
        logger.info("Started RequestHandler for replica {}", replicaId);
    }

    public void stop() {
        messageBus.stop();
        logger.info("Stopped RequestHandler for replica {}", replicaId);
    }

    public void registerReplica(String targetReplicaId, String host, int port) {
        messageBus.connect(targetReplicaId, host, port);
        replicaIds.add(targetReplicaId);
        logger.info("Registered replica {}", targetReplicaId);
    }

    public void registerReplica(String targetReplicaId) {
        registerReplica(targetReplicaId, "localhost", 0); // Port 0 means system will pick an available port
    }

    public void unregisterReplica(String targetReplicaId) {
        messageBus.disconnect(targetReplicaId);
        replicaIds.remove(targetReplicaId);
        logger.info("Unregistered replica {}", targetReplicaId);
    }

    public List<String> getReplicaIds() {
        return new ArrayList<>(replicaIds);
    }

    public void tick() {
        cleanupCompletedRequests();
        // Process any pending operations
        logger.debug("Ticking RequestHandler");
    }

    public CompletableFuture<GetResponse> get(String key) {
        CompletableFuture<GetResponse> future = new CompletableFuture<>();
        try {
            String targetReplica = selectReplica();
            GetRequest request = new GetRequest(replicaId, targetReplica, key);
            RequestTracker<GetResponse> tracker = new RequestTracker<>(request.getMessageId().toString(), replicaIds.size());
            tracker.setOriginalRequest(request);
            requestTrackers.put(request.getMessageId(), tracker);
            
            messageBus.sendMessage(request);
            metrics.recordRequest();
            
            tracker.setFuture(future);
        } catch (IllegalStateException e) {
            handleNoReplicasAvailableForGet(key, future);
        }
        return future;
    }

    public CompletableFuture<SetResponse> set(String key, String value) {
        CompletableFuture<SetResponse> future = new CompletableFuture<>();
        try {
            String targetReplica = selectReplica();
            SetRequest request = new SetRequest(replicaId, targetReplica, key, value);
            RequestTracker<SetResponse> tracker = new RequestTracker<>(request.getMessageId().toString(), replicaIds.size());
            tracker.setOriginalRequest(request);
            requestTrackers.put(request.getMessageId(), tracker);
            
            messageBus.sendMessage(request);
            metrics.recordRequest();
            
            tracker.setFuture(future);
        } catch (IllegalStateException e) {
            handleNoReplicasAvailableForSet(key, future);
        }
        return future;
    }

    private void handleSetRequest(SetRequest request) {
        SetResponse response = new SetResponse(
            UUID.randomUUID(),
            System.currentTimeMillis(),
            replicaId,
            request.getSourceId(),
            request.getKey(),
            true,
            null,
            System.currentTimeMillis()
        );
        messageBus.sendMessage(response);
    }

    private void handleGetRequest(GetRequest request) {
        GetResponse response = new GetResponse(
            UUID.randomUUID(),
            System.currentTimeMillis(),
            replicaId,
            request.getSourceId(),
            request.getKey(),
            "value",
            true,
            null,
            System.currentTimeMillis()
        );
        messageBus.sendMessage(response);
    }

    private String selectReplica() {
        if (replicaIds.isEmpty()) {
            return null;
        }
        // Try each replica in round-robin order until we find an available one
        for (int i = 0; i < replicaIds.size(); i++) {
            currentReplicaIndex = (currentReplicaIndex + 1) % replicaIds.size();
            String replicaId = replicaIds.get(currentReplicaIndex);
            if (isReplicaAvailable(replicaId)) {
                return replicaId;
            }
        }
        // If no replicas are available, return the next one anyway (it will be retried)
        return replicaIds.get((currentReplicaIndex + 1) % replicaIds.size());
    }

    private boolean isReplicaAvailable(String replicaId) {
        Long failureTime = replicaFailures.get(replicaId);
        if (failureTime == null) {
            return true;
        }
        // If more than RETRY_INTERVAL_MS has passed, consider the replica available again
        return System.currentTimeMillis() - failureTime > RETRY_INTERVAL_MS;
    }

    private void markReplicaFailed(String replicaId) {
        replicaFailures.put(replicaId, System.currentTimeMillis());
    }

    private void cleanupCompletedRequests() {
        requestTrackers.entrySet().removeIf(entry -> entry.getValue().getFuture().isDone());
    }

    private void handleSetResponse(SetResponse response) {
        @SuppressWarnings("unchecked")
        RequestTracker<SetResponse> tracker = (RequestTracker<SetResponse>) requestTrackers.get(response.getMessageId());
        if (tracker != null) {
            if (!response.isSuccessful()) {
                // Mark the replica as failed and try another one if available
                markReplicaFailed(response.getSourceId());
                if (tracker.getAttempts() < replicaIds.size() * 2) { // Allow more retries
                    // Try another replica
                    String newTargetReplica = selectReplica();
                    if (newTargetReplica != null) {
                        SetRequest originalRequest = (SetRequest) tracker.getOriginalRequest();
                        SetRequest newRequest = new SetRequest(
                            originalRequest.getMessageId(),
                            System.currentTimeMillis(),
                            originalRequest.getSourceId(),
                            newTargetReplica,
                            originalRequest.getKey(),
                            originalRequest.getValue(),
                            originalRequest.getValueTimestamp()
                        );
                        messageBus.sendMessage(newRequest);
                        tracker.incrementAttempts();
                        return;
                    }
                }
            }
            tracker.getFuture().complete(response);
        }
    }

    private void handleGetResponse(GetResponse response) {
        @SuppressWarnings("unchecked")
        RequestTracker<GetResponse> tracker = (RequestTracker<GetResponse>) requestTrackers.get(response.getMessageId());
        if (tracker != null) {
            if (!response.isSuccessful()) {
                // Try another replica if available
                markReplicaFailed(response.getSourceId());
                if (tracker.getAttempts() < replicaIds.size() * 2) { // Allow more retries
                    String newTargetReplica = selectReplica();
                    if (newTargetReplica != null) {
                        GetRequest originalRequest = (GetRequest) tracker.getOriginalRequest();
                        GetRequest newRequest = new GetRequest(
                            originalRequest.getMessageId(),
                            System.currentTimeMillis(),
                            originalRequest.getSourceId(),
                            newTargetReplica,
                            originalRequest.getKey()
                        );
                        messageBus.sendMessage(newRequest);
                        tracker.incrementAttempts();
                        return;
                    }
                }
            }
            tracker.getFuture().complete(response);
        }
    }

    private void handleNoReplicasAvailableForGet(String key, CompletableFuture<GetResponse> future) {
        GetResponse response = new GetResponse(
            UUID.randomUUID(),
            System.currentTimeMillis(),
            replicaId,
            "client",
            key,
            null,
            false,
            "No replicas available",
            0
        );
        future.complete(response);
    }

    private void handleNoReplicasAvailableForSet(String key, CompletableFuture<SetResponse> future) {
        SetResponse response = new SetResponse(
            UUID.randomUUID(),
            System.currentTimeMillis(),
            replicaId,
            "client",
            key,
            false,
            "No replicas available",
            0
        );
        future.complete(response);
    }

    /**
     * Gets the statistics for this request handler.
     *
     * @return A map of statistic names to values
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("pendingRequests", requestTrackers.size());
        stats.put("registeredReplicas", replicaIds.size());
        return stats;
    }
} 