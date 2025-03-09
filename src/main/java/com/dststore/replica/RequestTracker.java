package com.dststore.replica;

import com.dststore.message.Message;
import com.dststore.message.MessageType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;

/**
 * Tracks responses for a given request and determines when a quorum is reached.
 */
public class RequestTracker<T extends Message> {
    private final String requestId;
    private final int quorumSize;
    private final Map<String, Message> responses = new ConcurrentHashMap<>();
    private final Set<String> receivedFrom = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> timeoutFuture;
    private final AtomicInteger responseCount = new AtomicInteger(0);
    private volatile boolean quorumReached = false;
    private CompletableFuture<T> future;

    /**
     * Creates a new RequestTracker for the specified request ID and quorum size.
     *
     * @param requestId The unique identifier for the request
     * @param quorumSize The number of responses required to reach a quorum
     */
    public RequestTracker(String requestId, int quorumSize) {
        this.requestId = requestId;
        this.quorumSize = quorumSize;
        this.scheduler = null; // Scheduler is not needed for deterministic simulation
    }

    /**
     * Adds a response to the tracker and checks if a quorum is reached.
     *
     * @param replicaId The ID of the replica that sent the response
     * @param response The response message
     * @return True if a quorum is reached, false otherwise
     */
    public synchronized boolean addResponse(String replicaId, Message response) {
        if (quorumReached || receivedFrom.contains(replicaId)) {
            return false;
        }

        responses.put(replicaId, response);
        receivedFrom.add(replicaId);
        int count = receivedFrom.size();

        if (count >= quorumSize) {
            quorumReached = true;
            cancelTimeout();
            return true;
        }

        return false;
    }

    /**
     * Starts a timeout for the request.
     *
     * @param timeout The timeout duration in milliseconds
     */
    public void startTimeout(long timeout) {
        timeoutFuture = scheduler.schedule(() -> {
            if (!quorumReached) {
                handleTimeout();
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Cancels the timeout if it is running.
     */
    private void cancelTimeout() {
        if (timeoutFuture != null && !timeoutFuture.isDone()) {
            timeoutFuture.cancel(true);
        }
    }

    /**
     * Handles the timeout scenario for the request.
     */
    private void handleTimeout() {
        // Implement logic to handle timeout, e.g., notify the client or log an error
    }

    /**
     * Gets the best response to return to the client.
     *
     * @return The selected response message
     */
    public T getBestResponse() {
        // Implement logic to select the best response from the received responses
        return (T) responses.values().iterator().next(); // Placeholder
    }

    /**
     * Sets the future associated with this request tracker.
     *
     * @param future The future to set
     */
    public void setFuture(CompletableFuture<T> future) {
        this.future = future;
    }

    /**
     * Gets the future associated with this request tracker.
     *
     * @return The future
     */
    public CompletableFuture<T> getFuture() {
        return future;
    }
} 