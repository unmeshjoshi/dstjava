package com.dststore.quorum;

import com.dststore.quorum.messages.GetValueResponse;
import com.dststore.quorum.messages.SetValueResponse;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Tracks the state of client requests in the quorum-based key-value store.
 */
public class ClientState {
    private static final Logger LOGGER = Logger.getLogger(ClientState.class.getName());
    
    // Maps client message IDs to their corresponding futures
    private final Map<String, CompletableFuture<GetValueResponse>> pendingGetRequests = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<SetValueResponse>> pendingSetRequests = new ConcurrentHashMap<>();

    /**
     * Registers a GET request and returns a future that will be completed when the request is fulfilled.
     *
     * @param messageId The client message ID
     * @return A future that will be completed with the response
     */
    public CompletableFuture<GetValueResponse> registerGetRequest(String messageId) {
        CompletableFuture<GetValueResponse> future = new CompletableFuture<>();
        pendingGetRequests.put(messageId, future);
        return future;
    }

    /**
     * Registers a SET request and returns a future that will be completed when the request is fulfilled.
     *
     * @param messageId The client message ID
     * @return A future that will be completed with the response
     */
    public CompletableFuture<SetValueResponse> registerSetRequest(String messageId) {
        CompletableFuture<SetValueResponse> future = new CompletableFuture<>();
        pendingSetRequests.put(messageId, future);
        return future;
    }

    /**
     * Completes a GET request with the given response.
     *
     * @param messageId The client message ID
     * @param response The response to complete the request with
     * @return true if the request was completed, false if it wasn't found
     */
    public boolean completeGetRequest(String messageId, GetValueResponse response) {
        CompletableFuture<GetValueResponse> future = pendingGetRequests.remove(messageId);
        if (future != null) {
            future.complete(response);
            LOGGER.fine("Completed GET request with messageId: " + messageId);
            return true;
        }
        LOGGER.warning("No pending GET request found for messageId: " + messageId);
        return false;
    }

    /**
     * Completes a SET request with the given response.
     *
     * @param messageId The client message ID
     * @param response The response to complete the request with
     * @return true if the request was completed, false if it wasn't found
     */
    public boolean completeSetRequest(String messageId, SetValueResponse response) {
        CompletableFuture<SetValueResponse> future = pendingSetRequests.remove(messageId);
        if (future != null) {
            future.complete(response);
            LOGGER.fine("Completed SET request with messageId: " + messageId);
            return true;
        }
        LOGGER.warning("No pending SET request found for messageId: " + messageId);
        return false;
    }

    /**
     * Completes all pending requests with an exception.
     *
     * @param exception The exception to complete the requests with
     */
    public void completeAllWithException(Throwable exception) {
        pendingGetRequests.forEach((id, future) -> future.completeExceptionally(exception));
        pendingSetRequests.forEach((id, future) -> future.completeExceptionally(exception));
        pendingGetRequests.clear();
        pendingSetRequests.clear();
        LOGGER.info("Completed all pending requests with exception: " + exception.getMessage());
    }
} 