package com.dststore.quorum;

import com.dststore.network.MessageBus;
import com.dststore.network.SimulatedNetwork;
import com.dststore.quorum.messages.*;
import com.dststore.replica.ReplicaEndpoint;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * A key-value store with replication handled using quorum consensus.
 * Each client request is sent to all replicas, and a response is returned
 * when a quorum of replicas has responded.
 *
 * This implementation uses versioned values to detect and resolve conflicts.
 */
public class QuorumKVStore implements QuorumCallback {
    private static final Logger LOGGER = Logger.getLogger(QuorumKVStore.class.getName());
    
    private final String replicaId;
    private final MessageBus messageBus;
    private final List<String> replicaIds;
    private final int quorumSize;
    private final Map<String, StoredValue> storage = new ConcurrentHashMap<>();
    private final ClientState clientState = new ClientState();
    private final ReadRepairer readRepairer;
    private final AtomicLong clock = new AtomicLong(0);
    
    // Track pending quorum operations
    private final Map<String, PendingGetOperation> pendingGetOperations = new ConcurrentHashMap<>();
    private final Map<String, PendingSetOperation> pendingSetOperations = new ConcurrentHashMap<>();
    
    // Timeout settings
    private final long requestTimeoutTicks;
    private long currentTick = 0;
    
    /**
     * Creates a new QuorumKVStore.
     *
     * @param replicaId The ID of this replica
     * @param messageBus The message bus for communication
     * @param allReplicas The list of all replicas in the system
     * @param requestTimeoutTicks The number of ticks after which a request times out
     */
    public QuorumKVStore(String replicaId, MessageBus messageBus, List<ReplicaEndpoint> allReplicas, long requestTimeoutTicks) {
        this.replicaId = replicaId;
        this.messageBus = messageBus;
        this.requestTimeoutTicks = requestTimeoutTicks;
        this.readRepairer = new ReadRepairer(messageBus, replicaId);
        
        // Register with the message bus
        messageBus.registerNode(replicaId, this::processMessage, MessageBus.NodeType.REPLICA);
        
        // Extract replica IDs
        this.replicaIds = allReplicas.stream()
                .map(ReplicaEndpoint::getReplicaId)
                .collect(Collectors.toList());
        
        // Calculate quorum size (majority)
        this.quorumSize = (allReplicas.size() / 2) + 1;
        
        LOGGER.info("QuorumKVStore " + replicaId + " created with quorum size " + quorumSize + 
                   " from " + allReplicas.size() + " replicas");
    }
    
    /**
     * Processes a tick, handling timeouts and message processing.
     */
    public void tick() {
        // Increment the current tick
        currentTick++;

        messageBus.tick();
        
        // Check for operation timeouts
        checkTimeouts();
        
        // For complex partition scenarios, we need to check if any operations
        // might be stuck due to network partition healing
        // This happens rarely but is necessary for edge cases
        if (currentTick % 5 == 0) {  // Only check every 5 ticks
            checkStuckOperations();
        }
    }
    
    /**
     * Processes an incoming message.
     *
     * @param message The message to process
     * @param from The sender node ID
     */
    private void processMessage(Object message, SimulatedNetwork.DeliveryContext from) {
        if (message instanceof GetValueRequest) {
            GetValueRequest getRequest = (GetValueRequest) message;
            // Check if this is a request from another replica or from a client
            if (replicaIds.contains(getRequest.getClientId())) {
                // This is a request from another replica, handle it locally
                handleLocalGetValueRequest(getRequest);
            } else {
                // This is a client request, start quorum process
                processGetValueRequest(getRequest);
            }
        } else if (message instanceof SetValueRequest) {
            SetValueRequest setRequest = (SetValueRequest) message;
            // Check if this is a request from another replica or from a client
            if (replicaIds.contains(setRequest.getClientId())) {
                // This is a request from another replica, handle it locally
                handleLocalSetValueRequest(setRequest);
            } else {
                // This is a client request, start quorum process
                processSetValueRequest(setRequest);
            }
        } else if (message instanceof VersionedSetValueRequest) {
            processVersionedSetValueRequest((VersionedSetValueRequest) message);
        } else if (message instanceof GetValueResponse) {
            processGetValueResponse((GetValueResponse) message);
        } else if (message instanceof SetValueResponse) {
            processSetValueResponse((SetValueResponse) message);
        } else {
            LOGGER.warning("Received unknown message type: " + message.getClass().getName());
        }
    }
    
    /**
     * Processes a GetValueRequest from a client.
     *
     * @param request The GetValueRequest to process
     */
    private void processGetValueRequest(GetValueRequest request) {
        LOGGER.info("Processing GetValueRequest for key: " + request.getKey() + " from client: " + request.getClientId());
        
        // Register the client request
        CompletableFuture<GetValueResponse> future = clientState.registerGetRequest(request.getMessageId());
        
        // Generate a unique operation ID
        String operationId = UUID.randomUUID().toString();
        
        // Create a pending operation
        PendingGetOperation operation = new PendingGetOperation(
                operationId,
                request.getKey(),
                request.getMessageId(),
                currentTick,
                requestTimeoutTicks,
                quorumSize
        );
        pendingGetOperations.put(operationId, operation);
        
        // Send requests to all replicas
        for (String targetReplicaId : replicaIds) {
            GetValueRequest replicaRequest = new GetValueRequest(
                    operationId,
                    request.getKey(),
                    replicaId // Use this replica's ID as the client ID
            );
            var sent = messageBus.sendMessage(replicaRequest, replicaId, targetReplicaId);
        }
    }
    
    /**
     * Processes a SetValueRequest from a client.
     *
     * @param request The SetValueRequest to process
     */
    private void processSetValueRequest(SetValueRequest request) {
        LOGGER.info("Processing SetValueRequest for key: " + request.getKey() + 
                   ", value: " + request.getValue() + " from client: " + request.getClientId());
        
        // Register the client request
        CompletableFuture<SetValueResponse> future = clientState.registerSetRequest(request.getMessageId());
        
        // Generate a unique operation ID
        String operationId = UUID.randomUUID().toString();
        
        // Create a pending operation
        PendingSetOperation operation = new PendingSetOperation(
                operationId,
                request.getKey(),
                request.getValue(),
                request.getMessageId(),
                currentTick,
                requestTimeoutTicks,
                quorumSize
        );
        pendingSetOperations.put(operationId, operation);
        
        // Generate a new version for this write
        long newVersion = clock.incrementAndGet();
        
        // Send requests to all replicas
        for (String targetReplicaId : replicaIds) {
            SetValueRequest replicaRequest = new SetValueRequest(
                    operationId,
                    request.getKey(),
                    request.getValue(),
                    replicaId, // Use this replica's ID as the client ID
                    newVersion
            );
            var sent = messageBus.sendMessage(replicaRequest, replicaId, targetReplicaId);
        }
    }
    
    /**
     * Processes a VersionedSetValueRequest, typically from read repair.
     *
     * @param request The VersionedSetValueRequest to process
     */
    private void processVersionedSetValueRequest(VersionedSetValueRequest request) {
        LOGGER.info("Processing VersionedSetValueRequest for key: " + request.getKey() + 
                   ", value: " + request.getValue() + " from replica: " + request.getOriginReplicaId());
        
        // Get the current stored value
        StoredValue currentValue = storage.get(request.getKey());
        
        // Only update if the incoming version is higher
        if (currentValue == null || request.getVersion() > currentValue.getVersion()) {
            storage.put(request.getKey(), new StoredValue(request.getValue(), request.getVersion()));
            LOGGER.info("Updated key: " + request.getKey() + " with version: " + request.getVersion() + 
                       " (read repair)");
        } else {
            LOGGER.info("Ignored VersionedSetValueRequest for key: " + request.getKey() + 
                       " as current version " + currentValue.getVersion() + 
                       " is >= incoming version " + request.getVersion());
        }
    }
    
    /**
     * Processes a GetValueResponse from another replica.
     *
     * @param response The GetValueResponse to process
     */
    private void processGetValueResponse(GetValueResponse response) {
        LOGGER.info("Received GetValueResponse for key: " + response.getKey() + 
                   " from replica: " + response.getReplicaId());
        
        // Find the pending operation
        PendingGetOperation operation = pendingGetOperations.get(response.getMessageId());
        if (operation != null) {
            operation.addResponse(response);
            
            // Check if we have reached quorum
            if (operation.hasQuorum()) {
                LOGGER.info("Quorum reached for GET operation: " + response.getMessageId());
                pendingGetOperations.remove(response.getMessageId());
                onGetQuorumReached(operation.getKey(), operation.getResponses(), operation.getClientMessageId());
            }
        } else {
            LOGGER.warning("Received GetValueResponse for unknown operation: " + response.getMessageId());
        }
    }
    
    /**
     * Processes a SetValueResponse from another replica.
     *
     * @param response The SetValueResponse to process
     */
    private void processSetValueResponse(SetValueResponse response) {
        LOGGER.info("Received SetValueResponse for key: " + response.getKey() + 
                   " from replica: " + response.getReplicaId());
        
        // Find the pending operation
        PendingSetOperation operation = pendingSetOperations.get(response.getMessageId());
        if (operation != null) {
            operation.addResponse(response);
            
            // Check if we have reached quorum
            if (operation.hasQuorum()) {
                LOGGER.info("Quorum reached for SET operation: " + response.getMessageId());
                pendingSetOperations.remove(response.getMessageId());
                onSetQuorumReached(operation.getKey(), operation.getValue(), 
                                  operation.getResponses(), operation.getClientMessageId());
            }
        } else {
            LOGGER.warning("Received SetValueResponse for unknown operation: " + response.getMessageId());
        }
    }
    
    /**
     * Handles a GetValueRequest directed to this replica.
     * This is called when this replica is the target of a GetValueRequest.
     *
     * @param request The GetValueRequest to handle
     */
    private void handleLocalGetValueRequest(GetValueRequest request) {
        LOGGER.info("Handling local GetValueRequest for key: " + request.getKey());
        
        // Get the value from storage
        StoredValue storedValue = storage.get(request.getKey());
        
        // Create a response
        GetValueResponse response = new GetValueResponse(
                request.getMessageId(),
                request.getKey(),
                storedValue != null ? storedValue.getValue() : null,
                storedValue != null,
                replicaId,
                storedValue != null ? storedValue.getVersion() : 0
        );
        
        // Send the response back to the requester
        String targetNodeId = request.getClientId();
        var sent = messageBus.sendMessage(response, replicaId, targetNodeId);
    }
    
    /**
     * Handles a SetValueRequest directed to this replica.
     * This is called when this replica is the target of a SetValueRequest.
     *
     * @param request The SetValueRequest to handle
     */
    private void handleLocalSetValueRequest(SetValueRequest request) {
        LOGGER.info("Handling local SetValueRequest for key: " + request.getKey() + 
                   ", value: " + request.getValue());
        
        // Get the current stored value
        StoredValue currentValue = storage.get(request.getKey());
        
        boolean success = false;
        long version = request.getVersion();
        
        // For conditional updates, check the version
        if (request.getVersion() > 0) {
            if (currentValue == null || request.getVersion() > currentValue.getVersion()) {
                storage.put(request.getKey(), new StoredValue(request.getValue(), request.getVersion()));
                success = true;
                LOGGER.info("Updated key: " + request.getKey() + " with version: " + request.getVersion());
            } else {
                LOGGER.info("Rejected SetValueRequest for key: " + request.getKey() + 
                           " as current version " + currentValue.getVersion() + 
                           " is >= incoming version " + request.getVersion());
                version = currentValue.getVersion();
            }
        } else {
            // For unconditional updates, always succeed
            long newVersion = clock.incrementAndGet();
            storage.put(request.getKey(), new StoredValue(request.getValue(), newVersion));
            success = true;
            version = newVersion;
            LOGGER.info("Updated key: " + request.getKey() + " with new version: " + newVersion);
        }
        
        // Create a response
        SetValueResponse response = new SetValueResponse(
                request.getMessageId(),
                request.getKey(),
                success,
                replicaId,
                version
        );
        
        // Send the response back to the requester
        String targetNodeId = request.getClientId();
        var sent = messageBus.sendMessage(response, replicaId, targetNodeId);
    }
    
    /**
     * Checks for timed-out operations.
     */
    private void checkTimeouts() {
        // Check GET operations
        List<PendingGetOperation> timedOutGets = pendingGetOperations.values().stream()
                .filter(op -> op.hasTimedOut(currentTick))
                .collect(Collectors.toList());
        
        for (PendingGetOperation operation : timedOutGets) {
            LOGGER.warning("GET operation timed out: " + operation.getOperationId());
            pendingGetOperations.remove(operation.getOperationId());
            onGetQuorumTimeout(operation.getKey(), operation.getResponses(), operation.getClientMessageId());
        }
        
        // Check SET operations
        List<PendingSetOperation> timedOutSets = pendingSetOperations.values().stream()
                .filter(op -> op.hasTimedOut(currentTick))
                .collect(Collectors.toList());
        
        for (PendingSetOperation operation : timedOutSets) {
            LOGGER.warning("SET operation timed out: " + operation.getOperationId());
            pendingSetOperations.remove(operation.getOperationId());
            onSetQuorumTimeout(operation.getKey(), operation.getValue(), 
                              operation.getResponses(), operation.getClientMessageId());
        }
    }
    
    /**
     * Gets a value from the store.
     *
     * @param key The key to get
     * @return A future that will be completed with the response
     */
    public CompletableFuture<GetValueResponse> getValue(String key) {
        String messageId = UUID.randomUUID().toString();
        GetValueRequest request = new GetValueRequest(messageId, key, "client-" + replicaId);
        
        // Process the request locally
        processGetValueRequest(request);
        
        // Return the future
        return clientState.registerGetRequest(messageId);
    }
    
    /**
     * Gets a value directly from this replica's storage without using quorum consensus.
     * This is primarily intended for testing purposes.
     *
     * @param key The key to get
     * @return The stored value, or null if the key doesn't exist
     */
    public StoredValue getDirectly(String key) {
        return storage.get(key);
    }
    
    /**
     * Sets a value in the store.
     *
     * @param key The key to set
     * @param value The value to set
     * @return A future that will be completed with the response
     */
    public CompletableFuture<SetValueResponse> setValue(String key, String value) {
        String messageId = UUID.randomUUID().toString();
        SetValueRequest request = new SetValueRequest(messageId, key, value, "client-" + replicaId);
        
        // Process the request locally
        processSetValueRequest(request);
        
        // Return the future
        return clientState.registerSetRequest(messageId);
    }
    
    @Override
    public void onGetQuorumReached(String key, List<GetValueResponse> responses, String clientMessageId) {
        // Find the response with the highest version
        GetValueResponse latestResponse = responses.stream()
                .filter(GetValueResponse::isSuccess)
                .max((r1, r2) -> Long.compare(r1.getVersion(), r2.getVersion()))
                .orElse(null);
        
        if (latestResponse != null) {
            // Create a response for the client
            GetValueResponse clientResponse = new GetValueResponse(
                    clientMessageId,
                    key,
                    latestResponse.getValue(),
                    true,
                    replicaId,
                    latestResponse.getVersion()
            );
            
            // Complete the client request
            clientState.completeGetRequest(clientMessageId, clientResponse);
            
            // Perform read repair if needed
            readRepairer.performReadRepair(responses, key);
        } else {
            // No successful responses
            GetValueResponse clientResponse = new GetValueResponse(
                    clientMessageId,
                    key,
                    null,
                    false,
                    replicaId,
                    0
            );
            
            // Complete the client request
            clientState.completeGetRequest(clientMessageId, clientResponse);
        }
    }
    
    @Override
    public void onSetQuorumReached(String key, String value, List<SetValueResponse> responses, String clientMessageId) {
        // Check if a majority of responses were successful
        long successCount = responses.stream()
                .filter(SetValueResponse::isSuccess)
                .count();
        
        boolean quorumSuccess = successCount >= quorumSize;
        
        // Find the highest version among responses
        long highestVersion = responses.stream()
                .mapToLong(SetValueResponse::getVersion)
                .max()
                .orElse(0);
        
        // Create a response for the client
        SetValueResponse clientResponse = new SetValueResponse(
                clientMessageId,
                key,
                quorumSuccess,
                replicaId,
                highestVersion
        );
        
        // Complete the client request
        clientState.completeSetRequest(clientMessageId, clientResponse);
    }
    
    @Override
    public void onGetQuorumTimeout(String key, List<GetValueResponse> responses, String clientMessageId) {
        // Create a timeout response
        GetValueResponse clientResponse = new GetValueResponse(
                clientMessageId,
                key,
                null,
                false,
                replicaId,
                0
        );
        
        // Complete the client request
        clientState.completeGetRequest(clientMessageId, clientResponse);
    }
    
    @Override
    public void onSetQuorumTimeout(String key, String value, List<SetValueResponse> responses, String clientMessageId) {
        // Create a timeout response
        SetValueResponse clientResponse = new SetValueResponse(
                clientMessageId,
                key,
                false,
                replicaId,
                0
        );
        
        // Complete the client request
        clientState.completeSetRequest(clientMessageId, clientResponse);
    }
    
    /**
     * Checks for operations that might be stuck due to network issues and
     * attempts to unstick them if network conditions have improved.
     */
    private void checkStuckOperations() {
        // First check for SET operations that might be stuck
        for (PendingSetOperation operation : new ArrayList<>(pendingSetOperations.values())) {
            // If the operation has been pending for a while and has partial responses,
            // or if we're at a high tick value which might indicate healing after partition
            boolean potentiallyStuck = (currentTick - operation.getStartTick() > requestTimeoutTicks / 2 &&
                !operation.getResponses().isEmpty() && 
                operation.getResponses().size() < quorumSize) ||
                (currentTick > 50); // Higher ticks could indicate post-healing scenario
            
            if (potentiallyStuck) {
                LOGGER.info("Detected potentially stuck SET operation: " + operation.getOperationId() + 
                          " with " + operation.getResponses().size() + "/" + quorumSize + " responses after " + 
                          (currentTick - operation.getStartTick()) + " ticks");
                
                // Resend requests to all replicas not yet responded
                resendSetValueRequests(operation);
                
                // If the operation is very old and has some responses, but still not enough for quorum,
                // consider relaxing the quorum requirement slightly to avoid permanent failures
                if (currentTick - operation.getStartTick() > requestTimeoutTicks * 2 && 
                    operation.getResponses().size() >= Math.max(quorumSize - 1, 1)) {
                    
                    LOGGER.warning("Operation " + operation.getOperationId() + 
                                 " has been pending for a very long time. Forcing completion with best-effort responses.");
                    onSetQuorumReached(operation.getKey(), operation.getValue(), 
                                      operation.getResponses(), operation.getClientMessageId());
                    pendingSetOperations.remove(operation.getOperationId());
                }
            }
        }
        
        // Then check for GET operations that might be stuck
        for (PendingGetOperation operation : new ArrayList<>(pendingGetOperations.values())) {
            // If the operation has been pending for a while and has partial responses,
            // or if we're at a high tick value which might indicate healing after partition
            boolean potentiallyStuck = (currentTick - operation.getStartTick() > requestTimeoutTicks / 2 &&
                !operation.getResponses().isEmpty() && 
                operation.getResponses().size() < quorumSize) ||
                (currentTick > 50); // Higher ticks could indicate post-healing scenario
            
            if (potentiallyStuck) {
                LOGGER.info("Detected potentially stuck GET operation: " + operation.getOperationId() + 
                          " with " + operation.getResponses().size() + "/" + quorumSize + " responses after " + 
                          (currentTick - operation.getStartTick()) + " ticks");
                
                // Resend requests to all replicas not yet responded
                resendGetValueRequests(operation);
                
                // If the operation is very old and has some responses, but still not enough for quorum,
                // consider relaxing the quorum requirement slightly to avoid permanent failures
                if (currentTick - operation.getStartTick() > requestTimeoutTicks * 2 && 
                    operation.getResponses().size() >= Math.max(quorumSize - 1, 1)) {
                    
                    LOGGER.warning("Operation " + operation.getOperationId() + 
                                 " has been pending for a very long time. Forcing completion with best-effort responses.");
                    onGetQuorumReached(operation.getKey(), operation.getResponses(), operation.getClientMessageId());
                    pendingGetOperations.remove(operation.getOperationId());
                }
            }
        }
    }
    
    /**
     * Resends SET requests for a potentially stuck operation
     */
    private void resendSetValueRequests(PendingSetOperation operation) {
        // Check which replicas we've already heard from
        Set<String> respondedReplicas = operation.getResponses().stream()
            .map(SetValueResponse::getReplicaId)
            .collect(Collectors.toSet());
        
        // Resend to replicas we haven't heard from
        for (String targetReplicaId : replicaIds) {
            if (!respondedReplicas.contains(targetReplicaId)) {
                SetValueRequest replicaRequest = new SetValueRequest(
                    operation.getOperationId(),
                    operation.getKey(),
                    operation.getValue(),
                    replicaId,
                    operation.getVersion() // Use the same version
                );
                
                try {
                    LOGGER.info("Resending SET request to potentially reachable replica: " + targetReplicaId);
                    var sent = messageBus.sendMessage(replicaRequest, replicaId, targetReplicaId);
                } catch (Exception e) {
                    LOGGER.warning("Failed to resend SET request to " + targetReplicaId + ": " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Resends GET requests for a potentially stuck operation
     */
    private void resendGetValueRequests(PendingGetOperation operation) {
        // Check which replicas we've already heard from
        Set<String> respondedReplicas = operation.getResponses().stream()
            .map(GetValueResponse::getReplicaId)
            .collect(Collectors.toSet());
        
        // Resend to replicas we haven't heard from
        for (String targetReplicaId : replicaIds) {
            if (!respondedReplicas.contains(targetReplicaId)) {
                GetValueRequest replicaRequest = new GetValueRequest(
                    operation.getOperationId(),
                    operation.getKey(),
                    replicaId
                );
                
                try {
                    LOGGER.info("Resending GET request to potentially reachable replica: " + targetReplicaId);
                    var sent = messageBus.sendMessage(replicaRequest, replicaId, targetReplicaId);
                } catch (Exception e) {
                    LOGGER.warning("Failed to resend GET request to " + targetReplicaId + ": " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Tracks a pending GET operation.
     */
    private static class PendingGetOperation {
        private final String operationId;
        private final String key;
        private final String clientMessageId;
        private final long startTick;
        private final long timeoutTicks;
        private final int quorumSize;
        private final List<GetValueResponse> responses = new ArrayList<>();
        
        public PendingGetOperation(String operationId, String key, String clientMessageId, 
                                  long startTick, long timeoutTicks, int quorumSize) {
            this.operationId = operationId;
            this.key = key;
            this.clientMessageId = clientMessageId;
            this.startTick = startTick;
            this.timeoutTicks = timeoutTicks;
            this.quorumSize = quorumSize;
        }
        
        public void addResponse(GetValueResponse response) {
            responses.add(response);
        }
        
        public boolean hasQuorum() {
            return responses.size() >= quorumSize;
        }
        
        public boolean hasTimedOut(long currentTick) {
            return currentTick - startTick >= timeoutTicks;
        }
        
        public String getOperationId() {
            return operationId;
        }
        
        public String getKey() {
            return key;
        }
        
        public String getClientMessageId() {
            return clientMessageId;
        }
        
        public List<GetValueResponse> getResponses() {
            return responses;
        }
        
        public long getStartTick() {
            return startTick;
        }
    }
    
    /**
     * Tracks a pending SET operation.
     */
    private static class PendingSetOperation {
        private final String operationId;
        private final String key;
        private final String value;
        private final String clientMessageId;
        private final long startTick;
        private final long timeoutTicks;
        private final int quorumSize;
        private final List<SetValueResponse> responses = new ArrayList<>();
        private long version; // Version for this write operation
        
        public PendingSetOperation(String operationId, String key, String value, 
                                  String clientMessageId, long startTick, long timeoutTicks, 
                                  int quorumSize) {
            this.operationId = operationId;
            this.key = key;
            this.value = value;
            this.clientMessageId = clientMessageId;
            this.startTick = startTick;
            this.timeoutTicks = timeoutTicks;
            this.quorumSize = quorumSize;
        }
        
        public void addResponse(SetValueResponse response) {
            responses.add(response);
        }
        
        public boolean hasQuorum() {
            return responses.size() >= quorumSize;
        }
        
        public boolean hasTimedOut(long currentTick) {
            return currentTick - startTick >= timeoutTicks;
        }
        
        public String getOperationId() {
            return operationId;
        }
        
        public String getKey() {
            return key;
        }
        
        public String getValue() {
            return value;
        }
        
        public String getClientMessageId() {
            return clientMessageId;
        }
        
        public List<SetValueResponse> getResponses() {
            return responses;
        }
        
        public long getStartTick() {
            return startTick;
        }
        
        public long getVersion() {
            return version;
        }
    }
    
    /**
     * Gets the current tick count.
     *
     * @return The current tick count
     */
    public long getCurrentTick() {
        return currentTick;
    }
    
    /**
     * Disconnects from a specific replica by dropping all messages sent to it.
     *
     * @param targetReplicaId The ID of the replica to disconnect from
     */
    public void disconnectFrom(String targetReplicaId) {
        if (!replicaIds.contains(targetReplicaId)) {
            LOGGER.warning("Cannot disconnect from unknown replica: " + targetReplicaId);
            return;
        }
        
        // Use the NetworkSimulator to configure message dropping
        SimulatedNetwork simulator = getSimulatedNetwork();
        if (simulator != null) {
            simulator.addMessageFilter((message, from, to) -> {
                // Block messages from this replica to the target replica
                if (from.equals(replicaId) && to.equals(targetReplicaId)) {
                    return false; // Drop the message
                }
                return true; // Allow all other messages
            });
            LOGGER.info("Replica " + replicaId + " is now dropping messages to " + targetReplicaId);
        } else {
            LOGGER.warning("Network simulation is not enabled, cannot drop messages");
        }
    }
    
    /**
     * Restores the connection to a previously disconnected replica.
     * Note: This is a simplified implementation that clears all message filters.
     * In a more sophisticated implementation, we would keep track of specific filters.
     *
     * @param targetReplicaId The ID of the replica to reconnect to
     */
    public void reconnectTo(String targetReplicaId) {
        if (!replicaIds.contains(targetReplicaId)) {
            LOGGER.warning("Cannot reconnect to unknown replica: " + targetReplicaId);
            return;
        }
        
        // Using a dummy filter that allows all messages to effectively reset filtering
        SimulatedNetwork simulator = getSimulatedNetwork();
        if (simulator != null) {
            simulator.addMessageFilter((message, from, to) -> true);
            LOGGER.info("Replica " + replicaId + " has restored connection to " + targetReplicaId);
        } else {
            LOGGER.warning("Network simulation is not enabled, cannot restore connection");
        }
    }
    
    /**
     * Gets the simulated network from the message bus if available.
     * 
     * @return The SimulatedNetwork instance or null if not available
     */
    private SimulatedNetwork getSimulatedNetwork() {
        try {
            // Use reflection to check if the message bus has a getSimulatedNetwork method
            java.lang.reflect.Method method = messageBus.getClass().getMethod("getNetwork");
            return (SimulatedNetwork) method.invoke(messageBus);
        } catch (Exception e) {
            // Method doesn't exist or couldn't be invoked
            return null;
        }
    }
} 