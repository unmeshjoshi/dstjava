package com.dststore.replica;

import com.dststore.message.*;
import com.dststore.network.MessageBus;
import com.dststore.network.SimulatedNetwork;

import java.util.*;
import java.util.logging.Logger;

/**
 * A simple implementation of a replica that uses a basic quorum consensus approach.
 */
public class SimpleReplica extends Replica {
    private static final Logger LOGGER = Logger.getLogger(SimpleReplica.class.getName());
    
    // Storage for key-value pairs
    private final Map<String, String> storage = new HashMap<>();
    
    // Tracking for pending quorum operations
    private final Map<String, SimpleQuorumTracker> pendingQuorum = new HashMap<>();
    
    /**
     * Creates a new SimpleReplica with the specified parameters.
     *
     * @param replicaId The ID of this replica
     * @param messageBus The message bus for communication
     * @param ipAddress The IP address of this replica
     * @param port The port of this replica
     * @param allReplicas The list of all replicas in the system
     * @param requestTimeoutTicks The number of ticks after which a request times out
     */
    public SimpleReplica(String replicaId, MessageBus messageBus, String ipAddress, int port, 
                        List<ReplicaEndpoint> allReplicas, long requestTimeoutTicks) {
        super(replicaId, messageBus, ipAddress, port, allReplicas, requestTimeoutTicks);
    }
    
    /**
     * Creates a new SimpleReplica with default timeout.
     *
     * @param replicaId The ID of this replica
     * @param messageBus The message bus for communication
     * @param ipAddress The IP address of this replica
     * @param port The port of this replica
     * @param allReplicas The list of all replicas in the system
     */
    public SimpleReplica(String replicaId, MessageBus messageBus, String ipAddress, int port, 
                        List<ReplicaEndpoint> allReplicas) {
        super(replicaId, messageBus, ipAddress, port, allReplicas);
    }
    
    /**
     * Creates a new SimpleReplica with default values.
     *
     * @param replicaId The ID of this replica
     * @param messageBus The message bus for communication
     */
    public SimpleReplica(String replicaId, MessageBus messageBus) {
        super(replicaId, messageBus);
    }
    
    /**
     * Sets a value in the local storage.
     *
     * @param key The key to set
     * @param value The value to set
     */
    public void setValue(String key, String value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        storage.put(key, value);
        System.out.println("Replica " + replicaId + " set value " + key + "=" + value);
    }
    
    @Override
    protected void processMessage(Object message, SimulatedNetwork.DeliveryContext from) {
        if (message instanceof GetRequest) {
            LOGGER.info("Replica " + replicaId + " processing GetRequest: " + 
                        ((GetRequest) message).getKey());
            processGetRequest((GetRequest) message);
        } else if (message instanceof PutRequest) {
            PutRequest putRequest = (PutRequest) message;
            LOGGER.info("Replica " + replicaId + " processing PutRequest: " + 
                        putRequest.getKey() + "=" + putRequest.getValue());
            processPutRequest(putRequest);
        } else if (message instanceof ReplicaRequest) {
            LOGGER.info("Replica " + replicaId + " processing ReplicaRequest: " + 
                        ((ReplicaRequest) message).getKey());
            processReplicaRequest((ReplicaRequest) message);
        } else if (message instanceof ReplicaResponse) {
            LOGGER.info("Replica " + replicaId + " processing ReplicaResponse for request: " + 
                        ((ReplicaResponse) message).getRequestId());
            processReplicaResponse((ReplicaResponse) message);
        } else {
            LOGGER.warning("Replica " + replicaId + " received unknown message type: " + 
                           message.getClass().getName());
        }
    }
    
    private void processGetRequest(GetRequest request) {
        // Common handling for client requests
        String clientMessageId = request.getMessageId();
        processClientRequest(
            request.getClientId(),
            clientMessageId,
            request.getKey(),
            null, // No value for GET requests
            OperationType.GET
        );
    }
    
    private void processPutRequest(PutRequest request) {
        // Common handling for client requests
        String clientMessageId = request.getMessageId();
        processClientRequest(
            request.getClientId(),
            clientMessageId,
            request.getKey(),
            request.getValue(),
            OperationType.PUT
        );
    }
    
    private void processClientRequest(String clientId, String clientMessageId, String key, 
                                     String value, OperationType operationType) {
        // Generate a unique ID for this consensus operation
        String requestId = generateRequestId();
        System.out.println("Replica " + replicaId + " generated requestId " + requestId + 
                          " for client " + operationType + " request " + clientMessageId + 
                          " at tick " + currentTick);
        
        // Create a new quorum tracker
        SimpleQuorumTracker tracker = new SimpleQuorumTracker(
            requestId, 
            clientId, 
            clientMessageId, 
            key, 
            quorumSize, 
            operationType, 
            currentTick, 
            requestTimeoutTicks
        );
        pendingQuorum.put(requestId, tracker);
        
        // Process the request locally
        boolean success;
        if (operationType == OperationType.GET) {
            // Lookup the value
            String storedValue = storage.get(key);
            success = storedValue != null;
            value = storedValue != null ? storedValue : "";
            
            System.out.println("Replica " + replicaId + " local lookup for key " + key + 
                             ": found=" + success + 
                             ", value=" + (storedValue != null ? "'" + storedValue + "'" : "null"));
        } else {
            // Store the value locally
            storage.put(key, value);
            success = true;
            System.out.println("Replica " + replicaId + " stored key=" + key + 
                             " with value='" + value + "' locally");
        }
        
        // Create and track our own response
        ReplicaResponse localResponse = new ReplicaResponse(
            requestId, replicaId, success, key, value != null ? value : ""
        );
        tracker.addResponse(localResponse);
        System.out.println("Replica " + replicaId + " added own response to tracker");
        
        // Forward to all peers
        forwardRequestToPeers(requestId, clientId, key, value, operationType);
        
        // Check if we already have quorum (e.g., single-node case)
        checkForQuorumAndRespond(requestId);
    }
    
    private void forwardRequestToPeers(String requestId, String clientId, String key, 
                                      String value, OperationType operationType) {
        int forwardCount = 0;
        for (Map.Entry<String, ReplicaEndpoint> entry : peers.entrySet()) {
            String peerId = entry.getKey();
            if (!peerId.equals(replicaId)) {
                ReplicaRequest replicaRequest = new ReplicaRequest(
                    requestId, 
                    replicaId, 
                    clientId, 
                    key, 
                    operationType.toString(), 
                    value != null ? value : ""
                );
                var sent = messageBus.sendMessage(replicaRequest, replicaId, peerId);
                forwardCount++;
                System.out.println("Replica " + replicaId + " forwarded " + operationType + 
                                 " request to peer " + peerId);
            }
        }
        System.out.println("Replica " + replicaId + " forwarded " + operationType + 
                         " request to " + forwardCount + " peers");
    }
    
    private void processReplicaRequest(ReplicaRequest request) {
        // Process the forwarded request and send response back to originator
        String key = request.getKey();
        boolean success = false;
        String value = "";
        String operation = request.getOperation();
        
        if (OperationType.GET.toString().equals(operation)) {
            // Handle GET operation
            value = storage.get(key);
            success = value != null;
            value = (value != null) ? value : "";
            
            System.out.println("Replica " + replicaId + " processing GET replica request for key " + key + 
                            " from " + request.getOriginReplicaId() + 
                            ": found=" + success + 
                            ", value=" + (value.isEmpty() ? "null" : "'" + value + "'"));
        } 
        else if (OperationType.PUT.toString().equals(operation)) {
            // Handle PUT operation
            storage.put(key, request.getValue());
            success = true;
            value = request.getValue();
            
            System.out.println("Replica " + replicaId + " processing PUT replica request for key " + key + 
                            " from " + request.getOriginReplicaId() + 
                            ": value='" + value + "'");
        }
        
        // Send response back to the originating replica
        sendReplicaResponse(request, success, value);
    }
    
    private void sendReplicaResponse(ReplicaRequest request, boolean success, String value) {
        ReplicaResponse response = new ReplicaResponse(
            request.getRequestId(), replicaId, success, request.getKey(), value
        );

        String targetNodeId = request.getOriginReplicaId();
        var sent = messageBus.sendMessage(response, replicaId, targetNodeId);
        System.out.println("Replica " + replicaId + " sent response back to " + request.getOriginReplicaId());
    }
    
    private void processReplicaResponse(ReplicaResponse response) {
        SimpleQuorumTracker tracker = pendingQuorum.get(response.getRequestId());
        if (tracker != null) {
            LOGGER.info("Replica " + replicaId + " received response from " + 
                       response.getResponderReplicaId() + 
                       " for request " + response.getRequestId() + 
                       ": success=" + response.isSuccess() + 
                       ", value=" + response.getValue());
            
            tracker.addResponse(response);
            LOGGER.info("Replica " + replicaId + " now has " + 
                        (tracker.hasQuorum() ? "reached" : "not reached") + 
                        " quorum (required: " + quorumSize + ")");
            
            checkForQuorumAndRespond(response.getRequestId());
        } else {
            LOGGER.warning("Replica " + replicaId + " received response for unknown request: " + 
                           response.getRequestId());
        }
    }
    
    private void checkForQuorumAndRespond(String requestId) {
        SimpleQuorumTracker tracker = pendingQuorum.get(requestId);
        if (tracker != null && tracker.hasQuorum()) {
            System.out.println("Replica " + replicaId + " has quorum for request " + requestId + 
                              ", sending response to client " + tracker.getClientId() + 
                              " at tick " + currentTick);
            
            // Determine the correct response type based on the operation in the tracker
            OperationType operationType = tracker.getOperationType();
            
            if (operationType == OperationType.PUT) {
                sendPutResponse(tracker);
            } else {
                sendGetResponse(tracker);
            }
            
            // Mark as completed and remove from pending
            tracker.markCompleted();
            pendingQuorum.remove(requestId);
            System.out.println("Replica " + replicaId + " marked request " + requestId + " as completed");
        } else if (tracker != null) {
            System.out.println("Replica " + replicaId + " does not yet have quorum for request " + requestId);
        }
    }
    
    private void sendPutResponse(SimpleQuorumTracker tracker) {
        PutResponse clientResponse = new PutResponse(
            tracker.getClientMessageId(),
            tracker.getKey(),
            true,
            replicaId
        );
        String targetNodeId = tracker.getClientId();
        var sent = messageBus.sendMessage(clientResponse, replicaId, targetNodeId);
        System.out.println("Replica " + replicaId + " sent PutResponse to client: " +
                        "messageId=" + clientResponse.getMessageId() + 
                        ", key=" + clientResponse.getKey() + 
                        ", success=" + clientResponse.isSuccess());
    }
    
    private void sendGetResponse(SimpleQuorumTracker tracker) {
        GetResponse clientResponse = tracker.createClientResponse(replicaId);
        String targetNodeId = tracker.getClientId();
        var sent = messageBus.sendMessage(clientResponse, replicaId, targetNodeId);
        System.out.println("Replica " + replicaId + " sent GetResponse to client: " +
                        "messageId=" + clientResponse.getMessageId() + 
                        ", key=" + clientResponse.getKey() + 
                        ", value=" + clientResponse.getValue() + 
                        ", success=" + clientResponse.isSuccess());
    }
    
    @Override
    protected void checkTimeouts() {
        List<String> completedRequests = new ArrayList<>();
        
        for (Map.Entry<String, SimpleQuorumTracker> entry : pendingQuorum.entrySet()) {
            String requestId = entry.getKey();
            SimpleQuorumTracker tracker = entry.getValue();
            
            if (tracker.hasTimedOut(currentTick)) {
                Timeout timeout = tracker.getTimeout();
                LOGGER.warning("Replica " + replicaId + " detected timeout for request " + requestId + 
                             " at tick " + currentTick + " (started at tick " + timeout.getStartTick() + 
                             ", timeout after " + timeout.getTimeoutTicks() + " ticks)");
                
                handleTimeoutForTracker(requestId, tracker);
                completedRequests.add(requestId);
            }
        }
        
        // Remove completed requests
        for (String requestId : completedRequests) {
            pendingQuorum.remove(requestId);
            LOGGER.info("Replica " + replicaId + " removed timed-out request " + requestId);
        }
    }
    
    private void handleTimeoutForTracker(String requestId, SimpleQuorumTracker tracker) {
        // Determine the correct response type based on the operation in the tracker
        OperationType operationType = tracker.getOperationType();
        
        if (operationType == OperationType.PUT) {
            sendPutTimeoutResponse(tracker);
        } else {
            sendGetTimeoutResponse(tracker);
        }
        
        tracker.markCompleted();
    }
    
    private void sendPutTimeoutResponse(SimpleQuorumTracker tracker) {
        PutResponse failureResponse = new PutResponse(
            tracker.getClientMessageId(),
            tracker.getKey(),
            false,
            replicaId
        );
        String targetNodeId = tracker.getClientId();
        var sent = messageBus.sendMessage(failureResponse, replicaId, targetNodeId);
        System.out.println("Replica " + replicaId + " sent timeout PutResponse to client: " +
                         "messageId=" + failureResponse.getMessageId() + 
                         ", key=" + failureResponse.getKey() + 
                         ", success=false");
    }
    
    private void sendGetTimeoutResponse(SimpleQuorumTracker tracker) {
        GetResponse failureResponse = new GetResponse(
            tracker.getClientMessageId(),
            tracker.getKey(),
            "",
            false,
            replicaId
        );
        String targetNodeId = tracker.getClientId();
        var sent = messageBus.sendMessage(failureResponse, replicaId, targetNodeId);
        System.out.println("Replica " + replicaId + " sent timeout GetResponse to client: " +
                         "messageId=" + failureResponse.getMessageId() + 
                         ", key=" + failureResponse.getKey() + 
                         ", value='', success=false");
    }
    
    /**
     * A simple implementation of a quorum tracker for the SimpleReplica.
     */
    private class SimpleQuorumTracker {
        private final String requestId;
        private final String clientId;
        private final String clientMessageId; // Original message ID from client request
        private final String key;
        private final Timeout timeout;
        private final Map<String, ReplicaResponse> responses = new HashMap<>();
        private boolean completed = false;
        private final int quorumSize;
        private final OperationType operationType; // GET or PUT
        
        public SimpleQuorumTracker(String requestId, String clientId, String clientMessageId, String key, 
                             int quorumSize, OperationType operationType, long currentTick, long timeoutTicks) {
            if (requestId == null || requestId.isEmpty()) {
                throw new IllegalArgumentException("Request ID cannot be null or empty");
            }
            if (clientId == null || clientId.isEmpty()) {
                throw new IllegalArgumentException("Client ID cannot be null or empty");
            }
            if (clientMessageId == null || clientMessageId.isEmpty()) {
                throw new IllegalArgumentException("Client message ID cannot be null or empty");
            }
            if (operationType == null) {
                throw new IllegalArgumentException("Operation type cannot be null");
            }
            
            this.requestId = requestId;
            this.clientId = clientId;
            this.clientMessageId = clientMessageId;
            this.key = key;
            this.timeout = new Timeout(currentTick, timeoutTicks);
            this.quorumSize = quorumSize;
            this.operationType = operationType;
            
            System.out.println("Created SimpleQuorumTracker with requestId=" + requestId + 
                              ", clientMessageId=" + clientMessageId + 
                              ", operation=" + operationType +
                              ", timeout=" + timeout);
        }
        
        public boolean addResponse(ReplicaResponse response) {
            if (response == null) {
                throw new IllegalArgumentException("Response cannot be null");
            }
            
            if (!completed) {
                responses.put(response.getResponderReplicaId(), response);
                return true;
            }
            return false;
        }
        
        public boolean hasQuorum() {
            return responses.size() >= quorumSize;
        }
        
        public ReplicaResponse getFirstResponse() {
            if (responses.isEmpty()) {
                return null;
            }
            return responses.values().iterator().next();
        }
        
        public GetResponse createClientResponse(String originReplicaId) {
            if (operationType != OperationType.GET) {
                throw new IllegalStateException("createClientResponse is only valid for GET operations");
            }
            
            if (!hasQuorum()) {
                return null;
            }
            
            // 1. First, try to find a successful response
            Optional<ReplicaResponse> successfulResponse = responses.values().stream()
                .filter(ReplicaResponse::isSuccess)
                .findFirst();
            
            if (successfulResponse.isPresent()) {
                // We found a successful response with a value
                ReplicaResponse response = successfulResponse.get();
                System.out.println("SimpleQuorumTracker using successful response from " + 
                                  response.getResponderReplicaId() + 
                                  " with value: '" + response.getValue() + "'");
                
                return new GetResponse(
                    clientMessageId,
                    response.getKey(),
                    response.getValue(),
                    true,
                    originReplicaId
                );
            }
            
            // 2. If no successful response, use the first response
            ReplicaResponse firstResponse = responses.values().iterator().next();
            System.out.println("SimpleQuorumTracker using first response from " + 
                              firstResponse.getResponderReplicaId() + 
                              " (no successful responses)");
            
            return new GetResponse(
                clientMessageId,
                firstResponse.getKey(),
                firstResponse.getValue(),
                false,
                originReplicaId
            );
        }
        
        public void markCompleted() {
            this.completed = true;
        }
        
        public boolean hasTimedOut(long currentTick) {
            return timeout.hasExpired(currentTick);
        }
        
        public String getRequestId() {
            return requestId;
        }
        
        public String getClientId() {
            return clientId;
        }
        
        public String getClientMessageId() {
            return clientMessageId;
        }
        
        public String getKey() {
            return key;
        }
        
        public Timeout getTimeout() {
            return timeout;
        }
        
        public boolean isCompleted() {
            return completed;
        }
        
        public OperationType getOperationType() {
            return operationType;
        }
    }
} 