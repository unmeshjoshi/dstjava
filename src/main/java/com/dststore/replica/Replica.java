package com.dststore.replica;

import com.dststore.message.*;
import com.dststore.network.MessageBus;
import com.dststore.network.SimulatedNetwork;

import java.util.*;
import java.util.logging.Logger;

public class Replica {
    private final String replicaId;
    
    private final Map<String, ReplicaEndpoint> peers;
    
    private final Map<String, String> storage = new HashMap<>();
    
    private final Map<String, QuorumTracker> pendingQuorum = new HashMap<>();
    
    private final int quorumSize;
    
    private final MessageBus messageBus;
    
    private long currentTick = 0;
    
    private final long requestTimeoutTicks;
    
    private final String ipAddress;
    private final int port;
    
    private static final long DEFAULT_REQUEST_TIMEOUT_TICKS = 10;
    
    private static final Logger LOGGER = Logger.getLogger(Replica.class.getName());
    
    public Replica(String replicaId, MessageBus messageBus, String ipAddress, int port, 
                   List<ReplicaEndpoint> allReplicas, long requestTimeoutTicks) {
        // Validate inputs
        if (replicaId == null || replicaId.isEmpty()) {
            throw new IllegalArgumentException("Replica ID cannot be null or empty");
        }
        if (messageBus == null) {
            throw new IllegalArgumentException("Message bus cannot be null");
        }
        if (ipAddress == null || ipAddress.isEmpty()) {
            throw new IllegalArgumentException("IP address cannot be null or empty");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("Port must be a positive integer");
        }
        if (allReplicas == null || allReplicas.isEmpty()) {
            throw new IllegalArgumentException("Replica list cannot be null or empty");
        }
        if (requestTimeoutTicks <= 0) {
            throw new IllegalArgumentException("Request timeout ticks must be positive");
        }
        
        this.replicaId = replicaId;
        this.messageBus = messageBus;
        this.ipAddress = ipAddress;
        this.port = port;
        this.requestTimeoutTicks = requestTimeoutTicks;
        
        messageBus.registerNode(replicaId, this::processMessage, MessageBus.NodeType.REPLICA);
        
        // Add all replicas to peers map (including self for simplicity)
        this.peers = new HashMap<>();
        for (ReplicaEndpoint endpoint : allReplicas) {
            peers.put(endpoint.getReplicaId(), endpoint);
        }
        
        // Calculate quorum size (majority)
        this.quorumSize = (allReplicas.size() / 2) + 1;
        LOGGER.info("Replica " + replicaId + " created with quorum size " + quorumSize + 
                    " from " + allReplicas.size() + " replicas" +
                    ", timeout: " + requestTimeoutTicks + " ticks");
    }
    
    public Replica(String replicaId, MessageBus messageBus, String ipAddress, int port, 
                   List<ReplicaEndpoint> allReplicas) {
        this(replicaId, messageBus, ipAddress, port, allReplicas, DEFAULT_REQUEST_TIMEOUT_TICKS);
    }
    
    public Replica(String replicaId, MessageBus messageBus) {
        this(replicaId, messageBus, "localhost", 8000 + Integer.parseInt(replicaId.split("-")[1]), 
             List.of(new ReplicaEndpoint(replicaId, "localhost", 8000 + Integer.parseInt(replicaId.split("-")[1]))));
    }
    
    public String getReplicaId() {
        return replicaId;
    }
    
    public String getIpAddress() {
        return ipAddress;
    }
    
    public int getPort() {
        return port;
    }
    
    public long getCurrentTick() {
        return currentTick;
    }
    
    public long getRequestTimeoutTicks() {
        return requestTimeoutTicks;
    }
    
    public void setValue(String key, String value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        storage.put(key, value);
        System.out.println("Replica " + replicaId + " set value " + key + "=" + value);
    }
    
    public void tick() {
        // Increment tick counter
        currentTick++;
        LOGGER.fine("Replica " + replicaId + " tick incremented to " + currentTick);
        
        // Check for timeouts
        checkTimeouts();
    }
    
    private void processMessage(Object message, SimulatedNetwork.DeliveryContext from) {
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
        String requestId = UUID.randomUUID().toString();
        System.out.println("Replica " + replicaId + " generated requestId " + requestId + 
                          " for client " + operationType + " request " + clientMessageId + 
                          " at tick " + currentTick);
        
        // Create a new quorum tracker
        QuorumTracker tracker = new QuorumTracker(requestId, clientId, clientMessageId, key, quorumSize, 
                                                 operationType, currentTick, requestTimeoutTicks);
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
        QuorumTracker tracker = pendingQuorum.get(response.getRequestId());
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
        QuorumTracker tracker = pendingQuorum.get(requestId);
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
    
    private void sendPutResponse(QuorumTracker tracker) {
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
    
    private void sendGetResponse(QuorumTracker tracker) {
        GetResponse clientResponse = tracker.createClientResponse(replicaId);
        String targetNodeId = tracker.getClientId();
        var sent = messageBus.sendMessage(clientResponse, replicaId, targetNodeId);
        System.out.println("Replica " + replicaId + " sent GetResponse to client: " +
                        "messageId=" + clientResponse.getMessageId() + 
                        ", key=" + clientResponse.getKey() + 
                        ", value=" + clientResponse.getValue() + 
                        ", success=" + clientResponse.isSuccess());
    }
    
    private void checkTimeouts() {
        List<String> completedRequests = new ArrayList<>();
        
        for (Map.Entry<String, QuorumTracker> entry : pendingQuorum.entrySet()) {
            String requestId = entry.getKey();
            QuorumTracker tracker = entry.getValue();
            
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
    
    private void handleTimeoutForTracker(String requestId, QuorumTracker tracker) {
        // Determine the correct response type based on the operation in the tracker
        OperationType operationType = tracker.getOperationType();
        
        if (operationType == OperationType.PUT) {
            sendPutTimeoutResponse(tracker);
        } else {
            sendGetTimeoutResponse(tracker);
        }
        
        tracker.markCompleted();
    }
    
    private void sendPutTimeoutResponse(QuorumTracker tracker) {
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
    
    private void sendGetTimeoutResponse(QuorumTracker tracker) {
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
} 
