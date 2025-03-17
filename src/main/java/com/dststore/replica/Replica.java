package com.dststore.replica;

import com.dststore.message.*;
import com.dststore.network.MessageBus;
import com.dststore.network.SimulatedNetwork;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base abstract class for replica implementations.
 * Encapsulates common functionality from different replication strategies.
 */
public abstract class Replica {
    private static final Logger LOGGER = Logger.getLogger(Replica.class.getName());
    
    // Basic properties
    protected final String replicaId;
    protected final MessageBus messageBus;
    protected final Map<String, ReplicaEndpoint> peers;
    protected final int quorumSize;
    protected long currentTick = 0;
    protected final long requestTimeoutTicks;
    
    // Network information
    protected final String ipAddress;
    protected final int port;
    
    private static final long DEFAULT_REQUEST_TIMEOUT_TICKS = 10;
    
    /**
     * Creates a new Replica with the specified parameters.
     *
     * @param replicaId The ID of this replica
     * @param messageBus The message bus for communication
     * @param ipAddress The IP address of this replica
     * @param port The port of this replica
     * @param allReplicas The list of all replicas in the system
     * @param requestTimeoutTicks The number of ticks after which a request times out
     * @throws IllegalArgumentException if any parameter is invalid
     */
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
        
        // Register with message bus
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
    
    /**
     * Creates a new Replica with default timeout.
     *
     * @param replicaId The ID of this replica
     * @param messageBus The message bus for communication
     * @param ipAddress The IP address of this replica
     * @param port The port of this replica
     * @param allReplicas The list of all replicas in the system
     */
    public Replica(String replicaId, MessageBus messageBus, String ipAddress, int port, 
                  List<ReplicaEndpoint> allReplicas) {
        this(replicaId, messageBus, ipAddress, port, allReplicas, DEFAULT_REQUEST_TIMEOUT_TICKS);
    }
    
    /**
     * Creates a new Replica with default values.
     *
     * @param replicaId The ID of this replica
     * @param messageBus The message bus for communication
     */
    public Replica(String replicaId, MessageBus messageBus) {
        this(replicaId, messageBus, "localhost", 8000 + Integer.parseInt(replicaId.split("-")[1]), 
             List.of(new ReplicaEndpoint(replicaId, "localhost", 8000 + Integer.parseInt(replicaId.split("-")[1]))));
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
     * Gets the IP address of this replica.
     *
     * @return The IP address
     */
    public String getIpAddress() {
        return ipAddress;
    }
    
    /**
     * Gets the port of this replica.
     *
     * @return The port
     */
    public int getPort() {
        return port;
    }
    
    /**
     * Gets the current tick count.
     *
     * @return The current tick
     */
    public long getCurrentTick() {
        return currentTick;
    }
    
    /**
     * Gets the request timeout in ticks.
     *
     * @return The request timeout
     */
    public long getRequestTimeoutTicks() {
        return requestTimeoutTicks;
    }
    
    /**
     * Processes a tick, handling timeouts and message processing.
     */
    public void tick() {
        // Increment tick counter
        currentTick++;
        LOGGER.fine("Replica " + replicaId + " tick incremented to " + currentTick);
        
        // Check for timeouts
        checkTimeouts();
    }
    
    /**
     * Processes an incoming message.
     * This method is called by the message bus when a message is received.
     *
     * @param message The message to process
     * @param from The delivery context containing sender information
     */
    protected abstract void processMessage(Object message, SimulatedNetwork.DeliveryContext from);
    
    /**
     * Checks for timed-out operations.
     * This method should be implemented by subclasses to check for timeouts
     * specific to their operation tracking mechanism.
     */
    protected abstract void checkTimeouts();
    
    /**
     * Generates a unique request ID.
     *
     * @return A unique request ID
     */
    protected String generateRequestId() {
        return UUID.randomUUID().toString();
    }
    
    /**
     * Disconnects from a specific replica by dropping all messages sent to it.
     *
     * @param targetReplicaId The ID of the replica to disconnect from
     */
    public void disconnectFrom(String targetReplicaId) {
        if (!peers.containsKey(targetReplicaId)) {
            LOGGER.warning("Cannot disconnect from unknown replica: " + targetReplicaId);
            return;
        }
        
        // Use the NetworkSimulator to configure message dropping
        SimulatedNetwork simulator = getSimulatedNetwork();
        if (simulator != null) {
            simulator.disconnectNodesBidirectional(replicaId, targetReplicaId);
            LOGGER.info("Replica " + replicaId + " is now disconnected from " + targetReplicaId);
        } else {
            LOGGER.warning("Network simulation is not enabled, cannot disconnect nodes");
        }
    }
    
    /**
     * Restores the connection to a previously disconnected replica.
     *
     * @param targetReplicaId The ID of the replica to reconnect to
     */
    public void reconnectTo(String targetReplicaId) {
        if (!peers.containsKey(targetReplicaId)) {
            LOGGER.warning("Cannot reconnect to unknown replica: " + targetReplicaId);
            return;
        }
        
        SimulatedNetwork simulator = getSimulatedNetwork();
        if (simulator != null) {
            simulator.reconnectAll();
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
    protected SimulatedNetwork getSimulatedNetwork() {
        try {
            return messageBus.getSimulatedNetwork();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Could not get simulated network", e);
            return null;
        }
    }
    
    /**
     * Enum representing the type of operation.
     */
    protected enum OperationType {
        GET, PUT
    }
    
    /**
     * Class representing a timeout.
     */
    protected static class Timeout {
        private final long startTick;
        private final long timeoutTicks;
        private final long expiryTick;
        private boolean expired;
        
        public Timeout(long startTick, long timeoutTicks) {
            this.startTick = startTick;
            this.timeoutTicks = timeoutTicks;
            this.expiryTick = startTick + timeoutTicks;
            this.expired = false;
        }
        
        public boolean hasExpired(long currentTick) {
            if (!expired && currentTick >= expiryTick) {
                expired = true;
                return true;
            }
            return expired;
        }
        
        public long getStartTick() {
            return startTick;
        }
        
        public long getTimeoutTicks() {
            return timeoutTicks;
        }
        
        public long getExpiryTick() {
            return expiryTick;
        }
        
        public boolean isExpired() {
            return expired;
        }
        
        @Override
        public String toString() {
            return "Timeout{" +
                   "startTick=" + startTick +
                   ", timeoutTicks=" + timeoutTicks +
                   ", expiryTick=" + expiryTick +
                   ", expired=" + expired +
                   '}';
        }
    }
} 
