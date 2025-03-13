package com.dststore.testing;

import com.dststore.network.MessageBus;
import com.dststore.network.NetworkSimulator;
import com.dststore.network.NetworkSimulator.DelayedMessage;
import com.dststore.replica.Replica;
import com.dststore.client.Client;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides TigerBeetle-style deterministic testing capabilities for distributed systems.
 * This class manages the execution of tick-based simulation across all system components.
 */
public class SimulationRunner {
    private static final Logger LOGGER = Logger.getLogger(SimulationRunner.class.getName());
    
    private final Map<String, Replica> replicas;
    private final Map<String, MessageBus> messageBuses;
    private final NetworkSimulator networkSimulator;
    private final Random random;
    private final Set<String> stoppedReplicas = new HashSet<>();
    private long currentTick = 0;
    private Client client;
    
    /**
     * Create a new simulation runner with the given components.
     */
    public SimulationRunner(Map<String, Replica> replicas, MessageBus messageBus) {
        this.replicas = new HashMap<>(replicas);
        this.messageBuses = new HashMap<>();
        
        // Enable network simulation on the message bus
        this.networkSimulator = messageBus.enableNetworkSimulation();
        
        // Use seeded random for deterministic behavior
        this.random = new Random(42);
        
        // Use the same message bus for all replicas
        for (Map.Entry<String, Replica> entry : replicas.entrySet()) {
            messageBuses.put(entry.getKey(), messageBus);
        }
        
        LOGGER.info("Simulation runner initialized with " + replicas.size() + " replicas");
    }
    
    /**
     * Set the client to be included in the simulation.
     * @param client The client instance
     * @return This SimulationRunner instance for method chaining
     */
    public SimulationRunner withClient(Client client) {
        this.client = client;
        LOGGER.info("Added client " + client.getClientId() + " to simulation");
        return this;
    }
    
    /**
     * Run the simulation for a specific number of ticks.
     */
    public long runFor(int ticks) {
        for (int i = 0; i < ticks; i++) {
            tick();
        }
        return currentTick;
    }
    
    /**
     * Process a single tick of the simulation.
     */
    public long tick() {
        currentTick++;
        LOGGER.log(Level.FINE, "Processing simulation tick " + currentTick);
        
        // First process the message bus to handle messages
        MessageBus messageBus = messageBuses.values().iterator().next();
        messageBus.tick();
        
        // Then process all active replicas to handle those messages
        for (Map.Entry<String, Replica> entry : replicas.entrySet()) {
            String replicaId = entry.getKey();
            Replica replica = entry.getValue();
            
            if (!stoppedReplicas.contains(replicaId)) {
                replica.tick();
            }
        }
        
        // Process the client to handle responses and complete futures
        if (client != null) {
            client.tick();
            LOGGER.log(Level.FINEST, "Processed client " + client.getClientId() + " at tick " + currentTick);
        }
        
        return currentTick;
    }
    
    /**
     * Run the simulation until a condition is met or max ticks is reached.
     */
    public boolean runUntil(SimulationCondition condition, int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            tick();
            
            if (condition.isMet()) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Create a network partition between sets of nodes.
     */
    public int[] createNetworkPartition(String[] partition1, String[] partition2) {
        int p1 = networkSimulator.createPartition(partition1);
        int p2 = networkSimulator.createPartition(partition2);
        
        LOGGER.info("Created network partition between " + 
                   Arrays.toString(partition1) + " and " + Arrays.toString(partition2));
        
        return new int[] { p1, p2 };
    }
    
    /**
     * Heal a network partition by linking the partitions.
     */
    public void healNetworkPartition(int partition1Id, int partition2Id) {
        networkSimulator.linkPartitions(partition1Id, partition2Id);
        networkSimulator.linkPartitions(partition2Id, partition1Id);
        
        LOGGER.info("Healed network partition between partition " + 
                   partition1Id + " and partition " + partition2Id);
    }
    
    /**
     * Check if a replica is running.
     */
    public boolean isReplicaRunning(String replicaId) {
        return !stoppedReplicas.contains(replicaId);
    }
    
    /**
     * Stop a replica to simulate a crash.
     */
    public void crashReplica(String replicaId) {
        Replica replica = replicas.get(replicaId);
        if (replica != null) {
            stoppedReplicas.add(replicaId);
            LOGGER.info("Crashed replica: " + replicaId);
        } else {
            LOGGER.warning("Unknown replica ID: " + replicaId);
        }
    }
    
    /**
     * Restart a previously crashed replica.
     */
    public void restartReplica(String replicaId) {
        Replica replica = replicas.get(replicaId);
        if (replica != null) {
            stoppedReplicas.remove(replicaId);
            LOGGER.info("Restarted replica: " + replicaId);
        } else {
            LOGGER.warning("Unknown replica ID: " + replicaId);
        }
    }
    
    /**
     * Configure message loss rate for the network.
     */
    public void setMessageLossRate(double rate) {
        networkSimulator.withMessageLossRate(rate);
        LOGGER.info("Set message loss rate to " + rate);
    }
    
    /**
     * Configure message latency for the network.
     */
    public void setNetworkLatency(int minTicks, int maxTicks) {
        networkSimulator.withLatency(minTicks, maxTicks);
        LOGGER.info("Set network latency to " + minTicks + "-" + maxTicks + " ticks");
    }
    
    /**
     * Get the current simulation tick.
     */
    public long getCurrentTick() {
        return currentTick;
    }
    
    /**
     * Reset the simulation to initial state.
     */
    public void reset() {
        currentTick = 0;
        networkSimulator.reset();
        stoppedReplicas.clear();
        
        LOGGER.info("Simulation reset to initial state");
    }
    
    /**
     * Interface for defining simulation conditions to check.
     */
    @FunctionalInterface
    public interface SimulationCondition {
        boolean isMet();
    }
} 