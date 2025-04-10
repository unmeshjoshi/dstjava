package com.dststore.examples;

import com.dststore.network.MessageBus;
import com.dststore.network.SimulatedNetwork;
import com.dststore.replica.SimpleReplica;
import com.dststore.replica.ReplicaEndpoint;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Example demonstrating the use of the network simulator.
 */
public class NetworkSimulatorExample {
    private static final Logger LOGGER = Logger.getLogger(NetworkSimulatorExample.class.getName());
    
    /**
     * Demonstrates a network simulation with various features.
     */
    public static void main(String[] args) {
        // Create a message bus with network simulation
        MessageBus messageBus = new MessageBus();
        SimulatedNetwork simulator = messageBus.getNetwork();

        // Create three replica endpoints
        List<ReplicaEndpoint> replicaEndpoints = Arrays.asList(
            new ReplicaEndpoint("replica-1", "localhost", 8001),
            new ReplicaEndpoint("replica-2", "localhost", 8002),
            new ReplicaEndpoint("replica-3", "localhost", 8003)
        );
        
        // Create three replicas
        SimpleReplica replica1 = new SimpleReplica("replica-1", messageBus, "localhost", 8001, replicaEndpoints, 10);
        SimpleReplica replica2 = new SimpleReplica("replica-2", messageBus, "localhost", 8002, replicaEndpoints, 10);
        SimpleReplica replica3 = new SimpleReplica("replica-3", messageBus, "localhost", 8003, replicaEndpoints, 10);
        
        // Configure network conditions
        LOGGER.info("Configuring network conditions");
        
        // Add some latency (1-3 ticks)
        simulator.withLatency(1, 3);
        LOGGER.info("Set latency to 1-3 ticks");
        
        // Add some message loss (5%)
        simulator.withMessageLossRate(0.05);
        LOGGER.info("Set message loss rate to 5%");
        
        // Create a network partition
        LOGGER.info("Creating network partition...");
        simulator.disconnectNodesBidirectional("replica-1", "replica-3");
        simulator.disconnectNodesBidirectional("replica-2", "replica-3");
        
        // Run for a few ticks to let messages propagate
        for (int i = 0; i < 5; i++) {
            simulator.tick();
        }
        
        // Heal the partition
        LOGGER.info("Healing network partition...");
        simulator.reconnectAll();
        
        // Run for a few more ticks
        for (int i = 0; i < 5; i++) {
            simulator.tick();
        }
        
        // Print final statistics
        Map<String, Object> stats = simulator.getStatistics();
        LOGGER.info("Final network statistics: " + stats);
    }
} 