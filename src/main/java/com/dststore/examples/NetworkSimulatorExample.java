package com.dststore.examples;

import com.dststore.network.MessageBus;
import com.dststore.network.SimulatedNetwork;
import com.dststore.replica.Replica;
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
        SimulatedNetwork simulator = new SimulatedNetwork();
        MessageBus messageBus = new MessageBus(simulator);
        
        // Create three replica endpoints
        List<ReplicaEndpoint> replicaEndpoints = Arrays.asList(
            new ReplicaEndpoint("replica-1", "localhost", 8001),
            new ReplicaEndpoint("replica-2", "localhost", 8002),
            new ReplicaEndpoint("replica-3", "localhost", 8003)
        );
        
        // Create three replicas
        Replica replica1 = new Replica("replica-1", messageBus, "localhost", 8001, replicaEndpoints, 10);
        Replica replica2 = new Replica("replica-2", messageBus, "localhost", 8002, replicaEndpoints, 10);
        Replica replica3 = new Replica("replica-3", messageBus, "localhost", 8003, replicaEndpoints, 10);
        
        // Configure network conditions
        LOGGER.info("Configuring network conditions");
        
        // Add some latency (1-3 ticks)
        simulator.withLatency(1, 3);
        LOGGER.info("Set latency to 1-3 ticks");
        
        // Add some message loss (5%)
        simulator.withMessageLossRate(0.05);
        LOGGER.info("Set message loss rate to 5%");
        
        // Create a network partition
        LOGGER.info("Creating network partition: replica1 and replica2 in partition1, replica3 in partition2");
        int partition1 = simulator.createPartition("replica-1", "replica-2");
        int partition2 = simulator.createPartition("replica-3");
        
        // Run the simulation for a few ticks
        LOGGER.info("Running simulation with partitioned network");
        for (int i = 0; i < 10; i++) {
            LOGGER.info("Tick " + i);
            messageBus.tick();
            replica1.tick();
            replica2.tick();
            replica3.tick();
        }
        
        // Link the partitions to restore communication
        LOGGER.info("Restoring connection between partitions");
        simulator.linkPartitions(partition1, partition2);
        simulator.linkPartitions(partition2, partition1);
        
        // Run the simulation for a few more ticks
        LOGGER.info("Running simulation with restored network");
        for (int i = 0; i < 10; i++) {
            LOGGER.info("Tick " + (i + 10));
            messageBus.tick();
            replica1.tick();
            replica2.tick();
            replica3.tick();
        }
        
        // Print network statistics
        Map<String, Object> stats = simulator.getStatistics();
        LOGGER.info("Simulation complete. Statistics:");
        for (Map.Entry<String, Object> entry : stats.entrySet()) {
            LOGGER.info(entry.getKey() + ": " + entry.getValue());
        }
    }
} 