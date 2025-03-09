package com.dststore.simulation;

import com.dststore.replica.Replica;
import com.dststore.network.MessageBus;
import com.dststore.network.PacketSimulator;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulated environment for end-to-end testing of the replicated key-value store.
 */
public class SimulatedEnvironment {
    private final List<Replica> replicas = new ArrayList<>();
    private final PacketSimulator packetSimulator;
    private static final Logger logger = LoggerFactory.getLogger(SimulatedEnvironment.class);

    /**
     * Creates a new simulated environment with the specified packet simulator.
     *
     * @param packetSimulator The packet simulator for controlling network conditions
     */
    public SimulatedEnvironment(PacketSimulator packetSimulator) {
        this.packetSimulator = packetSimulator;
    }

    /**
     * Adds a replica to the simulated environment.
     *
     * @param replica The replica to add
     */
    public void addReplica(Replica replica) {
        replicas.add(replica);
    }

    /**
     * Runs the simulation for the specified number of ticks.
     *
     * @param ticks The number of ticks to run the simulation
     */
    public void runSimulation(int ticks) {
        for (int i = 0; i < ticks; i++) {
            packetSimulator.tick();
            for (Replica replica : replicas) {
                replica.tick();
            }
        }
    }

    /**
     * Simulates a network partition between replicas by disconnecting them.
     *
     * @param replica1 The first replica in the partition
     * @param replica2 The second replica in the partition
     */
    public void partitionNetwork(Replica replica1, Replica replica2) {
        packetSimulator.partitionNodes(replica1.getReplicaId(), replica2.getReplicaId());
        logger.info("Partitioned network between replicas {} and {}", replica1.getReplicaId(), replica2.getReplicaId());
    }

    /**
     * Heals a network partition between replicas by reconnecting them.
     *
     * @param replica1 The first replica in the partition
     * @param replica2 The second replica in the partition
     */
    public void healPartition(Replica replica1, Replica replica2) {
        packetSimulator.healPartition(replica1.getReplicaId(), replica2.getReplicaId());
        logger.info("Healed partition between replicas {} and {}", replica1.getReplicaId(), replica2.getReplicaId());
    }

    /**
     * Sends a request to all replicas and waits for a quorum of responses.
     *
     * @param request The request to send
     * @return The response from the quorum
     */
    public Object sendRequestToReplicas(Object request) {
        // Implement logic to send the request to all replicas and wait for a quorum of responses
        return null; // Placeholder
    }

    /**
     * Simulates a failure in a replica.
     *
     * @param replica The replica to fail
     */
    public void failReplica(Replica replica) {
        replica.stop();
        logger.info("Replica {} has been failed", replica.getReplicaId());
    }

    /**
     * Recovers a failed replica.
     *
     * @param replica The replica to recover
     */
    public void recoverReplica(Replica replica) {
        replica.start();
        logger.info("Replica {} has been recovered", replica.getReplicaId());
    }
} 