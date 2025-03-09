package com.dststore.simulation;

import com.dststore.network.PacketSimulator;
import com.dststore.replica.Replica;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SimulatedEnvironmentTest {
    private SimulatedEnvironment environment;
    private PacketSimulator packetSimulator;
    private Replica replica1;
    private Replica replica2;

    @BeforeEach
    void setUp() {
        packetSimulator = mock(PacketSimulator.class);
        environment = new SimulatedEnvironment(packetSimulator);
        replica1 = mock(Replica.class);
        replica2 = mock(Replica.class);
        when(replica1.getReplicaId()).thenReturn("replica-1");
        when(replica2.getReplicaId()).thenReturn("replica-2");
        environment.addReplica(replica1);
        environment.addReplica(replica2);
    }

    @Test
    void testNetworkPartitioning() {
        environment.partitionNetwork(replica1, replica2);
        verify(packetSimulator).partitionNodes("replica-1", "replica-2");

        environment.healPartition(replica1, replica2);
        verify(packetSimulator).healPartition("replica-1", "replica-2");
    }

    @Test
    void testFailureManagement() {
        environment.failReplica(replica1);
        verify(replica1).stop();

        environment.recoverReplica(replica1);
        verify(replica1).start();
    }

    // Additional tests for client interface and other functionalities can be added here
} 