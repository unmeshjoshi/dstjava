package com.dststore;

import com.dststore.client.Client;
import com.dststore.message.GetResponse;
import com.dststore.network.MessageBus;
import com.dststore.replica.SimpleReplica;
import com.dststore.replica.ReplicaEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

public class QuorumTest {
    
    private MessageBus messageBus;
    private List<SimpleReplica> replicas;
    private Client client;
    private static final int REPLICA_COUNT = 3; // Using 3 replicas for testing
    
    @BeforeEach
    public void setUp() { 
        // Create message bus with network simulation
        messageBus = new MessageBus();
        
        // Create replicas with network information
        replicas = new ArrayList<>();
        List<ReplicaEndpoint> allEndpoints = new ArrayList<>();
        
        // First, create all endpoints
        for (int i = 1; i <= REPLICA_COUNT; i++) {
            String replicaId = "replica-" + i;
            String ipAddress = "localhost";
            int port = 8000 + i;
            
            allEndpoints.add(new ReplicaEndpoint(replicaId, ipAddress, port));
        }
        
        // Then create all replicas with knowledge of all endpoints
        for (int i = 1; i <= REPLICA_COUNT; i++) {
            String replicaId = "replica-" + i;
            String ipAddress = "localhost";
            int port = 8000 + i;
            
            SimpleReplica replica = new SimpleReplica(replicaId, messageBus, ipAddress, port, allEndpoints);
            replicas.add(replica);
            
            // Set initial value in the first replica only
            if (i == 1) {
                replica.setValue("key1", "value1");
            }
        }
        
        // Create client
        client = new Client("client-1", messageBus);
        
        System.out.println("Setup complete with " + REPLICA_COUNT + " replicas");
    }
    
    @Test
    public void testQuorumGet() throws InterruptedException, ExecutionException, TimeoutException {
        // Send get request to the first replica
        System.out.println("Starting testQuorumGet test");
        CompletableFuture<GetResponse> future = client.getValue("key1", replicas.get(0).getReplicaId());
        
        // Run simulation until future completes
        runSimulationUntilComplete(future);
        
        // Verify response
        GetResponse response = future.get(1, TimeUnit.SECONDS);
        assertNotNull(response);
        assertEquals("key1", response.getKey());
        assertEquals("value1", response.getValue());
        assertTrue(response.isSuccess());
    }
    
    @Test
    public void testQuorumGetNonexistentKey() throws InterruptedException, ExecutionException, TimeoutException {
        // Send get request for a non-existent key
        System.out.println("Starting testQuorumGetNonexistentKey test");
        CompletableFuture<GetResponse> future = client.getValue("nonexistent", replicas.get(0).getReplicaId());
        
        // Run simulation until future completes
        runSimulationUntilComplete(future);
        
        // Verify response
        GetResponse response = future.get(1, TimeUnit.SECONDS);
        assertNotNull(response);
        assertEquals("nonexistent", response.getKey());
        assertEquals("", response.getValue());
        assertFalse(response.isSuccess());
    }
    
    private void runSimulationUntilComplete(CompletableFuture<GetResponse> future) {
        int maxTicks = 20; // Safeguard against infinite loops
        int tickCount = 0;
        
        while (!future.isDone() && tickCount < maxTicks) {
            // Process messages
            messageBus.tick();
            
            // Process all replicas
            for (SimpleReplica replica : replicas) {
                replica.tick();
            }
            
            tickCount++;
        }
        
        if (tickCount >= maxTicks && !future.isDone()) {
            fail("Simulation reached maximum tick count without completing the future");
        }
    }
} 