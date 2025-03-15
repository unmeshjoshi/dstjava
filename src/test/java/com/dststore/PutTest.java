package com.dststore;

import com.dststore.client.Client;
import com.dststore.message.GetResponse;
import com.dststore.message.PutRequest;
import com.dststore.message.PutResponse;
import com.dststore.network.MessageBus;
import com.dststore.replica.Replica;
import com.dststore.replica.ReplicaEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

public class PutTest {
    
    private MessageBus messageBus;
    private List<Replica> replicas;
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
            
            Replica replica = new Replica(replicaId, messageBus, ipAddress, port, allEndpoints);
            replicas.add(replica);
        }
        
        // Create client
        client = new Client("client-1", messageBus);
        
        System.out.println("Setup complete with " + REPLICA_COUNT + " replicas");
    }
    
    @Test
    public void testQuorumPut() throws InterruptedException, ExecutionException, TimeoutException {
        // Create PUT request
        String messageId = UUID.randomUUID().toString();
        String key = "testKey";
        String value = "testValue";
        PutRequest putRequest = new PutRequest(messageId, key, value, client.getClientId());
        
        // Set up a CompletableFuture to track completion
        CompletableFuture<PutResponse> putFuture = new CompletableFuture<>();
        client.getPendingRequests().put(messageId, putFuture);
        
        // Send to first replica
        System.out.println("Starting testQuorumPut test");
        messageBus.send(replicas.get(0).getReplicaId(), client.getClientId(), putRequest);
        System.out.println("Client sent PutRequest for key '" + key + "' with value '" + value + "' to replica " + replicas.get(0).getReplicaId());
        
        // Run simulation until put completes
        runSimulationUntilComplete(putFuture);
        
        // Verify put response
        PutResponse putResponse = putFuture.get(1, TimeUnit.SECONDS);
        assertNotNull(putResponse);
        assertEquals(key, putResponse.getKey());
        assertTrue(putResponse.isSuccess());
        
        // Now verify the value was stored by doing a GET
        CompletableFuture<GetResponse> getFuture = client.getValue(key, replicas.get(0).getReplicaId());
        
        // Run simulation until get completes
        runSimulationUntilComplete(getFuture);
        
        // Verify get response
        GetResponse getResponse = getFuture.get(1, TimeUnit.SECONDS);
        assertNotNull(getResponse);
        assertEquals(key, getResponse.getKey());
        assertEquals(value, getResponse.getValue());
        assertTrue(getResponse.isSuccess());
    }
    
    private void runSimulationUntilComplete(CompletableFuture<?> future) {
        int maxTicks = 40; // Increase ticks for distributed operation
        int tickCount = 0;
        
        while (!future.isDone() && tickCount < maxTicks) {
            // Process message bus
            messageBus.tick();
            
            // Process all replicas
            for (Replica replica : replicas) {
                replica.tick();
            }
            
            // Process client
            client.tick();
            
            if (tickCount % 5 == 0) {
                System.out.println("Tick " + tickCount + ": Future completed = " + future.isDone());
            }
            
            tickCount++;
        }
        
        if (tickCount >= maxTicks && !future.isDone()) {
            System.out.println("TEST FAILED: Maximum tick count reached without completing the future");
            fail("Simulation reached maximum tick count without completing the future");
        } else {
            System.out.println("Simulation completed in " + tickCount + " ticks");
        }
    }
} 