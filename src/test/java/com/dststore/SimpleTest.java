package com.dststore;

import com.dststore.client.Client;
import com.dststore.message.GetResponse;
import com.dststore.network.MessageBus;
import com.dststore.replica.Replica;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

public class SimpleTest {
    
    private MessageBus messageBus;
    private Replica replica;
    private Client client;
    
    @BeforeEach
    public void setUp() {
        // Create message bus with network simulation
        messageBus = new MessageBus();
        
        // Create replica and set initial value
        replica = new Replica("replica-1", messageBus);
        replica.setValue("key1", "value1");
        
        // Create client
        client = new Client("client-1", messageBus);
    }
    
    @AfterEach
    public void tearDown() {
        // Nothing to clean up yet
    }
    
    @Test
    public void testGetExistingKey() throws InterruptedException, ExecutionException, TimeoutException {
        // Send get request for existing key
        CompletableFuture<GetResponse> future = client.getValue("key1", "replica-1");
        
        // Run simulation until the future completes
        runSimulationUntilComplete(messageBus, replica, client, future);
        
        // Verify response
        GetResponse response = future.get(1, TimeUnit.SECONDS);
        assertNotNull(response);
        assertEquals("key1", response.getKey());
        assertEquals("value1", response.getValue());
        assertTrue(response.isSuccess());
        assertEquals("replica-1", response.getReplicaId());
    }
    
    @Test
    public void testGetNonExistentKey() throws InterruptedException, ExecutionException, TimeoutException {
        // Try to get a non-existent key
        CompletableFuture<GetResponse> future = client.getValue("key2", "replica-1");
        
        // Run simulation until the future completes
        runSimulationUntilComplete(messageBus, replica, client, future);
        
        // Verify response
        GetResponse response = future.get(1, TimeUnit.SECONDS);
        assertNotNull(response);
        assertEquals("key2", response.getKey());
        assertEquals("", response.getValue());
        assertFalse(response.isSuccess());
        assertEquals("replica-1", response.getReplicaId());
    }
    
    private void runSimulationUntilComplete(
            MessageBus messageBus, 
            Replica replica, 
            Client client, 
            CompletableFuture<GetResponse> future) {
        
        int maxTicks = 10; // Safeguard against infinite loops
        int tickCount = 0;
        
        while (!future.isDone() && tickCount < maxTicks) {
            // Process messages
            messageBus.tick();
            
            // Process replica
            replica.tick();
            
            // Process client
            client.tick();
            
            tickCount++;
        }
        
        if (tickCount >= maxTicks && !future.isDone()) {
            fail("Simulation reached maximum tick count without completing the future");
        }
    }
} 