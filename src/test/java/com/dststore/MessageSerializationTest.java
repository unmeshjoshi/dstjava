package com.dststore;

import com.dststore.client.Client;
import com.dststore.message.GetResponse;
import com.dststore.network.MessageBus;
import com.dststore.replica.Replica;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

public class MessageSerializationTest {
    
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
    
    @Test
    public void testMessageSerialization() throws InterruptedException, ExecutionException, TimeoutException {
        // Send get request
        CompletableFuture<GetResponse> future = client.getValue("key1", "replica-1");
        
        // Tick the message bus to move the request to the replica queue
        messageBus.tick();
        
        // Check message stats - should have 1 serialized GetRequest
        Map<String, Integer> stats = messageBus.getMessageStats();
        assertTrue(stats.containsKey("com.dststore.message.GetRequest"));
        
        // Process the request on the replica
        replica.tick();
        
        // Tick the message bus to move the response to the client queue
        messageBus.tick();
        
        // Check message stats again - should now also have a serialized GetResponse
        stats = messageBus.getMessageStats();
        assertTrue(stats.containsKey("com.dststore.message.GetResponse"));
        
        // Process the response on the client
        client.tick();
        
        // Verify the response was correctly deserialized and processed
        GetResponse response = future.get(1, TimeUnit.SECONDS);
        assertNotNull(response);
        assertEquals("key1", response.getKey());
        assertEquals("value1", response.getValue());
        assertTrue(response.isSuccess());
    }
} 