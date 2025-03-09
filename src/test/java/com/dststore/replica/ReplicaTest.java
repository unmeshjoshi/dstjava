package com.dststore.replica;

import com.dststore.message.GetRequest;
import com.dststore.message.GetResponse;
import com.dststore.message.Message;
import com.dststore.message.MessageType;
import com.dststore.message.SetRequest;
import com.dststore.message.SetResponse;
import com.dststore.network.MessageBus;
import com.dststore.network.PacketSimulator;
import com.dststore.storage.TimestampedValue;
import com.dststore.metrics.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the Replica class.
 */
public class ReplicaTest {
    
    @TempDir
    Path tempDir;
    
    private PacketSimulator packetSimulator;
    private MessageBus clientMessageBus;
    private MessageBus replica1MessageBus;
    private Replica replica1;
    private Metrics metrics;
    
    @BeforeEach
    void setUp() {
        metrics = new Metrics();
        // Create the packet simulator (deterministic network)
        packetSimulator = new PacketSimulator();
        
        // Create message buses for client and replicas
        clientMessageBus = new MessageBus("client", packetSimulator);
        replica1MessageBus = new MessageBus("replica-1", packetSimulator);
        
        // Create a replica with its own message bus
        replica1 = new Replica("replica-1", replica1MessageBus, metrics);
        
        // Connect the client to the replica
        clientMessageBus.connect("replica-1");
        replica1MessageBus.connect("client");
        
        // Start the replica
        replica1.start();
    }
    
    @Test
    void testReplicaCreation() {
        // Assert
        assertThat(replica1.getReplicaId()).isEqualTo("replica-1");
        assertThat(replica1.getStore()).isNotNull();
        
        Map<String, Long> stats = replica1.getStats();
        assertThat(stats.get("totalRequests")).isEqualTo(0);
        assertThat(stats.get("successfulRequests")).isEqualTo(0);
        assertThat(stats.get("failedRequests")).isEqualTo(0);
    }
    
    @Test
    void testSetRequestHandling() {
        // Arrange - create a reference to store the response
        AtomicReference<SetResponse> responseRef = new AtomicReference<>();
        
        // Register a handler for SET_RESPONSE messages
        clientMessageBus.registerHandler(new MessageBus.MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.SET_RESPONSE;
            }
            
            @Override
            public void handleMessage(Message message) {
                if (message instanceof SetResponse) {
                    responseRef.set((SetResponse) message);
                }
            }
        });
        
        // Act - send a SET request from client to replica
        SetRequest request = new SetRequest("client", "replica-1", "test-key", "test-value");
        clientMessageBus.queueMessage(request);
        
        // Run a few ticks to process the messages
        for (int i = 0; i < 3; i++) {
            clientMessageBus.tick();
            replica1.tick();
            packetSimulator.tick();
        }
        
        // Assert - verify the replica processed the request and sent a response
        SetResponse response = responseRef.get();
        assertThat(response).isNotNull();
        assertThat(response.getKey()).isEqualTo("test-key");
        assertThat(response.isSuccessful()).isTrue();
        
        // Verify the key was stored in the replica's store
        assertThat(replica1.getStore().get("test-key")).isEqualTo("test-value");
        
        // Verify the statistics were updated
        Map<String, Long> stats = replica1.getStats();
        assertThat(stats.get("totalRequests")).isEqualTo(1);
        assertThat(stats.get("successfulRequests")).isEqualTo(1);
        assertThat(stats.get("failedRequests")).isEqualTo(0);
        assertThat(stats.get("messages.SET_REQUEST")).isEqualTo(1);
    }
    
    @Test
    void testGetRequestHandling() {
        // Arrange - pre-populate the store with some data
        replica1.getStore().set("existing-key", "existing-value");
        
        // Create a reference to store the response
        AtomicReference<GetResponse> responseRef = new AtomicReference<>();
        
        // Register a handler for GET_RESPONSE messages
        clientMessageBus.registerHandler(new MessageBus.MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_RESPONSE;
            }
            
            @Override
            public void handleMessage(Message message) {
                if (message instanceof GetResponse) {
                    responseRef.set((GetResponse) message);
                }
            }
        });
        
        // Act - send a GET request from client to replica
        GetRequest request = new GetRequest("client", "replica-1", "existing-key");
        clientMessageBus.queueMessage(request);
        
        // Run a few ticks to process the messages
        for (int i = 0; i < 3; i++) {
            clientMessageBus.tick();
            replica1.tick();
            packetSimulator.tick();
        }
        
        // Assert - verify the replica processed the request and sent a response
        GetResponse response = responseRef.get();
        assertThat(response).isNotNull();
        assertThat(response.getKey()).isEqualTo("existing-key");
        assertThat(response.getValue()).isEqualTo("existing-value");
        assertThat(response.isSuccessful()).isTrue();
        
        // Verify the statistics were updated
        Map<String, Long> stats = replica1.getStats();
        assertThat(stats.get("totalRequests")).isEqualTo(1);
        assertThat(stats.get("successfulRequests")).isEqualTo(1);
        assertThat(stats.get("failedRequests")).isEqualTo(0);
        assertThat(stats.get("messages.GET_REQUEST")).isEqualTo(1);
    }
    
    @Test
    void testGetRequestForNonExistentKey() {
        // Arrange - create a reference to store the response
        AtomicReference<GetResponse> responseRef = new AtomicReference<>();
        
        // Register a handler for GET_RESPONSE messages
        clientMessageBus.registerHandler(new MessageBus.MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_RESPONSE;
            }
            
            @Override
            public void handleMessage(Message message) {
                if (message instanceof GetResponse) {
                    responseRef.set((GetResponse) message);
                }
            }
        });
        
        // Act - send a GET request for a non-existent key
        GetRequest request = new GetRequest("client", "replica-1", "non-existent-key");
        clientMessageBus.queueMessage(request);
        
        // Run a few ticks to process the messages
        for (int i = 0; i < 3; i++) {
            clientMessageBus.tick();
            replica1.tick();
            packetSimulator.tick();
        }
        
        // Assert - verify the replica processed the request and sent an error response
        GetResponse response = responseRef.get();
        assertThat(response).isNotNull();
        assertThat(response.getKey()).isEqualTo("non-existent-key");
        assertThat(response.isSuccessful()).isFalse();
        assertThat(response.getErrorMessage()).contains("not found");
        
        // Verify the statistics were updated
        Map<String, Long> stats = replica1.getStats();
        assertThat(stats.get("totalRequests")).isEqualTo(1);
        assertThat(stats.get("successfulRequests")).isEqualTo(0);
        assertThat(stats.get("failedRequests")).isEqualTo(1);
        assertThat(stats.get("messages.GET_REQUEST")).isEqualTo(1);
    }
    
    @Test
    void testTimestampConflictResolution() {
        // Arrange - pre-populate the store with a value with timestamp 200
        replica1.getStore().set("conflict-key", "original-value", 200);
        
        AtomicReference<SetResponse> responseRef = new AtomicReference<>();
        
        // Register a handler for SET_RESPONSE messages
        clientMessageBus.registerHandler(new MessageBus.MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.SET_RESPONSE;
            }
            
            @Override
            public void handleMessage(Message message) {
                if (message instanceof SetResponse) {
                    responseRef.set((SetResponse) message);
                }
            }
        });
        
        // Act - try to update with an older timestamp
        SetRequest request = new SetRequest("client", "replica-1", "conflict-key", "new-value", 100);
        clientMessageBus.queueMessage(request);
        
        // Run a few ticks to process the messages
        for (int i = 0; i < 3; i++) {
            clientMessageBus.tick();
            replica1.tick();
            packetSimulator.tick();
        }
        
        // Assert - update should be rejected
        SetResponse response = responseRef.get();
        assertThat(response).isNotNull();
        assertThat(response.getKey()).isEqualTo("conflict-key");
        assertThat(response.isSuccessful()).isFalse();
        assertThat(response.getErrorMessage()).contains("conflict");
        
        // Verify the original value is still in the store
        assertThat(replica1.getStore().get("conflict-key")).isEqualTo("original-value");
        
        // Verify timestamp
        TimestampedValue value = replica1.getStore().getWithTimestamp("conflict-key");
        assertThat(value.getTimestamp()).isEqualTo(200);
        
        // Verify the statistics were updated
        Map<String, Long> stats = replica1.getStats();
        assertThat(stats.get("totalRequests")).isEqualTo(1);
        assertThat(stats.get("successfulRequests")).isEqualTo(0);
        assertThat(stats.get("failedRequests")).isEqualTo(1);
    }
    
    @Test
    void testReplicaStartStop() {
        // Arrange - create a reference to store the response
        AtomicReference<GetResponse> responseRef = new AtomicReference<>();
        
        // Register a handler for GET_RESPONSE messages
        clientMessageBus.registerHandler(new MessageBus.MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_RESPONSE;
            }
            
            @Override
            public void handleMessage(Message message) {
                if (message instanceof GetResponse) {
                    responseRef.set((GetResponse) message);
                }
            }
        });
        
        // Stop the replica
        replica1.stop();
        
        // Act - send a GET request to a stopped replica
        GetRequest request = new GetRequest("client", "replica-1", "some-key");
        clientMessageBus.queueMessage(request);
        
        // Run a few ticks to process the messages
        for (int i = 0; i < 3; i++) {
            clientMessageBus.tick();
            replica1.tick();
            packetSimulator.tick();
        }
        
        // Assert - request should be ignored (no response)
        assertThat(responseRef.get()).isNull();
        
        // Restart the replica
        replica1.start();
        
        // Send another request
        request = new GetRequest("client", "replica-1", "some-key");
        clientMessageBus.queueMessage(request);
        
        // Run a few ticks to process the messages
        for (int i = 0; i < 3; i++) {
            clientMessageBus.tick();
            replica1.tick();
            packetSimulator.tick();
        }
        
        // Assert - now the replica should respond
        assertThat(responseRef.get()).isNotNull();
    }
    
    @Test
    void testMultipleRequests() {
        // Create a reference for responses
        AtomicReference<SetResponse> setResponseRef = new AtomicReference<>();
        AtomicReference<GetResponse> getResponseRef = new AtomicReference<>();
        
        // Register handlers for both types of responses
        clientMessageBus.registerHandler(new MessageBus.MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.SET_RESPONSE;
            }
            
            @Override
            public void handleMessage(Message message) {
                if (message instanceof SetResponse) {
                    setResponseRef.set((SetResponse) message);
                }
            }
        });
        
        clientMessageBus.registerHandler(new MessageBus.MessageHandler() {
            @Override
            public MessageType getHandledType() {
                return MessageType.GET_RESPONSE;
            }
            
            @Override
            public void handleMessage(Message message) {
                if (message instanceof GetResponse) {
                    getResponseRef.set((GetResponse) message);
                }
            }
        });
        
        // Act - send a SET followed by a GET
        SetRequest setRequest = new SetRequest("client", "replica-1", "multi-key", "multi-value");
        clientMessageBus.queueMessage(setRequest);
        
        // Run ticks to process SET request
        for (int i = 0; i < 3; i++) {
            clientMessageBus.tick();
            replica1.tick();
            packetSimulator.tick();
        }
        
        // Send GET request
        GetRequest getRequest = new GetRequest("client", "replica-1", "multi-key");
        clientMessageBus.queueMessage(getRequest);
        
        // Run more ticks to process GET request
        for (int i = 0; i < 3; i++) {
            clientMessageBus.tick();
            replica1.tick();
            packetSimulator.tick();
        }
        
        // Assert - both requests should be processed
        assertThat(setResponseRef.get()).isNotNull();
        assertThat(setResponseRef.get().isSuccessful()).isTrue();
        
        assertThat(getResponseRef.get()).isNotNull();
        assertThat(getResponseRef.get().isSuccessful()).isTrue();
        assertThat(getResponseRef.get().getValue()).isEqualTo("multi-value");
        
        // Verify statistics
        Map<String, Long> stats = replica1.getStats();
        assertThat(stats.get("totalRequests")).isEqualTo(2);
        assertThat(stats.get("successfulRequests")).isEqualTo(2);
        assertThat(stats.get("messages.SET_REQUEST")).isEqualTo(1);
        assertThat(stats.get("messages.GET_REQUEST")).isEqualTo(1);
    }
} 