package com.dststore.replica;

import com.dststore.message.GetResponse;
import com.dststore.message.SetResponse;
import com.dststore.network.MessageBus;
import com.dststore.network.PacketSimulator;
import com.dststore.network.PacketSimulatorOptions;
import com.dststore.metrics.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the RequestHandler class.
 */
public class RequestHandlerTest {
    private static final Logger logger = LoggerFactory.getLogger(RequestHandlerTest.class);
    
    private PacketSimulator packetSimulator;
    private MessageBus clientMessageBus;
    private RequestHandler requestHandler;
    private MessageBus replica1MessageBus;
    private Replica replica1;
    private MessageBus replica2MessageBus;
    private Replica replica2;
    
    // For simulation control
    private SimulationRunner simulationRunner;
    
    private Metrics metrics;
    
    @BeforeEach
    void setUp() {
        metrics = new Metrics();
        // Create the packet simulator with optimized settings for tests
        // No packet loss, fixed 1-tick delay to speed up tests
        PacketSimulatorOptions options = PacketSimulatorOptions.builder()
            .packetLossProbability(0.0)
            .fixedDelay(1)
            .build();
        packetSimulator = new PacketSimulator(options);
        
        // Create message buses for client and replicas
        clientMessageBus = new MessageBus("client", packetSimulator);
        replica1MessageBus = new MessageBus("replica-1", packetSimulator);
        replica2MessageBus = new MessageBus("replica-2", packetSimulator);
        
        // Create request handler and replicas
        requestHandler = new RequestHandler("client", clientMessageBus, metrics);
        replica1 = new Replica("replica-1", replica1MessageBus, metrics);
        replica2 = new Replica("replica-2", replica2MessageBus, metrics);
        
        // Connect replicas to client
        replica1MessageBus.connect("client");
        replica2MessageBus.connect("client");
        
        // Start the replicas
        replica1.start();
        replica2.start();
        
        // Register replicas with the request handler
        requestHandler.registerReplica("replica-1");
        requestHandler.registerReplica("replica-2");
        
        // Create a simulation runner that includes all components
        simulationRunner = new SimulationRunner()
            .addMessageBus(clientMessageBus)
            .addMessageBus(replica1MessageBus)
            .addMessageBus(replica2MessageBus)
            .addReplica(replica1)
            .addReplica(replica2)
            .addPacketSimulator(packetSimulator)
            .addRequestHandler(requestHandler);
    }
    
    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    void testReplicaRegistration() {
        // Assert
        List<String> replicaIds = requestHandler.getReplicaIds();
        assertThat(replicaIds).containsExactlyInAnyOrder("replica-1", "replica-2");
        
        // Verify connections are established
        assertThat(clientMessageBus.isConnected("replica-1")).isTrue();
        assertThat(clientMessageBus.isConnected("replica-2")).isTrue();
        
        // Unregister a replica
        requestHandler.unregisterReplica("replica-1");
        
        // Verify replica was unregistered
        replicaIds = requestHandler.getReplicaIds();
        assertThat(replicaIds).containsExactly("replica-2");
        assertThat(clientMessageBus.isConnected("replica-1")).isFalse();
        assertThat(clientMessageBus.isConnected("replica-2")).isTrue();
    }
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testSetRequest() throws ExecutionException, InterruptedException, TimeoutException {
        // Act - send a SET request
        CompletableFuture<SetResponse> future = requestHandler.set("test-key", "test-value");
        
        // Process simulation until future is complete or timeout
        simulationRunner.runUntilFutureCompleted(future, 10);
        
        // Assert - verify the request was processed successfully
        SetResponse response = future.get(100, TimeUnit.MILLISECONDS);
        assertThat(response).isNotNull();
        assertThat(response.getKey()).isEqualTo("test-key");
        assertThat(response.isSuccessful()).isTrue();
        
        // Verify the key was set in at least one replica
        assertThat(replica1.getStore().get("test-key")).isEqualTo("test-value");
        
        // Check statistics
        Map<String, Object> stats = requestHandler.getStats();
        assertThat(stats.get("pendingRequests")).isEqualTo(0);
        assertThat(stats.get("registeredReplicas")).isEqualTo(2);
    }
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testGetRequest() throws ExecutionException, InterruptedException, TimeoutException {
        // Arrange - pre-populate the replicas with some data
        replica1.getStore().set("existing-key", "existing-value");
        replica2.getStore().set("existing-key", "existing-value");
        
        // Act - send a GET request
        CompletableFuture<GetResponse> future = requestHandler.get("existing-key");
        
        // Process simulation until future is complete or timeout
        simulationRunner.runUntilFutureCompleted(future, 10);
        
        // Assert - verify the request was processed successfully
        GetResponse response = future.get(100, TimeUnit.MILLISECONDS);
        assertThat(response).isNotNull();
        assertThat(response.getKey()).isEqualTo("existing-key");
        assertThat(response.getValue()).isEqualTo("existing-value");
        assertThat(response.isSuccessful()).isTrue();
    }
    
    @Test
    // @Disabled("Test is flaky due to timing issues in simulation")
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testGetRequestForNonExistentKey() throws ExecutionException, InterruptedException, TimeoutException {
        // Act - send a GET request for a non-existent key
        CompletableFuture<GetResponse> future = requestHandler.get("non-existent-key");
        
        // Process simulation until future is complete or timeout
        simulationRunner.runUntilFutureCompleted(future, 10);
        
        // Assert - verify the request was processed and returned an error
        GetResponse response = future.get(100, TimeUnit.MILLISECONDS);
        assertThat(response).isNotNull();
        assertThat(response.getKey()).isEqualTo("non-existent-key");
        assertThat(response.isSuccessful()).isFalse();
        assertThat(response.getErrorMessage()).contains("not found");
    }
    
    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    void testRequestWithNoReplicas() {
        // Arrange - unregister all replicas
        requestHandler.unregisterReplica("replica-1");
        requestHandler.unregisterReplica("replica-2");
        
        // Act & Assert - verify that requests fail when no replicas are registered
        CompletableFuture<SetResponse> setFuture = requestHandler.set("key", "value");
        assertThatThrownBy(() -> setFuture.get(100, TimeUnit.MILLISECONDS))
            .hasCauseInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No replicas registered");
        
        CompletableFuture<GetResponse> getFuture = requestHandler.get("key");
        assertThatThrownBy(() -> getFuture.get(100, TimeUnit.MILLISECONDS))
            .hasCauseInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No replicas registered");
    }
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testReplicaFailure() throws ExecutionException, InterruptedException, TimeoutException {
        // Arrange - stop one replica
        replica1.stop();
        
        // Act - send a request (should still work with one replica)
        CompletableFuture<SetResponse> future = requestHandler.set("failure-test", "value");
        
        // Process simulation until future is complete or timeout
        simulationRunner.runUntilFutureCompleted(future, 10);
        
        // Assert - verify the request was processed by the working replica
        SetResponse response = future.get(100, TimeUnit.MILLISECONDS);
        assertThat(response).isNotNull();
        assertThat(response.getKey()).isEqualTo("failure-test");
        assertThat(response.isSuccessful()).isTrue();
        
        // Verify the key was set in the working replica
        assertThat(replica2.getStore().get("failure-test")).isEqualTo("value");
        assertThat(replica1.getStore().get("failure-test")).isNull(); // Not set in the stopped replica
    }
    
    /**
     * Helper class to run simulations in a coordinated manner.
     * Manages all the components that need to be ticked for the simulation.
     */
    static class SimulationRunner {
        private final List<MessageBus> messageBuses = new ArrayList<>();
        private final List<Replica> replicas = new ArrayList<>();
        private final List<PacketSimulator> packetSimulators = new ArrayList<>();
        private final List<RequestHandler> requestHandlers = new ArrayList<>();
        
        /**
         * Add a message bus to the simulation.
         */
        public SimulationRunner addMessageBus(MessageBus messageBus) {
            messageBuses.add(messageBus);
            return this;
        }
        
        /**
         * Add a replica to the simulation.
         */
        public SimulationRunner addReplica(Replica replica) {
            replicas.add(replica);
            return this;
        }
        
        /**
         * Add a packet simulator to the simulation.
         */
        public SimulationRunner addPacketSimulator(PacketSimulator packetSimulator) {
            packetSimulators.add(packetSimulator);
            return this;
        }
        
        /**
         * Add a request handler to the simulation.
         */
        public SimulationRunner addRequestHandler(RequestHandler requestHandler) {
            requestHandlers.add(requestHandler);
            return this;
        }
        
        /**
         * Run a single tick of the simulation for all components.
         */
        public void runTick() {
            // Tick message buses first for outgoing messages
            for (MessageBus messageBus : messageBuses) {
                messageBus.tick();
            }
            
            // Tick replicas
            for (Replica replica : replicas) {
                replica.tick();
            }
            
            // Tick request handlers
            for (RequestHandler requestHandler : requestHandlers) {
                requestHandler.tick();
            }
            
            // Tick packet simulators last to deliver messages
            for (PacketSimulator packetSimulator : packetSimulators) {
                packetSimulator.tick();
            }
        }
        
        /**
         * Run the simulation for a fixed number of ticks.
         */
        public void runTicks(int numberOfTicks) {
            for (int i = 0; i < numberOfTicks; i++) {
                runTick();
            }
        }
        
        /**
         * Run the simulation until the given future is complete or the maximum number of ticks is reached.
         */
        public <T> void runUntilFutureCompleted(CompletableFuture<T> future, int maxTicks) {
            int ticks = 0;
            while (!future.isDone() && ticks < maxTicks) {
                runTick();
                ticks++;
            }
            
            logger.info("Simulation ran for {} ticks, future completed: {}", ticks, future.isDone());
            
            if (!future.isDone()) {
                logger.warn("Future did not complete after {} ticks", maxTicks);
            }
        }
    }
} 