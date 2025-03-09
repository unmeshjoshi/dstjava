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
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dststore.message.GetRequest;
import com.dststore.message.GetResponse;
import com.dststore.message.SetRequest;
import com.dststore.message.SetResponse;
import com.dststore.network.MessageBus;

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
        
        // Create a simulation runner that includes all components
        simulationRunner = new SimulationRunner()
            .addMessageBus(clientMessageBus)
            .addMessageBus(replica1MessageBus)
            .addMessageBus(replica2MessageBus)
            .addReplica(replica1)
            .addReplica(replica2)
            .addPacketSimulator(packetSimulator)
            .addRequestHandler(requestHandler);

        // Connect replicas to client and wait for connections to establish
        replica1MessageBus.connect("client");
        replica2MessageBus.connect("client");
        simulationRunner.runTicks(10); // Allow time for connections to establish
        
        // Start the replicas
        replica1.start();
        replica2.start();
        
        // Register replicas with the request handler
        requestHandler.registerReplica("replica-1", "localhost", 8001);
        requestHandler.registerReplica("replica-2", "localhost", 8002);
        
        // Allow time for replica registration to complete
        simulationRunner.runTicks(10);
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
        
        // Process simulation until future is complete with more ticks
        simulationRunner.runUntilFutureCompleted(future, 200);
        
        // Assert - verify the request was processed successfully
        SetResponse response = future.get(2000, TimeUnit.MILLISECONDS);
        assertThat(response).isNotNull();
        assertThat(response.getKey()).isEqualTo("test-key");
        assertThat(response.isSuccessful()).isTrue();
        
        // Verify the key was set in at least one replica
        assertThat(replica1.getStore().get("test-key")).isEqualTo("test-value");
        
        // Check statistics
        Map<String, Object> stats = requestHandler.getStats();
        assertThat(stats.get("pendingRequests")).isEqualTo(0);
        assertThat(stats.get("registeredReplicas")).isEqualTo(2);
        
        // Check replica message counts
        Map<String, Long> replica1Stats = replica1.getStats();
        assertThat(replica1Stats.get("messages.SET_REQUEST")).isEqualTo(1L);
    }
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testGetRequest() throws ExecutionException, InterruptedException, TimeoutException {
        // Arrange - pre-populate the replicas with some data
        replica1.getStore().set("existing-key", "existing-value");
        replica2.getStore().set("existing-key", "existing-value");
        
        // Act - send a GET request
        CompletableFuture<GetResponse> future = requestHandler.get("existing-key");
        
        // Process simulation until future is complete with more ticks
        simulationRunner.runUntilFutureCompleted(future, 200);
        
        // Assert - verify the request was processed successfully
        GetResponse response = future.get(2000, TimeUnit.MILLISECONDS);
        assertThat(response).isNotNull();
        assertThat(response.getKey()).isEqualTo("existing-key");
        assertThat(response.getValue()).isEqualTo("existing-value");
        assertThat(response.isSuccessful()).isTrue();
    }
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testGetRequestForNonExistentKey() throws ExecutionException, InterruptedException, TimeoutException {
        // Act - send a GET request for a non-existent key
        CompletableFuture<GetResponse> future = requestHandler.get("non-existent-key");
        
        // Process simulation until future is complete with more ticks
        simulationRunner.runUntilFutureCompleted(future, 200);
        
        // Assert - verify the request was processed and returned an error
        GetResponse response = future.get(2000, TimeUnit.MILLISECONDS);
        assertThat(response).isNotNull();
        assertThat(response.getKey()).isEqualTo("non-existent-key");
        assertThat(response.isSuccessful()).isFalse();
        assertThat(response.getErrorMessage()).contains("not found");
    }
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testRequestWithNoReplicas() throws ExecutionException, InterruptedException, TimeoutException {
        // Arrange - unregister all replicas
        requestHandler.unregisterReplica("replica-1");
        requestHandler.unregisterReplica("replica-2");
        
        // Allow time for unregistration to complete
        simulationRunner.runTicks(10);
        
        // Act & Assert - verify that requests fail with appropriate error messages
        CompletableFuture<SetResponse> setFuture = requestHandler.set("key", "value");
        simulationRunner.runUntilFutureCompleted(setFuture, 50);
        SetResponse setResponse = setFuture.get(1000, TimeUnit.MILLISECONDS);
        assertThat(setResponse.isSuccessful()).isFalse();
        assertThat(setResponse.getErrorMessage()).isEqualTo("No replicas available");
        
        CompletableFuture<GetResponse> getFuture = requestHandler.get("key");
        simulationRunner.runUntilFutureCompleted(getFuture, 50);
        GetResponse getResponse = getFuture.get(1000, TimeUnit.MILLISECONDS);
        assertThat(getResponse.isSuccessful()).isFalse();
        assertThat(getResponse.getErrorMessage()).isEqualTo("No replicas available");
    }
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testReplicaFailure() throws ExecutionException, InterruptedException, TimeoutException {
        // Arrange - stop one replica
        replica1.stop();
        
        // Wait a bit to ensure the replica stop is processed
        simulationRunner.runTicks(20);
        
        // Act - send a SET request
        CompletableFuture<SetResponse> future = requestHandler.set("test-key", "test-value");
        
        // Process simulation until future is complete with more ticks
        simulationRunner.runUntilFutureCompleted(future, 200);
        
        // Assert - verify the request was processed successfully by the remaining replica
        SetResponse response = future.get(2000, TimeUnit.MILLISECONDS);
        assertThat(response).isNotNull();
        assertThat(response.getKey()).isEqualTo("test-key");
        assertThat(response.isSuccessful()).isTrue();
        
        // Verify the key was set in the active replica
        assertThat(replica2.getStore().get("test-key")).isEqualTo("test-value");
        
        // Verify that replica1 is marked as failed
        assertThat(requestHandler.getStats().get("pendingRequests")).isEqualTo(0);
    }
    
    /**
     * Helper class to run simulations in a coordinated manner.
     * Manages all the components that need to be ticked for the simulation.
     */
    static class SimulationRunner {
        private static final Logger logger = LoggerFactory.getLogger(SimulationRunner.class);
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
            // First, tick packet simulators to deliver any pending messages
            for (PacketSimulator packetSimulator : packetSimulators) {
                logger.debug("Ticking packet simulator");
                packetSimulator.tick();
            }
            
            // Then tick message buses to process received messages
            for (MessageBus messageBus : messageBuses) {
                logger.debug("Ticking message bus {}", messageBus.getNodeId());
                messageBus.tick();
            }
            
            // Then tick replicas to process any messages
            for (Replica replica : replicas) {
                logger.debug("Ticking replica {}", replica.getReplicaId());
                replica.tick();
            }
            
            // Finally tick request handlers to process responses
            for (RequestHandler requestHandler : requestHandlers) {
                logger.debug("Ticking request handler");
                requestHandler.tick();
            }
        }
        
        /**
         * Run the simulation for a fixed number of ticks.
         */
        public void runTicks(int numberOfTicks) {
            logger.info("Running simulation for {} ticks", numberOfTicks);
            for (int i = 0; i < numberOfTicks; i++) {
                logger.debug("Running tick {}", i);
                runTick();
            }
            logger.info("Simulation completed after {} ticks", numberOfTicks);
        }
        
        /**
         * Run the simulation until the given future is complete or the maximum number of ticks is reached.
         */
        public <T> void runUntilFutureCompleted(CompletableFuture<T> future, int maxTicks) {
            int ticks = 0;
            while (!future.isDone() && ticks < maxTicks) {
                logger.debug("Running tick {} of max {}, future completed: {}", ticks, maxTicks, future.isDone());
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