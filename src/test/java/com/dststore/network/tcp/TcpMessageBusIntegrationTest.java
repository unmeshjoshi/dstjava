package com.dststore.network.tcp;

import com.dststore.message.*;
import com.dststore.metrics.Metrics;
import com.dststore.network.IMessageBus;
import com.dststore.network.MessageHandler;
import com.dststore.network.PacketListener;
import com.dststore.replica.Replica;
import com.dststore.replica.RequestHandler;
import com.dststore.simulation.SimulatedClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class TcpMessageBusIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(TcpMessageBusIntegrationTest.class);
    
    private SimulatedClock clock;
    private List<TcpMessageBus> messageBuses;
    private List<Replica> replicas;
    private RequestHandler requestHandler;
    private Metrics metrics;
    private List<Integer> allocatedPorts;

    /**
     * Adapter class to make TcpMessageBus work with IMessageBus interface
     */
    private class MessageBusAdapter implements IMessageBus {
        private final String nodeId;
        private final TcpMessageBus tcpMessageBus;
        private final Map<MessageType, MessageHandler> messageHandlers = new HashMap<>();

        public MessageBusAdapter(String nodeId, TcpMessageBus tcpMessageBus) {
            this.nodeId = nodeId;
            this.tcpMessageBus = tcpMessageBus;
            // Register a listener that forwards messages to appropriate handlers
            tcpMessageBus.registerListener(message -> {
                MessageHandler handler = messageHandlers.get(message.getType());
                if (handler != null) {
                    handler.handleMessage(message);
                } else {
                    logger.warn("No handler registered for message type: {}", message.getType());
                }
            });
        }

        @Override
        public String getNodeId() {
            return nodeId;
        }

        @Override
        public void start() {
            tcpMessageBus.start();
        }

        @Override
        public void stop() {
            tcpMessageBus.stop();
        }

        @Override
        public void connect(String targetNodeId, String host, int port) {
            tcpMessageBus.connect(targetNodeId, host, port);
        }

        @Override
        public void disconnect(String targetNodeId) {
            tcpMessageBus.disconnect(targetNodeId);
        }

        @Override
        public void sendMessage(Message message) {
            tcpMessageBus.sendMessage(message);
        }

        @Override
        public void registerListener(PacketListener listener) {
            tcpMessageBus.registerListener(listener);
        }

        @Override
        public void registerHandler(MessageHandler messageHandler) {
            messageHandlers.put(messageHandler.getHandledType(), messageHandler);
        }

        @Override
        public void unregisterHandler(MessageType messageType) {
            messageHandlers.remove(messageType);
        }

        @Override
        public void tick() {
            tcpMessageBus.tick();
        }
    }

    /**
     * Find an available port by trying to bind to random ports
     */
    private int findAvailablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            int port = socket.getLocalPort();
            allocatedPorts.add(port);
            return port;
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        clock = new SimulatedClock();
        messageBuses = new ArrayList<>();
        replicas = new ArrayList<>();
        metrics = new Metrics();
        allocatedPorts = new ArrayList<>();

        // Create client message bus with dynamic port
        int clientPort = findAvailablePort();
        TcpMessageBus clientTcpBus = new TcpMessageBus("client", clientPort);
        messageBuses.add(clientTcpBus);
        IMessageBus clientMessageBus = new MessageBusAdapter("client", clientTcpBus);

        // Create request handler
        requestHandler = new RequestHandler("client", clientMessageBus, metrics);

        // Create three replicas with their message buses using dynamic ports
        for (int i = 1; i <= 3; i++) {
            String replicaId = "replica-" + i;
            int replicaPort = findAvailablePort();
            TcpMessageBus replicaTcpBus = new TcpMessageBus(replicaId, replicaPort);
            messageBuses.add(replicaTcpBus);
            IMessageBus replicaMessageBus = new MessageBusAdapter(replicaId, replicaTcpBus);

            Replica replica = new Replica(replicaId, replicaMessageBus, metrics);
            replicas.add(replica);
        }

        // Start all message buses
        for (TcpMessageBus messageBus : messageBuses) {
            messageBus.start();
        }

        // Connect client to all replicas using their dynamic ports
        for (int i = 1; i <= 3; i++) {
            TcpMessageBus replicaBus = messageBuses.get(i);
            requestHandler.registerReplica(replicaBus.getNodeId(), "localhost", replicaBus.getPort());
        }

        // Start all replicas
        for (Replica replica : replicas) {
            replica.start();
        }
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        // Stop all replicas
        for (Replica replica : replicas) {
            replica.stop();
        }

        // Stop all message buses
        for (TcpMessageBus messageBus : messageBuses) {
            messageBus.stop();
        }

        // Wait a bit for ports to be fully released
        Thread.sleep(100);

        // Clear allocated ports
        allocatedPorts.clear();
    }

    private void tickAll() {
        // Tick the clock first
        clock.tick();

        // Tick all message buses
        for (TcpMessageBus messageBus : messageBuses) {
            messageBus.tick();
        }

        // Tick the request handler
        requestHandler.tick();

        // Tick only active replicas
        for (Replica replica : replicas) {
            if (replica.isRunning()) {
                replica.tick();
            }
        }
    }

    @Test
    void testSetAndGetOperations() throws ExecutionException, InterruptedException, TimeoutException {
        // Set a value
        CompletableFuture<SetResponse> setFuture = requestHandler.set("test-key", "test-value");

        // Run simulation until set operation completes
        int maxTicks = 50;
        int ticks = 0;
        while (!setFuture.isDone() && ticks < maxTicks) {
            tickAll();
            ticks++;
        }

        // Assert set operation completed successfully
        SetResponse setResponse = setFuture.get(1000, TimeUnit.MILLISECONDS);
        assertThat(setResponse.isSuccessful()).isTrue();
        assertThat(setResponse.getKey()).isEqualTo("test-key");

        // Get the value back
        CompletableFuture<GetResponse> getFuture = requestHandler.get("test-key");

        // Run simulation until get operation completes
        ticks = 0;
        while (!getFuture.isDone() && ticks < maxTicks) {
            tickAll();
            ticks++;
        }

        // Assert get operation completed successfully
        GetResponse getResponse = getFuture.get(1000, TimeUnit.MILLISECONDS);
        assertThat(getResponse.isSuccessful()).isTrue();
        assertThat(getResponse.getKey()).isEqualTo("test-key");
        assertThat(getResponse.getValue()).isEqualTo("test-value");
    }

    @Test
    void testReplicaFailure() throws ExecutionException, InterruptedException, TimeoutException {
        // Stop one replica
        replicas.get(0).stop();

        // Set should still succeed with remaining replicas
        CompletableFuture<SetResponse> setFuture = requestHandler.set("failure-test", "value");

        // Run simulation until set operation completes
        int maxTicks = 50;
        int ticks = 0;
        while (!setFuture.isDone() && ticks < maxTicks) {
            tickAll();
            ticks++;
        }

        // Verify set operation succeeded
        SetResponse setResponse = setFuture.get(1000, TimeUnit.MILLISECONDS);
        assertThat(setResponse.isSuccessful()).isTrue();
        assertThat(setResponse.getKey()).isEqualTo("failure-test");

        // Verify the value was set in the working replicas
        for (int i = 1; i < replicas.size(); i++) {
            assertThat(replicas.get(i).getStore().get("failure-test")).isEqualTo("value");
        }

        // Verify the value was not set in the stopped replica
        assertThat(replicas.get(0).getStore().get("failure-test")).isNull();
    }

    @Test
    void testConcurrentRequests() throws ExecutionException, InterruptedException, TimeoutException {
        // Send multiple concurrent requests
        CompletableFuture<SetResponse> setFuture1 = requestHandler.set("key1", "value1");
        CompletableFuture<SetResponse> setFuture2 = requestHandler.set("key2", "value2");
        CompletableFuture<GetResponse> getFuture1 = requestHandler.get("key1");
        CompletableFuture<GetResponse> getFuture2 = requestHandler.get("key2");

        // Run simulation until all operations complete
        int maxTicks = 50;
        int ticks = 0;
        while ((!setFuture1.isDone() || !setFuture2.isDone() || !getFuture1.isDone() || !getFuture2.isDone()) 
               && ticks < maxTicks) {
            tickAll();
            ticks++;
        }

        // Assert all operations completed successfully
        assertThat(setFuture1.get(1000, TimeUnit.MILLISECONDS).isSuccessful()).isTrue();
        assertThat(setFuture2.get(1000, TimeUnit.MILLISECONDS).isSuccessful()).isTrue();
        assertThat(getFuture1.get(1000, TimeUnit.MILLISECONDS).getValue()).isEqualTo("value1");
        assertThat(getFuture2.get(1000, TimeUnit.MILLISECONDS).getValue()).isEqualTo("value2");
    }
} 