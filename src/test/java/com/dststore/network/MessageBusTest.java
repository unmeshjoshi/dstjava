package com.dststore.network;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import com.dststore.network.SimulatedNetwork.DeliveryContext;

public class MessageBusTest {
    private MessageBus messageBus;
    private SimulatedNetwork network;
    private List<String> receivedMessages = new ArrayList<>();
    private final String node1 = "node1";
    private final String node2 = "node2";
    private final String node3 = "node3";

    @BeforeEach
    public void setUp() {
        messageBus = new MessageBus();
        network = messageBus.getSimulatedNetwork();
        receivedMessages.clear();
    }

    @AfterEach
    public void tearDown() {
        messageBus.reset();

    }


    private void handleMessage(Object message, DeliveryContext from) {
        receivedMessages.add(message.toString());
    }
    
    @Test
    void testBasicMessageDelivery() {
        // Setup test nodes
        final String sender = "node1";
        final String receiver = "node2";
        final String messageContent = "Hello, World!";
        
        // Track received messages
        List<String> receivedMessages = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        
        // Register nodes
        messageBus.registerNode(sender, (message, from) -> {}, MessageBus.NodeType.REPLICA);
        messageBus.registerNode(receiver, (message, from) -> {
            receivedMessages.add((String) message);
            assertEquals(sender, from.getFrom());
            latch.countDown();
        }, MessageBus.NodeType.REPLICA);
        
        // Send a message
        boolean sent = messageBus.sendMessage(messageContent, sender, receiver);
        assertTrue(sent, "Message should be accepted for delivery");
        
        // Advance the simulation to trigger delivery
        int delivered = messageBus.tick();
        assertEquals(1, delivered, "One message should be delivered");
        
        // Verify message was received
        assertEquals(1, receivedMessages.size());
        assertEquals(messageContent, receivedMessages.get(0));
    }
    
    @Test
    void testMessageWithNetworkDelay() {
        network.withLatency(1, 3); // Set delay between 1-3 ticks
        messageBus.registerNode(node1, this::handleMessage, MessageBus.NodeType.REPLICA);
        messageBus.registerNode(node2, this::handleMessage, MessageBus.NodeType.REPLICA);
        messageBus.registerNode(node3, this::handleMessage, MessageBus.NodeType.REPLICA);
        
        // Create a network partition
        network.disconnectNodesBidirectional(node1, node3);
        network.disconnectNodesBidirectional(node2, node3);
        
        // Send a message that should be dropped
        var sent1 = messageBus.sendMessage("test message", node1, node3);

        // Run for a few ticks
        for (int i = 0; i < 5; i++) {
            messageBus.tick();
        }
        
        // Message should not be delivered due to partition
        assertEquals(0, receivedMessages.size());
        
        // Heal the partition
        network.reconnectAll();
        
        // Send another message that should be delivered
        var sent = messageBus.sendMessage("test message 2", node1, node3);

        // Run for enough ticks to allow for maximum delay
        for (int i = 0; i < 5; i++) {
            messageBus.tick();
        }
        
        // Message should be delivered after partition is healed
        assertEquals(1, receivedMessages.size());
        assertEquals("test message 2", receivedMessages.get(0));
    }
    
    @Test
    void testMessageLoss() {
        // Setup network with packet loss
        network.withMessageLossRate(1.0); // 100% packet loss
        
        // Setup test nodes
        final String sender = "node1";
        final String receiver = "node2";
        
        // Track received messages
        List<Object> receivedMessages = new ArrayList<>();
        
        // Register nodes
        messageBus.registerNode(sender, (message, from) -> {}, MessageBus.NodeType.REPLICA);
        messageBus.registerNode(receiver, (message, from) -> {
            receivedMessages.add(message);
        }, MessageBus.NodeType.REPLICA);
        
        // Send a message
        boolean sent = messageBus.sendMessage("This will be lost", sender, receiver);
        assertFalse(sent, "Message should be dropped due to loss rate");
        
        // Advance the simulation
        int delivered = messageBus.tick();
        assertEquals(0, delivered, "No message should be delivered");
        assertTrue(receivedMessages.isEmpty(), "No messages should be received");
    }
    
    @Test
    void testNetworkPartition() {
        // Setup test nodes
        final String node1 = "node1";
        final String node2 = "node2";
        final String node3 = "node3";
        
        // Create partitions
        network.disconnectNodesBidirectional(node1, node3);

        // Track received messages
        List<String> messagesNode2 = new ArrayList<>();
        List<String> messagesNode3 = new ArrayList<>();
        
        // Register nodes
        messageBus.registerNode(node1, (message, from) -> {}, MessageBus.NodeType.REPLICA);
        messageBus.registerNode(node2, (message, from) -> {
            messagesNode2.add((String) message);
        }, MessageBus.NodeType.REPLICA);
        messageBus.registerNode(node3, (message, from) -> {
            messagesNode3.add((String) message);
        }, MessageBus.NodeType.REPLICA);
        
        // Send messages
        messageBus.sendMessage("To node2 in same partition", node1, node2);
        messageBus.sendMessage("To node3 in different partition", node1, node3);
        
        // Advance the simulation
        messageBus.tick();
        
        // Node2 should receive the message (same partition)
        assertEquals(1, messagesNode2.size());
        assertEquals("To node2 in same partition", messagesNode2.get(0));
        
        // Node3 should not receive the message (different partition)
        assertEquals(0, messagesNode3.size());
        
        // Link the partitions
        network.reconnectAll();
        
        // Send another message to node3
        messageBus.sendMessage("To node3 after linking", node1, node3);
        
        // Advance the simulation
        messageBus.tick();
        
        // Node3 should now receive the message
        assertEquals(1, messagesNode3.size());
        assertEquals("To node3 after linking", messagesNode3.get(0));
    }
    
    @Test
    void testReplicaToClientCommunication() {
        // Setup test nodes
        final String replica = "replica1";
        final String client = "client1";
        final String messageContent = "Client request response";
        
        // Track received messages
        List<String> clientMessages = new ArrayList<>();
        
        // Register nodes
        messageBus.registerNode(replica, (message, from) -> {}, MessageBus.NodeType.REPLICA);
        messageBus.registerNode(client, (message, from) -> {
            clientMessages.add((String) message);
            assertEquals(replica, from.getFrom());
        }, MessageBus.NodeType.CLIENT);
        
        // Send a message from replica to client
        boolean sent = messageBus.sendMessage(messageContent, replica, client);
        assertTrue(sent, "Message should be accepted for delivery");
        
        // Advance the simulation
        int delivered = messageBus.tick();
        assertEquals(1, delivered, "One message should be delivered");
        
        // Verify message was received
        assertEquals(1, clientMessages.size());
        assertEquals(messageContent, clientMessages.get(0));
    }
    
    @Test
    void testClientToReplicaCommunication() {
        // Setup test nodes
        final String client = "client1";
        final String replica = "replica1";
        final String messageContent = "Client request";
        messageBus.getSimulatedNetwork().reset();
        // Track received messages
        List<String> replicaMessages = new ArrayList<>();
        
        // Register nodes
        messageBus.registerNode(client, (message, from) -> {}, MessageBus.NodeType.CLIENT);
        messageBus.registerNode(replica, (message, from) -> {
            replicaMessages.add((String) message);
            assertEquals(client, from.getFrom());
        }, MessageBus.NodeType.REPLICA);
        
        // Use the generic sendMessage method for client-to-replica communication
        boolean sent = messageBus.sendMessage(messageContent, client, replica);
        assertTrue(sent, "Message should be accepted for delivery");
        
        // Advance the simulation
        int delivered = messageBus.tick();
        assertEquals(1, delivered, "One message should be delivered");
        
        // Verify message was received
        assertEquals(1, replicaMessages.size());
        assertEquals(messageContent, replicaMessages.get(0));
    }
    
    @Test
    void testGetRegisteredNodesByType() {
        // Register different node types
        messageBus.registerNode("replica1", (message, from) -> {}, MessageBus.NodeType.REPLICA);
        messageBus.registerNode("replica2", (message, from) -> {}, MessageBus.NodeType.REPLICA);
        messageBus.registerNode("client1", (message, from) -> {}, MessageBus.NodeType.CLIENT);
        messageBus.registerNode("client2", (message, from) -> {}, MessageBus.NodeType.CLIENT);
        messageBus.registerNode("client3", (message, from) -> {}, MessageBus.NodeType.CLIENT);
        
        // Check all registered nodes
        assertEquals(5, messageBus.getRegisteredNodeIds().size());
        
        // Check replicas
        Set<String> replicas = messageBus.getRegisteredReplicaIds();
        assertEquals(2, replicas.size());
        assertTrue(replicas.contains("replica1"));
        assertTrue(replicas.contains("replica2"));
        
        // Check clients
        Set<String> clients = messageBus.getRegisteredClientIds();
        assertEquals(3, clients.size());
        assertTrue(clients.contains("client1"));
        assertTrue(clients.contains("client2"));
        assertTrue(clients.contains("client3"));
    }
    
    @Test
    void testInvalidNodeTypeSending() {
        // Register nodes
        messageBus.registerNode("replica1", (message, from) -> {}, MessageBus.NodeType.REPLICA);
        messageBus.registerNode("client1", (message, from) -> {}, MessageBus.NodeType.CLIENT);
        
        // Client cannot send to another client
        messageBus.registerNode("client2", (message, from) -> {}, MessageBus.NodeType.CLIENT);

        // Verify that client can send to replica using generic method
        boolean sent = messageBus.sendMessage("Valid message", "client1", "replica1");
        assertTrue(sent);
    }
} 