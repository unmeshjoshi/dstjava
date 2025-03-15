package com.dststore.network;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class MessageBusTest {
    private SimulatedNetwork network;
    private MessageBus messageBus;
    
    @BeforeEach
    void setUp() {
        // Create a message bus with a properly configured network
        messageBus = new MessageBus();
        network = messageBus.getSimulatedNetwork();
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
            assertEquals(sender, from);
            latch.countDown();
        }, MessageBus.NodeType.REPLICA);
        
        // Send a message
        boolean sent = messageBus.sendMessageToReplica(messageContent, sender, receiver);
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
        // Setup network with delay
        network.withLatency(2, 2); // 2 ticks delay
        
        // Setup test nodes
        final String sender = "node1";
        final String receiver = "node2";
        final String messageContent = "Delayed message";
        
        // Track received messages
        AtomicReference<Object> receivedMessage = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        
        // Register nodes
        messageBus.registerNode(sender, (message, from) -> {}, MessageBus.NodeType.REPLICA);
        messageBus.registerNode(receiver, (message, from) -> {
            receivedMessage.set(message);
            latch.countDown();
        }, MessageBus.NodeType.REPLICA);
        
        // Send a message
        boolean sent = messageBus.sendMessageToReplica(messageContent, sender, receiver);
        assertTrue(sent, "Message should be accepted for delivery");
        
        // First tick - no delivery yet due to delay
        int delivered1 = messageBus.tick();
        assertEquals(0, delivered1, "No message should be delivered yet");
        assertNull(receivedMessage.get(), "Message should not be received yet");
        
        // Second tick - message should be delivered
        int delivered2 = messageBus.tick();
        assertEquals(1, delivered2, "One message should be delivered");
        assertEquals(messageContent, receivedMessage.get(), "Message content should match");
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
        boolean sent = messageBus.sendMessageToReplica("This will be lost", sender, receiver);
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
        int partition1Id = network.createPartition(node1, node2);
        int partition2Id = network.createPartition(node3);
        
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
        messageBus.sendMessageToReplica("To node2 in same partition", node1, node2);
        messageBus.sendMessageToReplica("To node3 in different partition", node1, node3);
        
        // Advance the simulation
        messageBus.tick();
        
        // Node2 should receive the message (same partition)
        assertEquals(1, messagesNode2.size());
        assertEquals("To node2 in same partition", messagesNode2.get(0));
        
        // Node3 should not receive the message (different partition)
        assertEquals(0, messagesNode3.size());
        
        // Link the partitions
        network.linkPartitions(partition1Id, partition2Id);
        
        // Send another message to node3
        messageBus.sendMessageToReplica("To node3 after linking", node1, node3);
        
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
            assertEquals(replica, from);
        }, MessageBus.NodeType.CLIENT);
        
        // Send a message from replica to client
        boolean sent = messageBus.sendMessageToClient(messageContent, replica, client);
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
        
        // Track received messages
        List<String> replicaMessages = new ArrayList<>();
        
        // Register nodes
        messageBus.registerNode(client, (message, from) -> {}, MessageBus.NodeType.CLIENT);
        messageBus.registerNode(replica, (message, from) -> {
            replicaMessages.add((String) message);
            assertEquals(client, from);
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
        
        // Try to send a message from client to another client using sendMessageToClient
        Exception exception = assertThrows(IllegalStateException.class, () -> {
            messageBus.sendMessageToClient("Invalid message", "client1", "client2");
        });
        assertTrue(exception.getMessage().contains("is not a replica"));
        
        // Verify that client can send to replica using generic method
        boolean sent = messageBus.sendMessage("Valid message", "client1", "replica1");
        assertTrue(sent);
    }
} 