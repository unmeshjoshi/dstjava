package com.dststore.network;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;


import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for the enhanced NetworkSimulator with TigerBeetle-style message scheduling.
 */
public class SimulatedNetworkTest {
    
    private MessageBus messageBus;
    private SimulatedNetwork simulatedNetwork;
    
    @BeforeEach
    public void setUp() {
        // Create a new MessageBus with default constructor that sets up SimulatedNetwork properly
        messageBus = new MessageBus();
        
        // Get the SimulatedNetwork from the MessageBus
        simulatedNetwork = messageBus.getSimulatedNetwork();
    }
    

    @Test
    public void testPartitionedNetwork() {
        // Create partitions
        simulatedNetwork.disconnectNodesBidirectional("node1", "node3");
        simulatedNetwork.disconnectNodesBidirectional("node1", "node4");

        simulatedNetwork.disconnectNodesBidirectional("node2", "node3");
        simulatedNetwork.disconnectNodesBidirectional("node2", "node4");

        
        // Create message collectors
        List<String> node1Messages = Collections.synchronizedList(new ArrayList<>());
        List<String> node2Messages = Collections.synchronizedList(new ArrayList<>());
        List<String> node3Messages = Collections.synchronizedList(new ArrayList<>());
        List<String> node4Messages = Collections.synchronizedList(new ArrayList<>());
        
        // Register nodes with message handlers
        messageBus.registerNode("node1", (message, from) -> {
            node1Messages.add((String) message);
        }, MessageBus.NodeType.REPLICA);
        messageBus.registerNode("node2", (message, from) -> {
            node2Messages.add((String) message);
        }, MessageBus.NodeType.REPLICA);
        messageBus.registerNode("node3", (message, from) -> {
            node3Messages.add((String) message);
        }, MessageBus.NodeType.REPLICA);
        messageBus.registerNode("node4", (message, from) -> {
            node4Messages.add((String) message);
        }, MessageBus.NodeType.REPLICA);
        
        // Send messages within and across partitions
        messageBus.sendMessage("Message 1-2", "node1", "node2"); // Should be delivered (same partition)
        messageBus.sendMessage("Message 1-3", "node1", "node3"); // Should be dropped (different partition)
        messageBus.sendMessage("Message 3-4", "node3", "node4"); // Should be delivered (same partition)
        messageBus.sendMessage("Message 3-2", "node3", "node2"); // Should be dropped (different partition)
        
        // Advance tick to process messages
        messageBus.tick();
        
        // Check delivery within partitions
        assertEquals(1, node2Messages.size());
        assertEquals("Message 1-2", node2Messages.get(0));
        
        assertEquals(1, node4Messages.size());
        assertEquals("Message 3-4", node4Messages.get(0));
        
        // Check messages dropped across partitions
        assertEquals(0, node3Messages.size());
        assertEquals(0, node1Messages.size());
        
        // Now link partition1 to partition2 (one-way)
        simulatedNetwork.reconnectAll();
        
        // Clear previous messages
        node1Messages.clear();
        node2Messages.clear();
        node3Messages.clear();
        node4Messages.clear();
        
        // Send messages again
        messageBus.sendMessage("Message 1-3 (after link)", "node1", "node3"); // Should be delivered (partition1 can send to partition2)
        messageBus.sendMessage("Message 3-2 (after link)", "node3", "node2"); // Should be dropped (partition2 cannot send to partition1)
        
        // Advance tick to process messages
        messageBus.tick();
        
        // Check delivery after linking
        assertEquals(1, node3Messages.size());
        assertEquals("Message 1-3 (after link)", node3Messages.get(0));
    }
    
    @Test
    public void testMessageLoss() {
        // Set up 100% packet loss
        simulatedNetwork.withMessageLossRate(1.0);
        
        // Message collectors
        List<String> receivedMessages = Collections.synchronizedList(new ArrayList<>());
        
        // Register nodes with message handlers
        messageBus.registerNode("node1", (message, from) -> {}, MessageBus.NodeType.REPLICA);
        messageBus.registerNode("node2", (message, from) -> {
            receivedMessages.add((String) message);
        }, MessageBus.NodeType.REPLICA);
        
        // Send 10 messages
        for (int i = 0; i < 10; i++) {
            messageBus.sendMessage("Message " + i, "node1", "node2");
        }
        
        // Advance tick to process messages
        messageBus.tick();
        
        // With 100% loss, no messages should be delivered
        assertEquals(0, receivedMessages.size());
        
        // Now set to 0% loss
        simulatedNetwork.withMessageLossRate(0.0);
        
        // Send 10 more messages
        for (int i = 10; i < 20; i++) {
            messageBus.sendMessage("Message " + i, "node1", "node2");
        }
        
        // Advance tick to process messages
        messageBus.tick();
        
        // With 0% loss, all messages should be delivered
        assertEquals(10, receivedMessages.size());
    }
    
    @Test
    public void testBandwidthLimit() {
        // Set bandwidth limit to 5 messages per tick
        simulatedNetwork.withBandwidthLimit(5);
        
        // Message collector
        List<String> receivedMessages = Collections.synchronizedList(new ArrayList<>());
        
        // Register nodes with message handlers
        messageBus.registerNode("node1", (message, from) -> {}, MessageBus.NodeType.REPLICA);
        messageBus.registerNode("node2", (message, from) -> {
            receivedMessages.add((String) message);
        }, MessageBus.NodeType.REPLICA);
        
        // Send 10 messages
        for (int i = 0; i < 10; i++) {
            messageBus.sendMessage("Message " + i, "node1", "node2");
        }
        
        // First tick - should deliver 5 messages due to bandwidth limit
        int delivered1 = messageBus.tick();
        assertEquals(5, delivered1);
        assertEquals(5, receivedMessages.size());
        
        // Second tick - should deliver the remaining 5 messages
        int delivered2 = messageBus.tick();
        assertEquals(5, delivered2);
        assertEquals(10, receivedMessages.size());
        
        // Check message order
        for (int i = 0; i < 10; i++) {
            assertEquals("Message " + i, receivedMessages.get(i));
        }
    }
    
    @Test
    public void testCustomMessageFilter() {
        // Add a filter that blocks messages containing "block"
        simulatedNetwork.addMessageFilter((message, from, to) -> {
            if (message instanceof String) {
                return !((String) message).contains("block");
            }
            return true;
        });
        
        // Message collector
        List<String> receivedMessages = Collections.synchronizedList(new ArrayList<>());
        
        // Register nodes with message handlers
        messageBus.registerNode("node1", (message, from) -> {}, MessageBus.NodeType.REPLICA);
        messageBus.registerNode("node2", (message, from) -> {
            receivedMessages.add((String) message);
        }, MessageBus.NodeType.REPLICA);
        
        // Send 5 normal messages and 5 messages to be blocked
        for (int i = 0; i < 5; i++) {
            messageBus.sendMessage("Message " + i, "node1", "node2");
            messageBus.sendMessage("block " + i, "node1", "node2");
        }
        
        // Advance tick to process messages
        messageBus.tick();
        
        // Only non-blocked messages should be delivered
        assertEquals(5, receivedMessages.size());
        
        // Verify the delivered messages
        for (String message : receivedMessages) {
            assertFalse(message.contains("block"));
        }
    }
} 