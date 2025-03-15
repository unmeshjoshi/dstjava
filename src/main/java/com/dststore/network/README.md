# Network Simulation for Distributed Systems Testing

This package provides a framework for simulating network conditions in a distributed system. It's designed for deterministic testing of distributed algorithms and protocols by allowing fine-grained control over message delivery, network partitions, and timing.

## Overview

The framework consists of two main components:

1. **SimulatedNetwork**: Simulates network conditions including partitions, message loss, delays, and bandwidth limitations.
2. **MessageBus**: Provides a high-level interface for sending messages between nodes using the simulated network.

## Using the Framework

### Basic Usage

```java
// Create a network simulator
SimulatedNetwork network = new SimulatedNetwork();

// Create a message bus using the network
MessageBus messageBus = new MessageBus(network);

// Register nodes with message handlers and types
messageBus.registerNode("replica1", (message, from) -> {
    System.out.println("Replica1 received: " + message + " from " + from);
    // Process the message
}, MessageBus.NodeType.REPLICA);

messageBus.registerNode("client1", (message, from) -> {
    System.out.println("Client1 received: " + message + " from " + from);
    // Process the message
}, MessageBus.NodeType.CLIENT);

// Send a message from replica to replica
messageBus.sendMessageToReplica("Hello, replica!", "replica1", "replica2");

// Send a message from replica to client
messageBus.sendMessageToClient("Response to client", "replica1", "client1");

// Advance the simulation by one tick to deliver messages
int deliveredCount = messageBus.tick();
```

### Specialized Communication Methods

The framework provides specialized methods for different communication patterns:

```java
// Replica-to-replica communication
messageBus.sendMessageToReplica(message, fromReplica, toReplica);

// Replica-to-client communication
messageBus.sendMessageToClient(message, fromReplica, toClient);

// Generic communication (for backward compatibility)
messageBus.sendMessage(message, from, to);
```

### Configuring Network Conditions

```java
// Configure network conditions using a fluent interface
SimulatedNetwork network = new SimulatedNetwork()
    .withMessageLossRate(0.1)         // 10% message loss
    .withLatency(2, 5)                // Random delay between 2-5 ticks
    .withBandwidthLimit(10);          // Max 10 messages per tick
```

### Creating Network Partitions

```java
// Create network partitions
int partition1 = network.createPartition("replica1", "replica2", "replica3");
int partition2 = network.createPartition("replica4", "replica5");

// Link partitions (one-way)
network.linkPartitions(partition1, partition2);

// Link partitions (two-way)
network.linkPartitionsBidirectional(partition1, partition2);

// Clear all partitions
network.clearPartitions();
```

### Custom Message Filters

```java
// Add a custom message filter
network.withMessageFilter((message, from, to) -> {
    // Only allow messages with specific properties
    if (message instanceof YourMessageType) {
        return ((YourMessageType) message).isValid();
    }
    return true;
});
```

## Tick-Based Simulation

The framework uses a tick-based approach for deterministic simulation:

1. Send messages through the `MessageBus`
2. Call `messageBus.tick()` to advance time and deliver messages
3. Messages are automatically delivered to registered handlers

This approach allows for:
- Precise control over when messages are delivered
- Reproducible test scenarios
- Controlled simulation of race conditions

## Node Types

The framework supports two types of nodes:

- **REPLICA**: Server nodes that form the distributed system core
- **CLIENT**: Client nodes that interact with the replicas

This distinction follows TigerBeetle's architecture and enables specialized communication patterns:
- Replicas can send messages to other replicas
- Replicas can send messages to clients
- Clients can send messages to replicas
- But clients cannot send messages directly to other clients

## Benefits for Testing

- **Deterministic Testing**: Control exactly when and how messages are delivered
- **Fault Injection**: Simulate network failures, partitions, and delays
- **Network Conditions**: Test how your system behaves under various network scenarios
- **Timing Control**: Control the order and timing of message delivery
- **Node Type Safety**: Enforce communication patterns between different node types

## Example: Testing a Distributed Consensus Algorithm

```java
@Test
void testLeaderElection() {
    // Create network and message bus
    SimulatedNetwork network = new SimulatedNetwork();
    MessageBus messageBus = new MessageBus(network);
    
    // Create replica nodes
    ConsensusMember node1 = new ConsensusMember("node1", messageBus);
    ConsensusMember node2 = new ConsensusMember("node2", messageBus);
    ConsensusMember node3 = new ConsensusMember("node3", messageBus);
    
    // Register nodes as replicas
    messageBus.registerNode("node1", node1, MessageBus.NodeType.REPLICA);
    messageBus.registerNode("node2", node2, MessageBus.NodeType.REPLICA);
    messageBus.registerNode("node3", node3, MessageBus.NodeType.REPLICA);
    
    // Start election
    node1.startElection();
    
    // Run simulation for 10 ticks
    for (int i = 0; i < 10; i++) {
        messageBus.tick();
    }
    
    // Assert leader is elected
    assertTrue(node1.hasLeader() || node2.hasLeader() || node3.hasLeader());
}
``` 