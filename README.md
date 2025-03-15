# Distributed Systems Toolkit (DST)

A toolkit for building and testing distributed systems, featuring configurable network simulation and deterministic testing capabilities.

## Features

- **MessageBus**: Communication backbone for distributed nodes
- **NetworkSimulator**: Simulates various network conditions with deterministic control
  - **NEW**: TigerBeetle-style priority queue for message scheduling
  - Network partitioning
  - Message loss simulation
  - Configurable message latency
  - Bandwidth limitation
  - Custom message filtering

## Network Simulator Enhancements

The NetworkSimulator has been enhanced with TigerBeetle-style message scheduling, providing more realistic and deterministic simulation of distributed systems. Key improvements include:

1. **Priority Queue-based Message Scheduling**: Messages are now scheduled based on their delivery tick, allowing for more precise timing control.
2. **Backward Compatibility**: The simulator maintains compatibility with existing tests while offering new capabilities.
3. **Deterministic Testing**: Enhanced control over message delivery timing enables more precise tests.

### Using TigerBeetle-style Scheduling

```java
// Create a message bus with network simulation
MessageBus messageBus = new MessageBus();
NetworkSimulator simulator = messageBus.enableNetworkSimulation();

// Enable TigerBeetle-style message scheduling
simulator.enableTigerBeetleStyle();

// Configure network conditions
simulator.withLatency(2, 5)         // Messages take 2-5 ticks to deliver
         .withMessageLossRate(0.1)  // 10% message loss
         .withBandwidthLimit(5);    // Process max 5 messages per tick

// Register message handlers
messageBus.registerHandler("node1", message -> {
    System.out.println("Node 1 received: " + message);
});

// Send messages
messageBus.send("Hello, Node 1!", "node2", "node1");

// Advance the simulation tick by tick
for (int i = 0; i < 10; i++) {
    int messagesDelivered = messageBus.advanceTick();
    System.out.println("Tick " + i + ": Delivered " + messagesDelivered + " messages");
}
```

### Network Partitioning Example

```java
// Create partitions
int partition1 = simulator.createPartition("node1", "node2");
int partition2 = simulator.createPartition("node3", "node4");

// Create a one-way link from partition1 to partition2
simulator.linkPartitions(partition1, partition2);

// Create a bidirectional link between partitions
simulator.linkPartitionsBidirectional(partition1, partition2);
```

## Running Examples

See `com.dststore.examples.NetworkSimulatorExample` for a complete example of using the NetworkSimulator.

```java
// Run the basic example
NetworkSimulatorExample.main(args);

// Run the distributed leader election example
NetworkSimulatorExample.runLeaderElectionExample();
```

## Building and Testing

```bash
# Build the project
mvn clean package

# Run tests
mvn test
```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 